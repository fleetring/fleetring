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
//!
//! What a root agent's token holds depends on the fleet (ADR-0047, points 2.6 and 12.2): once
//! every member reports `delegation` ([`Support::Every`]), it carries `fleetring.kind = "agent"`,
//! `fleetring.owner`, and defaults with `grant`, so the agent can delegate; before that, FR-081's
//! defaults and neither claim, which a 0.7 node accepts.
//!
//! A delegated agent's identity (ADR-0047, point 10) is a directory too, `<agents_dir>/42.c7/`,
//! holding only its token, `identity.toml` (version 2, `issued_by = "delegation"`), and the lock:
//! it connects with its root agent's certificate, in `<agents_dir>/42/`, so no certificate or key
//! is written for it. `agent delegate` writes the operation ID into the directory before it asks
//! the control plane, so a run after a crash resends the same request.

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
use fleetring_control::delegation::AgentName;
use fleetring_control::fork::OperationId;
use fleetring_control::{Feature, Member, Millis, Release, Timestamp, TokenId};
use fleetringd::auth::{
    Action, BearerToken, IssueError, MAX_TOKEN_LIFETIME, NOT_BEFORE_LEEWAY_SECS, ParseScopeError,
    Scope, TokenSpec,
};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::data::Client;
use fleetringd::dev::{self, DevChecked, DevError};
use fleetringd::mcp::Access;
use fleetringd::tls::{
    AGENT_PREFIX, AUTHORITY_CERT_FILE, AUTHORITY_KEY_FILE, Authority, AuthorityError, Identity,
    IdentityError, NameError, Principal, Validity,
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

/// What starts the name of the file `agent remove` leaves in the agents directory, beside the
/// removed identity's, to say when a replacement token may be minted (`AgentDir::replacement_file`).
const REPLACEMENT_FILE_PREFIX: &str = ".mint-after-";

/// The operation ID `agent delegate` asks under, in a delegated agent's directory until its
/// record is written (ADR-0047, point 10).
const OPERATION_FILE: &str = "operation";

/// The newest `identity.toml` format this release reads and writes (ADR-0044, point 1.7).
///
/// Version 2 adds `scopes_from`, and a delegated agent's record: `issued_by = "delegation"`, with
/// `root`, `issuer_key_id` and `operation`, and no `cert_expires_at`. A root agent's record is
/// written at version 2 only when it holds `scopes_from`, and at version 1, field for field, when
/// a 0.7 reader can use it as it is; a delegated agent's is always version 2, which 0.7 refuses
/// with "upgrade".
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

/// How long [`delegation_support`] waits for the fleet's answer before it calls it unknown.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// An agent's ID: 1 to 32 of `[a-z0-9_]`. Its principal is `agent-<id>` (ADR-0043, point 8.5).
///
/// Neither `-` nor `.` is allowed: the `-` after the ID in `agent-<id>-*` then always ends it,
/// and `.` is reserved for delegated agents' lineage (ADR-0044, point 1.4).
///
/// The principal and the default scopes are built when the ID is parsed, so that using them
/// can't fail. They are boxed, so that commands holding an ID stay small.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AgentId {
    id: String,
    principal: Principal,
    /// The scopes on the agent's own databases, `agent-<id>-*`.
    own: Box<Actions>,
    /// The scopes on its delegated agents' databases, `agent-<id>.*`.
    below: Box<Actions>,
}

/// One scope for each action a default may hold, on one pattern.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Actions {
    admin: Scope,
    read: Scope,
    stream: Scope,
    grant: Scope,
}

impl Actions {
    /// The scopes on `pattern`, or `None` if one doesn't parse.
    fn on(pattern: &str) -> Option<Self> {
        let scope = |action: &str| format!("{action}:{pattern}").parse().ok();
        Some(Self {
            admin: scope("admin")?,
            read: scope("read")?,
            stream: scope("stream")?,
            grant: scope("grant")?,
        })
    }

    /// The scope that gives `access`: `admin` or `read`.
    fn access(&self, access: Access) -> Scope {
        match access {
            Access::ReadOnly => self.read.clone(),
            Access::ReadWrite => self.admin.clone(),
        }
    }
}

/// Which defaults, and which claims, a root agent's token is minted with (ADR-0047, point 12.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Minting {
    /// FR-081's defaults, without `fleetring.kind` or `fleetring.owner`: while a member lacks
    /// `delegation`, or the fleet couldn't be asked. A 0.7 node accepts such a token.
    Fr081,
    /// This release's defaults, with `grant`, and `fleetring.kind = "agent"` and
    /// `fleetring.owner`, so the agent can delegate: once every member reports `delegation`.
    Delegating,
}

/// The defaults an identity holds, when they are exactly some release's (ADR-0047, point 12.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeldDefaults {
    /// 0.7's only default, `admin:agent-<id>-*` or `read:agent-<id>-*`.
    V07,
    /// FR-081's: that and `stream:agent-<id>-*`.
    Fr081,
    /// This release's, which can delegate.
    Delegating,
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

    /// The scopes an identity gets unless `--scope` says otherwise.
    ///
    /// - [`Minting::Fr081`] (FR-081): `admin:agent-<id>-*` and `stream:agent-<id>-*`, or
    ///   `read:agent-<id>-*` and `stream:agent-<id>-*` for a reading agent. No other action
    ///   implies `stream` (ADR-0025), and following an agent's own changes is reading them, so
    ///   both get it.
    /// - [`Minting::Delegating`] (ADR-0047, point 2.6): `admin` (or `read`), `stream`, and
    ///   `grant`, each on `agent-<id>-*` and `agent-<id>.*`, so the agent may act on its
    ///   delegated agents' databases and hand its own actions down to them.
    pub(crate) fn default_scopes(&self, access: Access, minting: Minting) -> Vec<Scope> {
        match minting {
            Minting::Fr081 => vec![self.own.access(access), self.own.stream.clone()],
            Minting::Delegating => vec![
                self.own.access(access),
                self.below.access(access),
                self.own.stream.clone(),
                self.below.stream.clone(),
                self.own.grant.clone(),
                self.below.grant.clone(),
            ],
        }
    }

    /// Which release's defaults `record` holds, exactly, for its access: `None` for scopes
    /// `--scope` chose, and for any others.
    pub(crate) fn held_defaults(&self, record: &AgentRecord) -> Option<HeldDefaults> {
        if record.scopes_from != ScopesFrom::Defaults {
            return None;
        }
        let access = record.access();
        let held: BTreeSet<&str> = record.scopes.iter().map(String::as_str).collect();
        let same = |scopes: &[Scope]| {
            let texts: Vec<String> = scopes.iter().map(ToString::to_string).collect();
            texts.iter().map(String::as_str).collect::<BTreeSet<&str>>() == held
        };
        if same(&[self.own.access(access)]) {
            Some(HeldDefaults::V07)
        } else if same(&self.default_scopes(access, Minting::Fr081)) {
            Some(HeldDefaults::Fr081)
        } else if same(&self.default_scopes(access, Minting::Delegating)) {
            Some(HeldDefaults::Delegating)
        } else {
            None
        }
    }

    /// The scopes `agent renew` gives `record` in place of its own, minting as `minting` says:
    /// `minting`'s defaults, when `record` holds exactly 0.7's or FR-081's defaults for its
    /// access and they are fewer (ADR-0047, point 12.4). `None` keeps the scopes as they are, so
    /// scopes of an identity's own, and scopes `--scope` chose, are never widened.
    pub(crate) fn widened(&self, record: &AgentRecord, minting: Minting) -> Option<Vec<Scope>> {
        let upgrade = match (self.held_defaults(record)?, minting) {
            (HeldDefaults::V07, _) | (HeldDefaults::Fr081, Minting::Delegating) => true,
            (HeldDefaults::Fr081, Minting::Fr081) | (HeldDefaults::Delegating, _) => false,
        };
        upgrade.then(|| self.default_scopes(record.access(), minting))
    }
}

impl FromStr for AgentId {
    type Err = AgentError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || AgentError::InvalidId {
            id: text.to_owned(),
        };
        if text.contains('.') && text.parse::<DelegatedId>().is_ok() {
            return Err(AgentError::Delegated {
                id: text.to_owned(),
            });
        }
        let valid = !text.is_empty()
            && text.len() <= MAX_ID_LEN
            && text
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        if !valid {
            return Err(invalid());
        }
        Ok(Self {
            id: text.to_owned(),
            principal: format!("{AGENT_PREFIX}{text}")
                .parse()
                .map_err(|_| invalid())?,
            own: Box::new(Actions::on(&format!("{AGENT_PREFIX}{text}-*")).ok_or_else(invalid)?),
            below: Box::new(Actions::on(&format!("{AGENT_PREFIX}{text}.*")).ok_or_else(invalid)?),
        })
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.id)
    }
}

/// A delegated agent's ID: a root agent's ID followed by one to eight `.<segment>`, such as
/// `42.c7`, whose principal is `agent-42.c7` (ADR-0047, point 2.5). Each segment is 1 to 32 of
/// `[a-z0-9_]`, and the principal at most 64 bytes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DelegatedId {
    id: String,
    name: AgentName,
    root: AgentId,
}

impl DelegatedId {
    /// The ID as text.
    pub(crate) fn as_str(&self) -> &str {
        &self.id
    }

    /// The principal, `agent-<id>`.
    pub(crate) fn name(&self) -> &AgentName {
        &self.name
    }

    /// The root agent of its lineage.
    pub(crate) fn root(&self) -> &AgentId {
        &self.root
    }

    /// The agent that delegates it: the ID less its last segment.
    pub(crate) fn parent(&self) -> AgentRef {
        // A leading part of a valid ID that still holds a '.' is a valid ID too.
        match self.id.rsplit_once('.') {
            Some((parent, _)) if parent.contains('.') => parent
                .parse()
                .map_or_else(|_| AgentRef::Root(self.root.clone()), AgentRef::Delegated),
            _ => AgentRef::Root(self.root.clone()),
        }
    }

    /// Its last segment, which the parent asks the control plane for.
    pub(crate) fn segment(&self) -> &str {
        self.id
            .rsplit_once('.')
            .map_or(self.id.as_str(), |(_, segment)| segment)
    }
}

impl FromStr for DelegatedId {
    type Err = AgentError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = |reason: String| AgentError::InvalidDelegatedId {
            id: text.to_owned(),
            reason,
        };
        let Some((root, _)) = text.split_once('.') else {
            return Err(invalid(
                "it names a root agent; a delegated agent's ID is its parent's followed by \
                 '.<segment>', such as 42.c7"
                    .to_owned(),
            ));
        };
        let root: AgentId = root
            .parse()
            .map_err(|_| invalid(format!("'{root}' isn't a root agent's ID")))?;
        let name: AgentName = format!("{AGENT_PREFIX}{text}").parse().map_err(
            |error: fleetring_control::delegation::ParseAgentNameError| invalid(error.to_string()),
        )?;
        Ok(Self {
            id: text.to_owned(),
            name,
            root,
        })
    }
}

impl fmt::Display for DelegatedId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.id)
    }
}

/// An agent the command line names: a root agent, `42`, or a delegated agent, `42.c7`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AgentRef {
    /// A root agent, with a certificate of its own.
    Root(AgentId),
    /// A delegated agent, which connects with its root's certificate.
    Delegated(DelegatedId),
}

impl AgentRef {
    /// The ID as text.
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Root(id) => id.as_str(),
            Self::Delegated(id) => id.as_str(),
        }
    }

    /// The principal, `agent-<id>`.
    pub(crate) fn principal(&self) -> String {
        match self {
            Self::Root(id) => id.principal().to_string(),
            Self::Delegated(id) => id.name().to_string(),
        }
    }

    /// The ID's segments, the root's first: the order `agent list` shows agents in, each
    /// delegated agent after its parent.
    pub(crate) fn segments(&self) -> Vec<&str> {
        self.as_str().split('.').collect()
    }
}

impl FromStr for AgentRef {
    type Err = AgentError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.contains('.') {
            text.parse().map(Self::Delegated)
        } else {
            text.parse().map(Self::Root)
        }
    }
}

impl fmt::Display for AgentRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The ID in a record's principal: `agent-42.c7` as `42.c7`. A name without the prefix, which a
/// valid record never holds, is returned as it is.
pub(crate) fn id_of(principal: &str) -> &str {
    principal.strip_prefix(AGENT_PREFIX).unwrap_or(principal)
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
    /// The agent that delegated this one, as a principal: a delegated agent's parent, absent for
    /// a root agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) parent: Option<String>,
    /// A delegated agent's root agent, as a principal; absent for a root agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) root: Option<String>,
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
    /// The token's ID, by which it is revoked: a delegated agent's delegation ID.
    pub(crate) token_id: String,
    /// When the token expires, in RFC 3339.
    pub(crate) token_expires_at: String,
    /// When the certificate expires, in RFC 3339; absent for a delegated agent, which has no
    /// certificate of its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cert_expires_at: Option<String>,
    /// What issued the credentials.
    pub(crate) issued_by: IssuedBy,
    /// The ID of the issuer key that signed a delegated agent's copy of its token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) issuer_key_id: Option<String>,
    /// The operation ID a delegated agent's token was asked for under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) operation: Option<String>,
    /// The lifetime in milliseconds a delegated agent's token was asked for with `--ttl`, which
    /// `agent renew` asks for again; absent when the control plane's default applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) lifetime_ms: Option<u64>,
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
        self.cert_expires_at
            .as_deref()
            .and_then(|text| crate::parse_time(text).ok())
            .map(Timestamp::from_millis)
    }

    /// Whether this is a delegated agent's record.
    pub(crate) fn is_delegated(&self) -> bool {
        self.issued_by == IssuedBy::Delegation
    }

    /// The ID of a root agent's token, which `token revoke --token-id` takes; `None` for a
    /// delegated agent, whose token ID is its delegation ID, or for a record that holds none.
    pub(crate) fn root_token_id(&self) -> Option<&str> {
        (!self.is_delegated() && !self.token_id.is_empty()).then_some(self.token_id.as_str())
    }

    /// Why this record can't be the identity of `principal`, an agent of `kind`, at its
    /// version, or `None` if it can: it names another principal, it says `scopes_from` at
    /// version 1, or it holds the fields of the other kind of identity.
    fn mismatch(&self, principal: &str, kind: Kind) -> Option<String> {
        if self.principal != principal {
            return Some(format!(
                "it names the principal '{}', not '{principal}'",
                self.principal
            ));
        }
        let needs = self.scopes_from.record_version();
        if self.version < needs {
            return Some(format!(
                "its version is {}, but scopes_from needs version {needs}",
                self.version
            ));
        }
        match kind {
            Kind::Root => {
                if self.is_delegated() {
                    return Some("a root agent's identity isn't issued by delegation".to_owned());
                }
                let delegated = [
                    ("parent", self.parent.is_some()),
                    ("root", self.root.is_some()),
                    ("issuer_key_id", self.issuer_key_id.is_some()),
                    ("operation", self.operation.is_some()),
                    ("lifetime_ms", self.lifetime_ms.is_some()),
                ];
                if let Some((field, _)) = delegated.iter().find(|(_, set)| *set) {
                    return Some(format!(
                        "a root agent's identity has no {field}, which only a delegated agent's \
                         holds"
                    ));
                }
                if self.cert_expires_at.is_none() {
                    return Some("it lacks cert_expires_at".to_owned());
                }
            }
            Kind::Delegated => {
                if !self.is_delegated() {
                    return Some("a delegated agent's identity is issued by delegation".to_owned());
                }
                if self.version < DELEGATED_VERSION {
                    return Some(format!(
                        "its version is {}, but a delegated agent's identity needs version \
                         {DELEGATED_VERSION}",
                        self.version
                    ));
                }
                if self.cert_expires_at.is_some() {
                    return Some(
                        "a delegated agent has no certificate, so its identity has no \
                         cert_expires_at"
                            .to_owned(),
                    );
                }
                let missing = [
                    ("parent", self.parent.is_none()),
                    ("root", self.root.is_none()),
                    ("issuer_key_id", self.issuer_key_id.is_none()),
                    ("operation", self.operation.is_none()),
                ];
                if let Some((field, _)) = missing.iter().find(|(_, absent)| *absent) {
                    return Some(format!("it lacks {field}"));
                }
                // The lineage is the directory's name's: `agent-42.c7.r1`'s parent is
                // `agent-42.c7` and its root `agent-42`. An edited record that names others
                // would act as, and revoke under, an agent it isn't.
                let lineage = [
                    ("parent", self.parent.as_deref(), principal.rsplit_once('.')),
                    ("root", self.root.as_deref(), principal.split_once('.')),
                ];
                for (field, recorded, expected) in lineage {
                    let expected = expected.map(|(name, _)| name);
                    if recorded != expected {
                        return Some(format!(
                            "its {field} is '{}', but {principal}'s is '{}'",
                            recorded.unwrap_or_default(),
                            expected.unwrap_or_default()
                        ));
                    }
                }
            }
        }
        None
    }
}

/// The `identity.toml` version a delegated agent's record is written at, and the least it may
/// say (ADR-0047, point 10).
const DELEGATED_VERSION: u32 = 2;

/// Which kind of agent an identity's directory holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A root agent: a certificate, a key and a token.
    Root,
    /// A delegated agent: a token only.
    Delegated,
}

/// Reads the record at `path`, of `principal`, an agent of `kind`, or `None` if there is none.
///
/// The format version is read first: a record from a newer release is refused with a message
/// that says to upgrade, never read in part (ADR-0044, point 1.7).
fn read_record(
    path: &Path,
    principal: &str,
    kind: Kind,
) -> Result<Option<AgentRecord>, AgentError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(AgentError::File {
                action: "read",
                path: path.to_owned(),
                source,
            });
        }
    };
    #[derive(Deserialize)]
    struct Header {
        version: Option<u32>,
    }
    let parse = |source| AgentError::Record {
        path: path.to_owned(),
        source,
    };
    let header: Header = toml::from_str(&text).map_err(parse)?;
    if header
        .version
        .is_some_and(|version| version > RECORD_VERSION)
    {
        return Err(AgentError::Newer {
            path: path.to_owned(),
        });
    }
    let record: AgentRecord = toml::from_str(&text).map_err(parse)?;
    if let Some(reason) = record.mismatch(principal, kind) {
        return Err(AgentError::Mismatch {
            path: path.to_owned(),
            reason,
        });
    }
    Ok(Some(record))
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
    /// The control plane, which recorded the delegation and signed the token (ADR-0047, point 5).
    Delegation,
}

impl fmt::Display for IssuedBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Dev => "dev",
            Self::Authority => "authority",
            Self::Delegation => "delegation",
        })
    }
}

/// Whether the fleet's members report the `delegation` fleet feature, which decides what a root
/// agent's token may carry (ADR-0047, points 2.6 and 12.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Support {
    /// The fleet has enabled `delegation`, or every member, live or down, reports it: the gate a
    /// control node applies to `DelegateToken` passes.
    Every,
    /// A member lacks it: the first, in ID order, and its release.
    Lacking { node: String, release: Release },
    /// The fleet couldn't be asked: no client, a caller without `fleet`, no answer, or no
    /// members.
    Unknown,
}

impl Support {
    /// What the membership the control plane sent says: `enabled` are the fleet features it
    /// has enabled for good, and `members` every member, live or down. It decides as the control
    /// node's feature gate does ([`fleetring_control::State::check_feature`]), so that a token
    /// minted to delegate is one the gate lets delegate.
    pub(crate) fn of<'m>(
        enabled: &[String],
        members: impl IntoIterator<Item = &'m Member>,
    ) -> Self {
        if enabled
            .iter()
            .any(|name| name == Feature::Delegation.as_str())
        {
            return Self::Every;
        }
        let mut any = false;
        for member in members {
            any = true;
            let software = member.running();
            if !software.supports(Feature::Delegation) {
                return Self::Lacking {
                    node: member.id().to_string(),
                    release: software.release().clone(),
                };
            }
        }
        if any { Self::Every } else { Self::Unknown }
    }

    /// How a root agent's token is minted on this fleet.
    pub(crate) fn minting(&self) -> Minting {
        match self {
            Self::Every => Minting::Delegating,
            Self::Lacking { .. } | Self::Unknown => Minting::Fr081,
        }
    }

    /// The note an identity minted without delegation gets, saying when agent `id` can
    /// delegate (ADR-0047, point 12.2); `None` once every member reports `delegation`.
    pub(crate) fn note(&self, id: &AgentId) -> Option<String> {
        let why = match self {
            Self::Every => return None,
            Self::Lacking { node, release } => {
                format!(
                    "node '{node}' runs fleetringd {release}, which doesn't report the \
                     delegation feature"
                )
            }
            Self::Unknown => "the fleet couldn't be asked whether every node can".to_owned(),
        };
        Some(format!(
            "note: agent {id} can delegate after `fleetring agent renew {id}` once every node \
             runs 0.8; {why}"
        ))
    }
}

/// Whether the fleet `client` reaches can mint root agents that delegate: the first update of
/// `WatchMembership`, which needs `fleet`. [`Support::Unknown`] without a client, or if the
/// fleet refuses, doesn't answer within [`PROBE_TIMEOUT`], or has no members.
pub(crate) async fn delegation_support(client: Option<&Client>) -> Support {
    let Some(client) = client else {
        return Support::Unknown;
    };
    match tokio::time::timeout(PROBE_TIMEOUT, client.status()).await {
        Ok(Ok(view)) => Support::of(view.features(), view.members().map(|(member, _)| member)),
        Ok(Err(_)) | Err(_) => Support::Unknown,
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
pub(crate) struct MintSpec {
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
    /// Whether the token says it is a root agent's, with `fleetring.kind` and
    /// `fleetring.owner`. A renewal takes the scopes from the record, and widens them only as
    /// [`AgentId::widened`] says for this.
    pub(crate) minting: Minting,
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
    /// They were `from`, 0.7's or FR-081's defaults, and are now the defaults the renewal
    /// minted with; `added` are the scopes the renewal added.
    Widened {
        from: HeldDefaults,
        added: Vec<String>,
    },
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
        read_record(
            &self.record_file(),
            self.id.principal().as_str(),
            Kind::Root,
        )
    }

    /// Mints a new identity from `issuer`: a certificate for `agent-<id>` and a token with
    /// `spec`'s scopes, both valid from `now`, with `ca.crt` and `identity.toml`. Holds the
    /// directory's lock while it works.
    ///
    /// # Errors
    ///
    /// [`AgentError::Exists`] if the directory holds an identity already (renew it instead), and
    /// the errors of reading the record, issuing, and writing.
    pub(crate) fn mint(
        &self,
        issuer: Issuer<'_>,
        spec: &MintSpec,
        now: Timestamp,
    ) -> Result<AgentRecord, AgentError> {
        let _lock = self.lock(Missing::Create)?;
        if self.record()?.is_some() {
            return Err(AgentError::Exists {
                id: self.id.to_string(),
                dir: self.dir.clone(),
            });
        }
        self.write_new(issuer, spec, now)
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
        spec: &MintSpec,
        now: Timestamp,
    ) -> Result<Acquired, AgentError> {
        let _lock = self.lock(Missing::Create)?;
        let Some(old) = self.record()? else {
            return self.write_new(issuer, spec, now).map(Acquired::Minted);
        };
        self.check_fleet(&old, issuer.authority())?;
        let renewal = self.renewal(&old, issuer.authority().certificate_pem(), now);
        if renewal == Renewal::None {
            return Ok(Acquired::Reused(old));
        }
        // Read before the new token replaces it.
        let old_token = renewal.token().then(|| self.old_token(&old, now));
        let record = self.reissue(issuer, &old, renewal, spec, now)?;
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
    /// The scopes are kept, except that an identity holding exactly 0.7's or FR-081's defaults
    /// gets the defaults `spec.minting` names, as [`AgentId::widened`] decides. Scopes of an
    /// identity's own are never widened. The token says it is an agent's when `spec.minting`
    /// is [`Minting::Delegating`], whatever its scopes.
    ///
    /// # Errors
    ///
    /// [`AgentError::NoIdentity`] if there is no identity to renew, [`AgentError::Mismatch`] if
    /// it is another fleet's, and the errors of issuing and writing.
    pub(crate) fn renew(
        &self,
        issuer: Issuer<'_>,
        spec: &MintSpec,
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
        let held = self.id.held_defaults(&old);
        let (old, scopes) = match (self.id.widened(&old, spec.minting), held) {
            (Some(widened), Some(from)) => {
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
                (record, ScopeRenewal::Widened { from, added })
            }
            _ => (old, ScopeRenewal::Kept),
        };
        let record = self.reissue(issuer, &old, Renewal::Both, spec, now)?;
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
        old_token_in(&self.token_file(), record, now)
    }

    /// The token in the directory, to present.
    ///
    /// # Errors
    ///
    /// [`AgentError::File`] if it can't be read.
    pub(crate) fn token(&self) -> Result<BearerToken, AgentError> {
        read_token(&self.token_file())
    }

    /// The certificate, its key and `ca.crt`, loaded, for a command that acts as the agent, or
    /// as one of its delegated agents, which connect with it.
    ///
    /// # Errors
    ///
    /// The error of loading them.
    pub(crate) fn identity(&self) -> Result<Identity, IdentityError> {
        Identity::load(&self.ca_file(), &self.cert_file(), &self.key_file())
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
        loose(&[
            (self.dir.clone(), "700"),
            (self.key_file(), "600"),
            (self.token_file(), "600"),
        ])
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

    /// The file, beside the directory and so outliving it, that holds when a replacement token
    /// may first be signed. `agent list` and the lineage scans skip it: no agent ID starts with
    /// a dot.
    fn replacement_file(&self) -> PathBuf {
        let name = format!("{REPLACEMENT_FILE_PREFIX}{}", self.id);
        self.dir.with_file_name(name)
    }

    /// Records that the control plane revoked this agent as a principal, so a token signed
    /// before `from` is refused by every node (see `revoke::replacement_from`). `agent remove`
    /// writes it as it deletes the directory, and [`replacement_wait`](Self::replacement_wait)
    /// reads it, so that `agent add`, `agent renew` and a `mcp --agent` mint can refuse before
    /// then instead of minting a token that no node accepts. It is advisory: it lives on this
    /// machine, and its time is this host's clock against the signing host's.
    ///
    /// # Errors
    ///
    /// [`AgentError::File`] if it can't be written.
    pub(crate) fn record_replacement_from(&self, from: Timestamp) -> Result<(), AgentError> {
        let path = self.replacement_file();
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(AgentError::File {
                    action: "remove",
                    path,
                    source,
                });
            }
        }
        write_new_private(&path, &format!("{}\n", from.as_millis()))
    }

    /// When a replacement token may first be signed, if that is still ahead of `now`; `None` once
    /// it has passed, which also deletes the record, and when there is none or it can't be read.
    pub(crate) fn replacement_wait(&self, now: Timestamp) -> Option<Timestamp> {
        let path = self.replacement_file();
        let text = fs::read_to_string(&path).ok()?;
        let from = Timestamp::from_millis(text.trim().parse().ok()?);
        if from > now {
            return Some(from);
        }
        // Best effort: a record left behind only repeats this check.
        let _ = fs::remove_file(&path);
        None
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
        lock_file(&self.dir)
    }

    /// Writes a whole new identity. The lock is held.
    fn write_new(
        &self,
        issuer: Issuer<'_>,
        spec: &MintSpec,
        now: Timestamp,
    ) -> Result<AgentRecord, AgentError> {
        let authority = issuer.authority();
        let written = self.write_credentials(
            authority,
            Renewal::Both,
            &spec.scopes,
            &spec.owner,
            spec,
            now,
        )?;
        let record = AgentRecord {
            version: spec.scopes_from.record_version(),
            principal: self.id.principal().to_string(),
            owner: spec.owner.clone(),
            parent: None,
            root: None,
            fleet: authority.fleet().to_string(),
            scopes: spec.scopes.iter().map(ToString::to_string).collect(),
            scopes_from: spec.scopes_from,
            read_only: spec.access == Access::ReadOnly,
            token_id: written.token_id.unwrap_or_default(),
            token_expires_at: written.token_expires_at.unwrap_or_default(),
            cert_expires_at: Some(written.cert_expires_at.unwrap_or_default()),
            issued_by: issuer.issued_by(),
            issuer_key_id: None,
            operation: None,
            lifetime_ms: None,
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
        spec: &MintSpec,
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
        let written =
            self.write_credentials(issuer.authority(), renewal, &scopes, &old.owner, spec, now)?;
        let record = AgentRecord {
            token_id: written.token_id.unwrap_or_else(|| old.token_id.clone()),
            token_expires_at: written
                .token_expires_at
                .unwrap_or_else(|| old.token_expires_at.clone()),
            cert_expires_at: written
                .cert_expires_at
                .or_else(|| old.cert_expires_at.clone()),
            issued_by: issuer.issued_by(),
            ..old.clone()
        };
        self.write_record(&record)?;
        Ok(record)
    }

    /// Issues and writes what `renewal` names: the certificate, its key and `ca.crt`, and the
    /// token with `scopes`, which says it is an agent's that answers to `owner` when
    /// `spec.minting` is [`Minting::Delegating`]. Returns the certificate's expiry, the token's
    /// ID and its expiry, for what it wrote.
    fn write_credentials(
        &self,
        authority: &Authority,
        renewal: Renewal,
        scopes: &[Scope],
        owner: &str,
        spec: &MintSpec,
        now: Timestamp,
    ) -> Result<Written, AgentError> {
        let principal = self.id.principal();
        let mut written = Written::default();
        let mut token = None;
        if renewal.token() {
            let ttl = spec.token_ttl.min(MAX_TOKEN_LIFETIME);
            let id = new_token_id()?;
            let expires_at = now.saturating_add(Millis::new(
                u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX),
            ));
            let plain = TokenSpec::new(
                principal.clone(),
                scopes.iter().cloned().collect(),
                id.clone(),
                now,
                expires_at,
            )
            // Signed now: `now` was read when the command started, before it found the node.
            .signed_at(SystemClock.now().max(now));
            let token_spec = match spec.minting {
                Minting::Fr081 => plain,
                Minting::Delegating => {
                    plain.agent(owner.parse().map_err(|source| AgentError::Owner {
                        owner: owner.to_owned(),
                        source,
                    })?)
                }
            };
            let issued = authority.token_signer()?.issue(&token_spec)?;
            token = Some((issued, id, expires_at));
        }
        if renewal.certificate() {
            let issued =
                authority.issue_client(&principal, Validity::issued_at(now, spec.cert_ttl))?;
            let lifetime =
                Millis::new(u64::try_from(spec.cert_ttl.as_millis()).unwrap_or(u64::MAX));
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
        write_record(&self.dir, &self.record_file(), self.id.as_str(), record)
    }
}

/// Writes `record`, agent `id`'s, to `path` in `dir`, and syncs `dir`.
fn write_record(dir: &Path, path: &Path, id: &str, record: &AgentRecord) -> Result<(), AgentError> {
    let text = toml::to_string(record).map_err(|source| AgentError::File {
        action: "write",
        path: path.to_owned(),
        source: io::Error::other(source),
    })?;
    let text =
        format!("# Written by fleetring: agent {id}'s identity. It holds no secret.\n{text}");
    write_private(path, text.as_bytes())?;
    sync_dir(dir)
}

/// Locks `dir`'s [`LOCK_FILE`], waiting for another holder; dropping the file releases it.
fn lock_file(dir: &Path) -> Result<File, AgentError> {
    let path = dir.join(LOCK_FILE);
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

/// A delegated agent's directory (ADR-0047, point 10): `<agents_dir>/<id>/`, holding its token,
/// its record, and the lock, and while `agent delegate` asks, the operation ID. It connects with
/// its root agent's certificate, in the root's directory.
#[derive(Debug, Clone)]
pub(crate) struct DelegatedDir {
    dir: PathBuf,
    id: DelegatedId,
    root: AgentDir,
}

/// A delegated agent's directory, locked, with the operation ID `agent delegate` asks under,
/// written into it before the call.
#[derive(Debug)]
pub(crate) struct Pending {
    /// Held until the record is written, so two runs at once ask once.
    _lock: File,
    operation: OperationId,
    from: OperationFrom,
}

impl Pending {
    /// The operation ID the request carries.
    pub(crate) fn operation(&self) -> OperationId {
        self.operation
    }

    /// Where the operation ID came from.
    pub(crate) fn from(&self) -> OperationFrom {
        self.from
    }
}

/// Where a [`Pending`] delegation's operation ID came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperationFrom {
    /// Made now, or given with `--operation-id`, and written into the directory.
    New,
    /// Read from the directory, where an earlier run that didn't finish left it.
    Earlier,
}

/// What the control plane answered a delegation with, and what the parent's record says, for
/// the delegated agent's record (ADR-0047, point 10).
#[derive(Debug, Clone)]
pub(crate) struct Delegation {
    /// The token, which only the token file ever holds.
    pub(crate) token: BearerToken,
    /// The principal the control plane named.
    pub(crate) principal: String,
    /// The delegation ID, the token's `jti`.
    pub(crate) delegation_id: String,
    /// The child's scopes, as recorded.
    pub(crate) scopes: Vec<String>,
    /// Whether `--scope` chose them.
    pub(crate) scopes_from: ScopesFrom,
    /// Whether `mcp --agent` serves only the reading tools.
    pub(crate) read_only: bool,
    /// When the token expires.
    pub(crate) expires_at: Timestamp,
    /// The ID of the issuer key that signed it.
    pub(crate) issuer_key_id: String,
    /// The lifetime asked for, which a renewal asks for again.
    pub(crate) lifetime_ms: Option<u64>,
    /// The parent's record, whose owner and fleet the child's are.
    pub(crate) parent: AgentRecord,
}

impl DelegatedDir {
    /// The directory for `id` under `agents_dir`.
    pub(crate) fn new(agents_dir: &Path, id: DelegatedId) -> Self {
        Self {
            dir: agents_dir.join(id.as_str()),
            root: AgentDir::new(agents_dir, id.root().clone()),
            id,
        }
    }

    /// The agent's ID.
    pub(crate) fn id(&self) -> &DelegatedId {
        &self.id
    }

    /// The directory itself.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// The root agent's directory, whose certificate and key the agent connects with.
    pub(crate) fn root(&self) -> &AgentDir {
        &self.root
    }

    /// The token's file.
    pub(crate) fn token_file(&self) -> PathBuf {
        self.dir.join(TOKEN_FILE)
    }

    /// The record's file.
    pub(crate) fn record_file(&self) -> PathBuf {
        self.dir.join(RECORD_FILE)
    }

    /// The file that holds the operation ID while `agent delegate` or `agent renew` asks.
    pub(crate) fn operation_file(&self) -> PathBuf {
        self.dir.join(OPERATION_FILE)
    }

    /// The identity's record, or `None` if the directory holds none.
    ///
    /// # Errors
    ///
    /// As [`AgentDir::record`]; [`AgentError::Mismatch`] also for a record that isn't a
    /// delegated agent's.
    pub(crate) fn record(&self) -> Result<Option<AgentRecord>, AgentError> {
        read_record(
            &self.record_file(),
            self.id.name().as_str(),
            Kind::Delegated,
        )
    }

    /// The record, which must exist, after checking that the root's, whose certificate the
    /// agent connects with, does too.
    ///
    /// # Errors
    ///
    /// [`AgentError::NoDelegated`] without a record, [`AgentError::NoRoot`] without the root's,
    /// and the errors of reading either.
    pub(crate) fn usable(&self) -> Result<AgentRecord, AgentError> {
        let record = self.record()?.ok_or_else(|| AgentError::NoDelegated {
            id: self.id.to_string(),
            dir: self.dir.clone(),
        })?;
        if self.root.record()?.is_none() {
            return Err(AgentError::NoRoot {
                id: self.id.to_string(),
                root: self.id.root().to_string(),
                dir: self.root.tls_dir().to_owned(),
            });
        }
        Ok(record)
    }

    /// The token in the directory, to revoke: its ID and expiry, read from the token itself, as
    /// [`AgentDir::old_token`] reads a root agent's.
    pub(crate) fn old_token(&self, record: &AgentRecord, now: Timestamp) -> OldToken {
        old_token_in(&self.token_file(), record, now)
    }

    /// The token in the directory, to present.
    ///
    /// # Errors
    ///
    /// [`AgentError::File`] if it can't be read.
    pub(crate) fn token(&self) -> Result<BearerToken, AgentError> {
        read_token(&self.token_file())
    }

    /// Locks the directory of an identity `agent renew` delegates again, and returns the
    /// operation ID to ask under: one an earlier renewal that didn't finish left there, or else a
    /// new one, written into the directory before this returns. [`finish`](Self::finish) then
    /// writes the new token and record over the old.
    ///
    /// # Errors
    ///
    /// [`AgentError::NoDelegated`] if the directory holds no record, and the errors of locking,
    /// reading, and writing.
    pub(crate) fn begin_renewal(&self) -> Result<Pending, AgentError> {
        let no_record = || AgentError::NoDelegated {
            id: self.id.to_string(),
            dir: self.dir.clone(),
        };
        if !self.dir.is_dir() {
            return Err(no_record());
        }
        let lock = lock_file(&self.dir)?;
        if self.record()?.is_none() {
            return Err(no_record());
        }
        let (operation, from) = self.operation_to_ask(None)?;
        Ok(Pending {
            _lock: lock,
            operation,
            from,
        })
    }

    /// Locks the directory, creating it `0700`, and returns the operation ID to ask under: one
    /// an earlier run left there, or else `given`, or else a new one, written into the directory
    /// before this returns, so a run after a crash asks under the same one.
    ///
    /// # Errors
    ///
    /// [`AgentError::DelegatedExists`] if the directory holds a record, and
    /// [`AgentError::OperationMismatch`] if it holds another operation ID than `given`; and the
    /// errors of creating, locking, reading, and writing.
    pub(crate) fn begin(&self, given: Option<OperationId>) -> Result<Pending, AgentError> {
        create_private_dir(&self.dir)?;
        // The directory's entry lasts a crash, as the operation ID written into it does.
        if let Some(agents_dir) = self.dir.parent() {
            sync_dir(agents_dir)?;
        }
        let lock = lock_file(&self.dir)?;
        if self.record_file().exists() {
            return Err(AgentError::DelegatedExists {
                id: self.id.to_string(),
                dir: self.dir.clone(),
            });
        }
        let (operation, from) = self.operation_to_ask(given)?;
        Ok(Pending {
            _lock: lock,
            operation,
            from,
        })
    }

    /// The operation ID to ask under, with the directory locked: one an earlier run left in it,
    /// or else `given`, or else a new one, written into the directory before this returns.
    fn operation_to_ask(
        &self,
        given: Option<OperationId>,
    ) -> Result<(OperationId, OperationFrom), AgentError> {
        let path = self.operation_file();
        let earlier = match fs::read_to_string(&path) {
            // A crash between creating the file and syncing its contents can leave it empty:
            // nothing was sent under an ID nobody wrote.
            Ok(text) if text.trim().is_empty() => {
                fs::remove_file(&path).map_err(|source| AgentError::File {
                    action: "remove",
                    path: path.clone(),
                    source,
                })?;
                None
            }
            Ok(text) => Some(
                text.trim()
                    .parse::<OperationId>()
                    .map_err(|_| AgentError::Operation { path: path.clone() })?,
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(source) => {
                return Err(AgentError::File {
                    action: "read",
                    path,
                    source,
                });
            }
        };
        Ok(match (earlier, given) {
            (Some(found), Some(given)) if found != given => {
                return Err(AgentError::OperationMismatch {
                    path,
                    found: found.to_string(),
                    given: given.to_string(),
                });
            }
            (Some(found), _) => (found, OperationFrom::Earlier),
            (None, given) => {
                let operation = match given {
                    Some(given) => given,
                    None => new_operation_id()?,
                };
                write_new_private(&path, &format!("{operation}\n"))?;
                sync_dir(&self.dir)?;
                (operation, OperationFrom::New)
            }
        })
    }

    /// Writes what the control plane answered `pending`'s request with: the token first, then
    /// the record (version 2, `issued_by = "delegation"`), then removes the operation ID, which
    /// the record now holds.
    ///
    /// # Errors
    ///
    /// [`AgentError::WrongChild`] if the answer names another principal, and the errors of
    /// writing.
    pub(crate) fn finish(
        &self,
        pending: Pending,
        delegation: Delegation,
    ) -> Result<AgentRecord, AgentError> {
        let want = self.id.name().as_str();
        if delegation.principal != want {
            return Err(AgentError::WrongChild {
                want: want.to_owned(),
                got: delegation.principal,
            });
        }
        let mut text = delegation.token.expose().to_owned();
        text.push('\n');
        write_private(&self.token_file(), text.as_bytes())?;
        let parent = match self.id.parent() {
            AgentRef::Root(id) => id.principal().to_string(),
            AgentRef::Delegated(id) => id.name().to_string(),
        };
        let record = AgentRecord {
            version: DELEGATED_VERSION,
            principal: want.to_owned(),
            owner: delegation.parent.owner.clone(),
            parent: Some(parent),
            root: Some(self.id.root().principal().to_string()),
            fleet: delegation.parent.fleet.clone(),
            scopes: delegation.scopes,
            scopes_from: delegation.scopes_from,
            read_only: delegation.read_only,
            token_id: delegation.delegation_id,
            token_expires_at: crate::format_time(delegation.expires_at.as_millis()),
            cert_expires_at: None,
            issued_by: IssuedBy::Delegation,
            issuer_key_id: Some(delegation.issuer_key_id),
            operation: Some(pending.operation.to_string()),
            lifetime_ms: delegation.lifetime_ms,
        };
        write_record(&self.dir, &self.record_file(), self.id.as_str(), &record)?;
        match fs::remove_file(self.operation_file()) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(AgentError::File {
                    action: "remove",
                    path: self.operation_file(),
                    source,
                });
            }
        }
        sync_dir(&self.dir)?;
        drop(pending);
        Ok(record)
    }

    /// Gives `pending` up after the control plane refused it, recording nothing: removes the
    /// operation ID, under the lock, so the next run asks under a new one. The lock file stays,
    /// so a run waiting on it never races one that opens a new one.
    ///
    /// # Errors
    ///
    /// [`AgentError::File`] if the operation ID can't be removed.
    pub(crate) fn abandon(&self, pending: Pending) -> Result<(), AgentError> {
        match fs::remove_file(self.operation_file()) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(AgentError::File {
                    action: "remove",
                    path: self.operation_file(),
                    source,
                });
            }
        }
        sync_dir(&self.dir)?;
        drop(pending);
        Ok(())
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

    /// One warning for each of the directory and the token, and the root's directory and key,
    /// that others than its owner may read.
    pub(crate) fn loose_files(&self) -> Vec<String> {
        loose(&[
            (self.dir.clone(), "700"),
            (self.token_file(), "600"),
            (self.root.tls_dir().to_owned(), "700"),
            (self.root.key_file(), "600"),
        ])
    }
}

/// The token in `path`, to revoke: its ID and expiry, read from the token itself, or, if it
/// can't be read, `record`'s ID, revoked until the latest any token issued by `now` could last.
fn old_token_in(path: &Path, record: &AgentRecord, now: Timestamp) -> OldToken {
    let claimed = fs::read_to_string(path)
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
                until: now.saturating_add(Millis::new(u64::try_from(longest).unwrap_or(u64::MAX))),
            }
        }
    }
}

/// The token in `path`, trimmed.
fn read_token(path: &Path) -> Result<BearerToken, AgentError> {
    fs::read_to_string(path)
        .map(|text| BearerToken::new(text.trim()))
        .map_err(|source| AgentError::File {
            action: "read",
            path: path.to_owned(),
            source,
        })
}

/// A new operation ID: 16 random bytes.
///
/// # Errors
///
/// [`AgentError::Random`] if the system's random number generator fails.
fn new_operation_id() -> Result<OperationId, AgentError> {
    let mut random = [0_u8; 16];
    SystemRandom::new()
        .fill(&mut random)
        .map_err(|_| AgentError::Random)?;
    Ok(OperationId::from_bytes(random))
}

/// One warning for each of `paths` that others than its owner may read, with the mode it
/// should have (`docs/cli.md`, section 9): a copy or a `chmod` may have loosened them.
fn loose(paths: &[(PathBuf, &str)]) -> Vec<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        paths
            .iter()
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
    {
        let _ = paths;
        Vec::new()
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

/// The note printed when a token is minted with `fork` or `grant`, which nodes before 0.8
/// refuse as words reserved for delegation (ADR-0043, point 8.11), or `None`.
pub(crate) fn pre_0_8_note(scopes: &[Scope]) -> Option<&'static str> {
    scopes
        .iter()
        .any(|scope| matches!(scope.action(), Some(Action::Fork | Action::Grant)))
        .then_some("note: nodes before 0.8 refuse this token, since it carries fork or grant")
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
        "'{id}' is a delegated agent's ID, which this command doesn't take: delegated agents are \
         created by `fleetring agent delegate`"
    )]
    Delegated { id: String },
    #[error("invalid delegated agent ID '{id}': {reason}")]
    InvalidDelegatedId { id: String, reason: String },
    #[error(
        "agent '{id}' has no identity in {}\n  hint: create it with `fleetring agent delegate \
         {id}`, as its parent",
        dir.display()
    )]
    NoDelegated { id: String, dir: PathBuf },
    #[error(
        "agent '{id}' already has an identity in {}\n  hint: pick another segment, or run \
         `fleetring agent renew {id}`",
        dir.display()
    )]
    DelegatedExists { id: String, dir: PathBuf },
    #[error(
        "{} holds operation {found} from an earlier run, not {given}\n  hint: pass \
         `--operation-id {found}` to resend that request, or leave --operation-id out",
        path.display()
    )]
    OperationMismatch {
        path: PathBuf,
        found: String,
        given: String,
    },
    #[error("{} isn't an operation ID", path.display())]
    Operation { path: PathBuf },
    #[error(
        "agent {id} connects with its root agent {root}'s certificate, but {root} has no identity \
         in {}\n  hint: delegated agents run where their root agent's identity is",
        dir.display()
    )]
    NoRoot {
        id: String,
        root: String,
        dir: PathBuf,
    },
    #[error("the control plane answered for '{got}', not '{want}', so nothing was written")]
    WrongChild { want: String, got: String },
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
    #[error(
        "the identity's owner '{owner}' isn't a principal name, so its token can't name it as \
         the agent's owner"
    )]
    Owner {
        owner: String,
        #[source]
        source: NameError,
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
    use fleetring_control::Software;
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

    fn spec(agent: &AgentId) -> MintSpec {
        MintSpec {
            scopes: agent.default_scopes(Access::ReadWrite, Minting::Fr081),
            scopes_from: ScopesFrom::Defaults,
            access: Access::ReadWrite,
            cert_ttl: DEV_CERT_LIFETIME,
            token_ttl: MAX_TOKEN_LIFETIME,
            owner: "admin".to_owned(),
            minting: Minting::Fr081,
        }
    }

    /// A token's claims, read without verifying it.
    fn claims_of(dir: &AgentDir) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let token = fs::read_to_string(dir.token_file())?;
        let payload = token.trim().split('.').nth(1).ok_or("a token")?;
        Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload)?)?)
    }

    fn texts(scopes: &[Scope]) -> Vec<String> {
        scopes.iter().map(ToString::to_string).collect()
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
            "42.",
            "42.C7",
            "Agent",
            "a b",
            "é",
            "../x",
            &"9".repeat(MAX_ID_LEN + 1),
        ] {
            let error = invalid.parse::<AgentId>().expect_err(invalid);
            assert!(error.to_string().contains("invalid agent ID"), "{error}");
        }
        // A delegated agent's ID is no root agent's, and the refusal names `agent delegate`.
        let error = "42.c7".parse::<AgentId>().expect_err("a delegated ID");
        assert!(
            error
                .to_string()
                .contains("delegated agents are created by `fleetring agent delegate`"),
            "{error}"
        );
        let agent = id("42");
        assert_eq!(agent.principal().as_str(), "agent-42");
        assert!(!agent.principal().is_reserved_agent_name());
        let shown = |access, minting| texts(&agent.default_scopes(access, minting));
        assert_eq!(
            shown(Access::ReadWrite, Minting::Fr081),
            ["admin:agent-42-*", "stream:agent-42-*"]
        );
        assert_eq!(
            shown(Access::ReadOnly, Minting::Fr081),
            ["read:agent-42-*", "stream:agent-42-*"]
        );
        // Once every member reports delegation: the same actions on the agent's subtree, and
        // grant on both (ADR-0047, point 2.6).
        assert_eq!(
            shown(Access::ReadWrite, Minting::Delegating),
            [
                "admin:agent-42-*",
                "admin:agent-42.*",
                "stream:agent-42-*",
                "stream:agent-42.*",
                "grant:agent-42-*",
                "grant:agent-42.*"
            ]
        );
        assert_eq!(
            shown(Access::ReadOnly, Minting::Delegating),
            [
                "read:agent-42-*",
                "read:agent-42.*",
                "stream:agent-42-*",
                "stream:agent-42.*",
                "grant:agent-42-*",
                "grant:agent-42.*"
            ]
        );
        for access in [Access::ReadWrite, Access::ReadOnly] {
            for minting in [Minting::Fr081, Minting::Delegating] {
                for scope in agent.default_scopes(access, minting) {
                    assert_eq!(scope.separator_warning(), None, "{scope}");
                }
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
            root: None,
            fleet: "dev".to_owned(),
            scopes: scopes.iter().map(|&scope| scope.to_owned()).collect(),
            scopes_from,
            read_only: access == Access::ReadOnly,
            token_id: "t1".to_owned(),
            token_expires_at: "2026-12-01T00:00:00Z".to_owned(),
            cert_expires_at: Some("2027-11-01T00:00:00Z".to_owned()),
            issued_by: IssuedBy::Dev,
            issuer_key_id: None,
            operation: None,
            lifetime_ms: None,
        }
    }

    #[test]
    fn only_exactly_0_7_or_fr081_defaults_are_widened() {
        let agent = id("42");
        let widened = |record: &AgentRecord, minting| -> Option<Vec<String>> {
            agent.widened(record, minting).map(|scopes| texts(&scopes))
        };
        use Access::{ReadOnly, ReadWrite};
        use Minting::{Delegating, Fr081};
        use ScopesFrom::{Defaults, Flags};
        let v07 = |access| match access {
            ReadWrite => vec!["admin:agent-42-*"],
            ReadOnly => vec!["read:agent-42-*"],
        };
        let defaults = |access, minting| texts(&agent.default_scopes(access, minting));
        for access in [ReadWrite, ReadOnly] {
            let fr081 = defaults(access, Fr081);
            let fr081: Vec<&str> = fr081.iter().map(String::as_str).collect();
            let delegating = defaults(access, Delegating);
            let delegating: Vec<&str> = delegating.iter().map(String::as_str).collect();
            // 0.7's default becomes the defaults the renewal mints with; FR-081's become this
            // release's once the agent can delegate, and stay as they are until then.
            for (held, minting, want) in [
                (&v07(access), Fr081, Some(fr081.clone())),
                (&v07(access), Delegating, Some(delegating.clone())),
                (&fr081, Fr081, None),
                (&fr081, Delegating, Some(delegating.clone())),
                (&delegating, Fr081, None),
                (&delegating, Delegating, None),
            ] {
                let record = record_with(held, Defaults, access);
                let want: Option<Vec<String>> =
                    want.map(|scopes| scopes.iter().map(|&scope| scope.to_owned()).collect());
                assert_eq!(widened(&record, minting), want, "{held:?} {minting:?}");
            }
            // In another order, they are the same set.
            let mut shuffled = delegating.clone();
            shuffled.reverse();
            assert_eq!(
                agent.held_defaults(&record_with(&shuffled, Defaults, access)),
                Some(HeldDefaults::Delegating)
            );
            // Chosen with --scope, even the defaults are never widened.
            for held in [v07(access), fr081.clone()] {
                let record = record_with(&held, Flags, access);
                assert_eq!(agent.held_defaults(&record), None);
                assert_eq!(widened(&record, Delegating), None, "{held:?}");
            }
        }
        // Everything else stays as it is.
        for record in [
            // FR-081's defaults, chosen with --scope.
            record_with(&["admin:agent-42-*", "stream:agent-42-*"], Flags, ReadWrite),
            // The old default, chosen with --scope.
            record_with(&["admin:agent-42-*"], Flags, ReadWrite),
            // A subset of this release's defaults.
            record_with(
                &["admin:agent-42-*", "stream:agent-42-*", "grant:agent-42.*"],
                Defaults,
                ReadWrite,
            ),
            // Another access's default.
            record_with(&["read:agent-42-*"], Defaults, ReadWrite),
            record_with(&["admin:agent-42-*"], Defaults, ReadOnly),
            // Scopes of its own, narrower, wider, or another agent's.
            record_with(&["write:agent-42-*"], Defaults, ReadWrite),
            record_with(&["admin:agent-42-*", "read:x-*"], Defaults, ReadWrite),
            record_with(&["admin:agent-4-*"], Defaults, ReadWrite),
            record_with(&[], Defaults, ReadWrite),
        ] {
            for minting in [Fr081, Delegating] {
                assert_eq!(widened(&record, minting), None, "{record:?}");
            }
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
        let old = MintSpec {
            scopes: vec!["admin:agent-42-*".parse()?],
            ..spec(&agent)
        };
        dir.mint(Issuer::Authority(&issuing), &old, now)?;
        let renewed = dir.renew(Issuer::Authority(&issuing), &spec(&agent), now)?;
        assert_eq!(
            renewed.scopes,
            ScopeRenewal::Widened {
                from: HeldDefaults::V07,
                added: vec!["stream:agent-42-*".to_owned()]
            }
        );
        assert_eq!(
            renewed.record.scopes,
            ["admin:agent-42-*", "stream:agent-42-*"]
        );
        assert_eq!(renewed.record.version, 1, "a 0.7 reader still reads it");
        // The token carries the new scope, and says nothing a 0.7 node refuses.
        let claims = claims_of(&dir)?;
        assert_eq!(
            claims["fleetring.scopes"],
            serde_json::json!(["admin:agent-42-*", "stream:agent-42-*"])
        );
        assert_eq!(claims.get("fleetring.kind"), None, "{claims}");
        assert_eq!(claims.get("fleetring.owner"), None, "{claims}");
        // A second renewal has nothing to add.
        let again = dir.renew(Issuer::Authority(&issuing), &spec(&agent), now)?;
        assert_eq!(again.scopes, ScopeRenewal::Kept);
        assert_eq!(again.record.scopes, renewed.record.scopes);

        // Once every member reports delegation, FR-081's defaults become this release's, and
        // the token says it is an agent's that answers to the record's owner.
        let delegating = MintSpec {
            minting: Minting::Delegating,
            owner: String::new(),
            ..spec(&agent)
        };
        let upgraded = dir.renew(Issuer::Authority(&issuing), &delegating, now)?;
        let defaults = texts(&agent.default_scopes(Access::ReadWrite, Minting::Delegating));
        assert_eq!(
            upgraded.scopes,
            ScopeRenewal::Widened {
                from: HeldDefaults::Fr081,
                added: vec![
                    "admin:agent-42.*".to_owned(),
                    "stream:agent-42.*".to_owned(),
                    "grant:agent-42-*".to_owned(),
                    "grant:agent-42.*".to_owned(),
                ]
            }
        );
        assert_eq!(upgraded.record.scopes, defaults);
        assert_eq!(upgraded.record.version, 1, "still the defaults");
        let claims = claims_of(&dir)?;
        assert_eq!(claims["fleetring.scopes"], serde_json::json!(defaults));
        assert_eq!(claims["fleetring.kind"], "agent");
        assert_eq!(claims["fleetring.owner"], "admin");
        let again = dir.renew(Issuer::Authority(&issuing), &delegating, now)?;
        assert_eq!(again.scopes, ScopeRenewal::Kept);
        // A renewal while a member lacks delegation keeps them, without the claims.
        let kept = dir.renew(Issuer::Authority(&issuing), &spec(&agent), now)?;
        assert_eq!(kept.scopes, ScopeRenewal::Kept);
        assert_eq!(kept.record.scopes, defaults);
        assert_eq!(claims_of(&dir)?.get("fleetring.kind"), None);

        // The same scope chosen with --scope is recorded so, at version 2, and kept.
        let chosen = AgentDir::new(root.path(), id("43"));
        let flags = MintSpec {
            scopes: vec!["admin:agent-43-*".parse()?],
            scopes_from: ScopesFrom::Flags,
            ..spec(&id("43"))
        };
        let record = chosen.mint(Issuer::Authority(&issuing), &flags, now)?;
        assert_eq!(record.version, 2);
        let text = fs::read_to_string(chosen.record_file())?;
        assert!(text.contains("scopes_from = \"flags\""), "{text}");
        let kept = chosen.renew(Issuer::Authority(&issuing), &spec(&id("43")), now)?;
        assert_eq!(kept.scopes, ScopeRenewal::Kept);
        assert_eq!(kept.record.scopes, ["admin:agent-43-*"]);
        assert_eq!(kept.record.version, 2);
        assert_eq!(chosen.record()?, Some(kept.record));
        // Never widened once the agent can delegate either, though the token then says it is
        // an agent's.
        let delegating = MintSpec {
            minting: Minting::Delegating,
            ..spec(&id("43"))
        };
        let kept = chosen.renew(Issuer::Authority(&issuing), &delegating, now)?;
        assert_eq!(kept.scopes, ScopeRenewal::Kept);
        assert_eq!(kept.record.scopes, ["admin:agent-43-*"]);
        assert_eq!(claims_of(&chosen)?["fleetring.kind"], "agent");
        Ok(())
    }

    #[test]
    fn support_follows_the_control_nodes_feature_gate() -> Result<(), Box<dyn std::error::Error>> {
        let release = Release::new(0, 8, 0);
        let delegating = Software::new(release.clone(), 2).with_feature(Feature::Delegation);
        let member =
            |id: &str, software: Option<Software>| -> Result<Member, Box<dyn std::error::Error>> {
                let member = Member::new(id.parse()?);
                Ok(match software {
                    Some(software) => member.with_software(software),
                    None => member,
                })
            };
        let every = [
            member("a", Some(delegating.clone()))?,
            member("b", Some(delegating.clone()))?,
        ];
        assert_eq!(Support::of(&[], &every), Support::Every);
        assert_eq!(Support::Every.minting(), Minting::Delegating);
        assert_eq!(Support::Every.note(&id("42")), None);
        // The first member, in the order given, that lacks it, with its release; one that
        // reports nothing runs 0.3.0.
        let mixed = [
            member("a", Some(delegating.clone()))?,
            member("b", Some(Software::new(Release::new(0, 7, 2), 1)))?,
            member("c", None)?,
        ];
        let lacking = Support::of(&[], &mixed);
        assert_eq!(
            lacking,
            Support::Lacking {
                node: "b".to_owned(),
                release: Release::new(0, 7, 2)
            }
        );
        assert_eq!(lacking.minting(), Minting::Fr081);
        assert_eq!(
            lacking.note(&id("42")).as_deref(),
            Some(
                "note: agent 42 can delegate after `fleetring agent renew 42` once every node \
                 runs 0.8; node 'b' runs fleetringd 0.7.2, which doesn't report the delegation \
                 feature"
            )
        );
        assert_eq!(
            Support::of(&[], &[member("c", None)?]),
            Support::Lacking {
                node: "c".to_owned(),
                release: Release::new(0, 3, 0)
            }
        );
        // Enabled for good, whoever lacks it; with no members, nobody can be shown to have it.
        assert_eq!(
            Support::of(&["delegation".to_owned()], &mixed),
            Support::Every
        );
        assert_eq!(Support::of(&[], &[]), Support::Unknown);
        assert_eq!(Support::Unknown.minting(), Minting::Fr081);
        assert!(Support::Unknown.note(&id("42")).is_some_and(|note| {
            note.ends_with("the fleet couldn't be asked whether every node can")
        }),);
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
        let record = dir.mint(Issuer::Authority(&issuing), &spec(&agent), now)?;
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
            .mint(Issuer::Authority(&issuing), &spec(&agent), now)
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
        let record = dir.mint(Issuer::Authority(&issuing), &spec(&agent), now)?;
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
    fn a_replacement_time_outlives_the_directory_and_ends_when_it_passes()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let agents = root.path().join("agents");
        fs::create_dir(&agents)?;
        let dir = AgentDir::new(&agents, id("42"));
        let from = Timestamp::from_millis(1_800_000_031_000);
        assert_eq!(dir.replacement_wait(from), None, "nothing was recorded");

        dir.record_replacement_from(from)?;
        // Recording again replaces the time.
        let later = Timestamp::from_millis(1_800_000_061_000);
        dir.record_replacement_from(later)?;
        assert_eq!(
            dir.replacement_wait(Timestamp::from_millis(1_800_000_000_000)),
            Some(later)
        );
        assert_eq!(fs::read_dir(&agents)?.count(), 1, "one file for the agent");
        // Another agent has its own.
        assert_eq!(
            AgentDir::new(&agents, id("43")).replacement_wait(from),
            None
        );
        assert_eq!(dir.replacement_wait(later), None, "the time has come");
        assert_eq!(fs::read_dir(&agents)?.count(), 0, "and the record is gone");
        Ok(())
    }

    #[test]
    fn renewing_a_missing_identity_creates_nothing() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let dir = AgentDir::new(&root.path().join("agents"), id("9"));
        let issuing = authority("dev");
        let renewed = dir.renew(
            Issuer::Authority(&issuing),
            &spec(&id("9")),
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

    fn delegated(text: &str) -> DelegatedId {
        text.parse().expect("a delegated agent's ID")
    }

    #[test]
    fn delegated_ids_name_their_parent_root_and_segment() {
        let child = delegated("42.c7");
        assert_eq!(child.name().as_str(), "agent-42.c7");
        assert_eq!(child.root(), &id("42"));
        assert_eq!(child.segment(), "c7");
        assert_eq!(child.parent(), AgentRef::Root(id("42")));
        let grandchild = delegated("42.c7.r1");
        assert_eq!(grandchild.segment(), "r1");
        assert_eq!(grandchild.parent(), AgentRef::Delegated(child.clone()));
        assert_eq!(grandchild.root(), &id("42"));
        // Eight levels below the root at most, within a 64-byte principal.
        let deep = format!("4{}", ".a".repeat(8));
        assert!(deep.parse::<DelegatedId>().is_ok(), "{deep}");
        for invalid in [
            "42",
            "42.",
            "42..c7",
            "42.c-7",
            "42.C7",
            "4-2.c7",
            &format!("4{}", ".a".repeat(9)),
            &format!("42.{}", "c".repeat(33)),
            &format!("42.{}.{}", "c".repeat(30), "d".repeat(30)),
        ] {
            let error = invalid.parse::<DelegatedId>().expect_err(invalid);
            assert!(
                error.to_string().contains("invalid delegated agent ID"),
                "{invalid}: {error}"
            );
        }
        // An agent the command line names is either.
        assert_eq!(
            "42".parse::<AgentRef>().ok(),
            Some(AgentRef::Root(id("42")))
        );
        assert_eq!(
            "42.c7".parse::<AgentRef>().ok(),
            Some(AgentRef::Delegated(child))
        );
        let mut order: Vec<AgentRef> = ["43", "42.c7.r1", "420", "42", "42.c7", "4"]
            .iter()
            .map(|text| text.parse().expect("an agent"))
            .collect();
        order.sort_by(|left, right| left.segments().cmp(&right.segments()));
        let order: Vec<&str> = order.iter().map(AgentRef::as_str).collect();
        assert_eq!(order, ["4", "42", "42.c7", "42.c7.r1", "420", "43"]);
    }

    /// A delegation's answer for `dir`'s child, of a parent whose record is `parent`.
    fn delegation(dir: &DelegatedDir, parent: AgentRecord) -> Delegation {
        Delegation {
            token: BearerToken::new("header.payload.signature"),
            principal: dir.id().name().to_string(),
            delegation_id: "Zq3v0b8yTfKx1nH2aLmQwA".to_owned(),
            scopes: vec![format!("admin:{}-*", dir.id().name())],
            scopes_from: ScopesFrom::Defaults,
            read_only: false,
            expires_at: Timestamp::from_millis(1_800_007_200_000),
            issuer_key_id: "issuer-key".to_owned(),
            lifetime_ms: Some(1_200_000),
            parent,
        }
    }

    #[cfg(unix)]
    #[test]
    fn child_directory_holds_only_token_identity_and_lock() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::unix::fs::PermissionsExt as _;
        let root = tempfile::tempdir()?;
        let parent = record_with(
            &["admin:agent-42-*"],
            ScopesFrom::Defaults,
            Access::ReadWrite,
        );
        let dir = DelegatedDir::new(root.path(), delegated("42.c7"));

        // The operation ID is on disk before the call; a run after a crash finds it again.
        let first = dir.begin(None)?;
        let operation = first.operation();
        assert_eq!(first.from(), OperationFrom::New);
        let written = fs::read_to_string(dir.dir().join(OPERATION_FILE))?;
        assert_eq!(written.trim(), operation.to_string());
        drop(first);
        let again = dir.begin(None)?;
        assert_eq!(again.operation(), operation);
        assert_eq!(again.from(), OperationFrom::Earlier);
        drop(again);
        // Another ID than the one left there is refused, naming both.
        let other = OperationId::from_bytes([9; 16]);
        let mismatch = dir.begin(Some(other)).expect_err("another operation ID");
        assert!(
            mismatch
                .to_string()
                .contains(&format!("--operation-id {operation}")),
            "{mismatch}"
        );

        let pending = dir.begin(Some(operation))?;
        let record = dir.finish(pending, delegation(&dir, parent))?;
        assert_eq!(record.version, 2);
        assert_eq!(record.principal, "agent-42.c7");
        assert_eq!(record.owner, "admin");
        assert_eq!(record.parent.as_deref(), Some("agent-42"));
        assert_eq!(record.root.as_deref(), Some("agent-42"));
        assert_eq!(record.token_id, "Zq3v0b8yTfKx1nH2aLmQwA");
        assert_eq!(record.issuer_key_id.as_deref(), Some("issuer-key"));
        let operation_text = operation.to_string();
        assert_eq!(record.operation.as_deref(), Some(operation_text.as_str()));
        assert_eq!(record.cert_expires_at, None);
        assert_eq!(record.issued_by, IssuedBy::Delegation);
        assert_eq!(dir.record()?, Some(record));

        let mut names: Vec<String> = fs::read_dir(dir.dir())?
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<Result<_, _>>()?;
        names.sort();
        assert_eq!(names, [".lock", "identity.toml", "token"]);
        let mode = |path: &Path| -> io::Result<u32> {
            Ok(fs::metadata(path)?.permissions().mode() & 0o777)
        };
        assert_eq!(mode(dir.dir())?, 0o700);
        assert_eq!(mode(&dir.token_file())?, 0o600);
        assert_eq!(mode(&dir.record_file())?, 0o600);
        let text = fs::read_to_string(dir.record_file())?;
        assert!(!text.contains("header.payload.signature"), "{text}");
        assert!(text.contains("issued_by = \"delegation\""), "{text}");
        assert!(!text.contains("cert_expires_at"), "{text}");
        // A second delegation of the same child is refused before it asks.
        let exists = dir.begin(None).expect_err("an identity exists");
        assert!(
            exists.to_string().contains("pick another segment"),
            "{exists}"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn an_empty_operation_file_is_absent_and_a_written_one_private()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt as _;
        let root = tempfile::tempdir()?;
        let dir = DelegatedDir::new(root.path(), delegated("42.c7"));
        fs::create_dir_all(dir.dir())?;
        // A crash before the ID reached the disk leaves the file empty.
        fs::write(dir.operation_file(), "")?;
        let pending = dir.begin(None)?;
        assert_eq!(pending.from(), OperationFrom::New);
        let written = fs::read_to_string(dir.operation_file())?;
        assert_eq!(written.trim(), pending.operation().to_string());
        assert_eq!(
            fs::metadata(dir.operation_file())?.permissions().mode() & 0o777,
            0o600
        );
        Ok(())
    }

    #[test]
    fn a_child_record_naming_another_parent_or_root_is_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let parent = record_with(
            &["admin:agent-42-*"],
            ScopesFrom::Defaults,
            Access::ReadWrite,
        );
        let dir = DelegatedDir::new(root.path(), delegated("42.c7.r1"));
        let pending = dir.begin(None)?;
        let record = dir.finish(pending, delegation(&dir, parent))?;
        assert_eq!(record.parent.as_deref(), Some("agent-42.c7"));
        assert_eq!(record.root.as_deref(), Some("agent-42"));
        assert_eq!(dir.record()?, Some(record.clone()));
        for edited in [
            AgentRecord {
                parent: Some("agent-42".to_owned()),
                ..record.clone()
            },
            AgentRecord {
                root: Some("agent-43".to_owned()),
                ..record.clone()
            },
        ] {
            fs::write(dir.record_file(), toml::to_string(&edited)?)?;
            let refused = dir.record().expect_err("another lineage");
            assert!(
                matches!(&refused, AgentError::Mismatch { reason, .. } if reason.contains("but agent-42.c7.r1's is")),
                "{refused}"
            );
        }
        Ok(())
    }

    #[test]
    fn a_renewal_asks_under_a_new_operation_and_writes_over_the_record()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let parent = record_with(
            &["admin:agent-42-*"],
            ScopesFrom::Defaults,
            Access::ReadWrite,
        );
        let dir = DelegatedDir::new(root.path(), delegated("42.c7"));
        assert!(matches!(
            dir.begin_renewal(),
            Err(AgentError::NoDelegated { .. })
        ));
        let pending = dir.begin(None)?;
        let first = pending.operation();
        let record = dir.finish(pending, delegation(&dir, parent.clone()))?;
        let renewal = dir.begin_renewal()?;
        assert_ne!(renewal.operation(), first);
        let second = renewal.operation();
        drop(renewal);
        // An interrupted renewal is resent under its own operation ID.
        let again = dir.begin_renewal()?;
        assert_eq!(
            (again.operation(), again.from()),
            (second, OperationFrom::Earlier)
        );
        let renewed = dir.finish(
            again,
            Delegation {
                delegation_id: "Ab3v0b8yTfKx1nH2aLmQwA".to_owned(),
                ..delegation(&dir, parent)
            },
        )?;
        assert_eq!(renewed.token_id, "Ab3v0b8yTfKx1nH2aLmQwA");
        assert_eq!(renewed.operation, Some(second.to_string()));
        assert_ne!(renewed.operation, record.operation);
        assert!(!dir.operation_file().exists());
        Ok(())
    }

    #[test]
    fn version_2_child_record_refused_by_the_0_7_version_gate()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let parent = record_with(
            &["admin:agent-42-*"],
            ScopesFrom::Defaults,
            Access::ReadWrite,
        );
        let dir = DelegatedDir::new(root.path(), delegated("42.c7"));
        let pending = dir.begin(None)?;
        dir.finish(pending, delegation(&dir, parent))?;
        let text = fs::read_to_string(dir.record_file())?;
        // 0.7 read at most version 1, and refused a later one with "upgrade" before reading
        // anything else (ADR-0044, point 1.7).
        const RECORD_VERSION_0_7: i64 = 1;
        let version = text
            .parse::<toml::Table>()?
            .get("version")
            .and_then(toml::Value::as_integer)
            .ok_or("a version")?;
        assert!(version > RECORD_VERSION_0_7, "{text}");

        // This release reads a version 1 root record as it did, and refuses each kind's record
        // as the other's.
        let root_dir = AgentDir::new(root.path(), id("42"));
        fs::create_dir_all(root_dir.tls_dir())?;
        let v1 = record_with(
            &["admin:agent-42-*"],
            ScopesFrom::Defaults,
            Access::ReadWrite,
        );
        fs::write(root_dir.record_file(), toml::to_string(&v1)?)?;
        assert_eq!(root_dir.record()?, Some(v1.clone()));
        fs::write(
            root_dir.record_file(),
            text.replace("agent-42.c7", "agent-42"),
        )?;
        assert!(
            matches!(root_dir.record(), Err(AgentError::Mismatch { .. })),
            "{:?}",
            root_dir.record()
        );
        let as_child = AgentRecord {
            principal: "agent-42.c7".to_owned(),
            ..v1.clone()
        };
        fs::write(dir.record_file(), toml::to_string(&as_child)?)?;
        assert!(
            matches!(dir.record(), Err(AgentError::Mismatch { .. })),
            "{:?}",
            dir.record()
        );
        // A root record without its certificate's expiry, or a child's with one, is refused.
        let no_cert = AgentRecord {
            cert_expires_at: None,
            ..v1
        };
        fs::write(root_dir.record_file(), toml::to_string(&no_cert)?)?;
        assert!(matches!(
            root_dir.record(),
            Err(AgentError::Mismatch { .. })
        ));
        for field in ["root", "issuer_key_id", "operation"] {
            let lacking: String = text
                .lines()
                .filter(|line| !line.starts_with(&format!("{field} = ")))
                .map(|line| format!("{line}\n"))
                .collect();
            fs::write(dir.record_file(), lacking)?;
            let error = dir.record().expect_err(field);
            assert!(error.to_string().contains(field), "{field}: {error}");
        }
        fs::write(
            dir.record_file(),
            format!("{text}cert_expires_at = \"2027-01-01T00:00:00Z\"\n"),
        )?;
        assert!(matches!(dir.record(), Err(AgentError::Mismatch { .. })));
        Ok(())
    }
}
