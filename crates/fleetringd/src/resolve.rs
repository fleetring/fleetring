//! Resolving a client command's settings (`docs/cli.md`, section 3; ADR-0043, point 4.4).
//!
//! Each setting is resolved on its own, and the first of these wins:
//!
//! 1. the setting's own flag;
//! 2. the context the `--context` flag names;
//! 3. the setting's environment variable;
//! 4. the context `FLEETRING_CONTEXT` names, or else the current context;
//! 5. the built-in default.
//!
//! [`resolve`] is a pure function of the flags, an [`EnvSnapshot`] and the selected context, so
//! it is tested without touching the process environment.

use std::ffi::OsString;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use fleetring_client::context::{Context, ContextName, SelectedBy};
use fleetringd::tls::Principal;

/// The variable that names the control node's address.
pub(crate) const CONTROL_VAR: &str = "FLEETRING_CONTROL";
/// The variable that names a directory with `ca.crt`, `client.crt`, `client.key`, and the
/// `client.<principal>.*` files.
pub(crate) const TLS_DIR_VAR: &str = "FLEETRING_TLS_DIR";
/// The variable that names the authority certificate.
pub(crate) const CA_VAR: &str = "FLEETRING_CA";
/// The variable that names the client certificate.
pub(crate) const CERT_VAR: &str = "FLEETRING_CERT";
/// The variable that names the client certificate's key.
pub(crate) const KEY_VAR: &str = "FLEETRING_KEY";
/// The variable that names the principal to act as.
pub(crate) const AS_VAR: &str = "FLEETRING_AS";
/// The variable that holds a bearer token.
pub(crate) const TOKEN_VAR: &str = "FLEETRING_TOKEN";
/// The variable that names a file holding a bearer token.
pub(crate) const TOKEN_FILE_VAR: &str = "FLEETRING_TOKEN_FILE";
/// The variable that names the context to use.
pub(crate) const CONTEXT_VAR: &str = "FLEETRING_CONTEXT";
/// The variable that names an agent whose identity to act as (ADR-0043, point 8.3).
pub(crate) const AGENT_VAR: &str = "FLEETRING_AGENT";

/// The environment variables resolution reads, captured once.
#[derive(Debug, Clone, Default)]
pub(crate) struct EnvSnapshot {
    pub(crate) control: Option<String>,
    pub(crate) tls_dir: Option<PathBuf>,
    pub(crate) ca: Option<PathBuf>,
    pub(crate) cert: Option<PathBuf>,
    pub(crate) key: Option<PathBuf>,
    pub(crate) principal: Option<String>,
    pub(crate) token: Option<String>,
    pub(crate) token_file: Option<PathBuf>,
    pub(crate) context: Option<String>,
    /// Whether `FLEETRING_CONTEXT` is set to nothing, which means no contexts file is read.
    pub(crate) contexts: ContextsFile,
    /// `FLEETRING_AGENT`. Read here, not through clap, so that clap's conflicts apply to the
    /// flags alone; the command fills in the identity it names.
    pub(crate) agent: Option<String>,
}

/// Whether a command reads the contexts file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ContextsFile {
    #[default]
    Read,
    /// `FLEETRING_CONTEXT=` (set, and empty): the way past a file that can't be read.
    Skip,
}

/// The contexts file can't be used, and every client command reads it.
#[derive(Debug, thiserror::Error)]
#[error(
    "{} can't be used, and every client command reads it\n  hint: set {CONTEXT_VAR}= (empty) \
     to run without contexts, or move the file aside",
    path.display()
)]
pub(crate) struct ContextsUnusable {
    pub(crate) path: PathBuf,
    #[source]
    pub(crate) source: fleetring_client::context::ContextError,
}

impl EnvSnapshot {
    /// Reads the process environment. An empty variable counts as unset.
    pub(crate) fn capture() -> Self {
        Self::from_lookup(&|name| std::env::var_os(name))
    }

    pub(crate) fn from_lookup(lookup: &dyn Fn(&str) -> Option<OsString>) -> Self {
        let text = |name: &str| {
            lookup(name)
                .filter(|value| !value.is_empty())
                .and_then(|value| value.into_string().ok())
        };
        let path = |name: &str| {
            lookup(name)
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
        };
        Self {
            control: text(CONTROL_VAR),
            tls_dir: path(TLS_DIR_VAR),
            ca: path(CA_VAR),
            cert: path(CERT_VAR),
            key: path(KEY_VAR),
            principal: text(AS_VAR),
            token: text(TOKEN_VAR),
            token_file: path(TOKEN_FILE_VAR),
            context: text(CONTEXT_VAR),
            contexts: if lookup(CONTEXT_VAR).is_some_and(|value| value.is_empty()) {
                ContextsFile::Skip
            } else {
                ContextsFile::Read
            },
            agent: text(AGENT_VAR),
        }
    }
}

/// Where a resolved setting came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Source {
    Flag(&'static str),
    Variable(&'static str),
    Context(ContextName),
    Default,
}

/// Whether the command talks TLS or `--insecure-plaintext`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Wire {
    #[default]
    Tls,
    Plaintext,
}

/// A bearer token, or where to read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TokenSource {
    Value(String),
    File(PathBuf),
    Stdin,
}

impl TokenSource {
    /// The token's text.
    ///
    /// # Errors
    ///
    /// [`ResolveError::TokenFile`] if the file or stdin can't be read, or holds nothing.
    pub(crate) fn read(&self) -> Result<String, ResolveError> {
        let (origin, text) = match self {
            Self::Value(token) => return Ok(token.clone()),
            Self::File(path) => (path.display().to_string(), std::fs::read_to_string(path)),
            Self::Stdin => ("stdin".to_owned(), {
                let mut text = String::new();
                std::io::stdin()
                    .lock()
                    .read_to_string(&mut text)
                    .map(|_| text)
            }),
        };
        let text = text.map_err(|source| ResolveError::TokenFile {
            origin: origin.clone(),
            reason: source.to_string(),
        })?;
        let token = text.trim();
        if token.is_empty() {
            return Err(ResolveError::TokenFile {
                origin,
                reason: "it holds no token".to_owned(),
            });
        }
        Ok(token.to_owned())
    }
}

/// The three files a command's identity is loaded from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentityFiles {
    pub(crate) ca: PathBuf,
    pub(crate) cert: PathBuf,
    pub(crate) key: PathBuf,
}

/// How a command secures its connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SecurityFiles {
    Plaintext,
    Tls(IdentityFiles),
    /// The identity couldn't be found, and why. The error waits until a command needs the
    /// identity, so that a missing control address is reported first.
    Unresolved(String),
}

/// `Target`'s fields as given on the command line.
#[derive(Debug, Clone, Default)]
pub(crate) struct TargetFlags {
    /// `--control`.
    pub(crate) control: Option<String>,
    /// The control address read from the file `--config` names, which counts as `--control`.
    pub(crate) config: Option<String>,
    pub(crate) tls_dir: Option<PathBuf>,
    pub(crate) ca: Option<PathBuf>,
    pub(crate) cert: Option<PathBuf>,
    pub(crate) key: Option<PathBuf>,
    pub(crate) principal: Option<String>,
    /// `--token`, or `--token-file`.
    pub(crate) token: Option<TokenSource>,
    pub(crate) wire: Wire,
    /// The agent `--agent` named, whose identity the command filled into `tls_dir`, `principal`
    /// and `token`; only the warnings read it.
    pub(crate) agent: Option<String>,
}

/// A command's settings, resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Resolved {
    /// The control addresses, tried in order; empty if nothing names one.
    pub(crate) control: Vec<String>,
    pub(crate) control_from: Source,
    pub(crate) security: SecurityFiles,
    pub(crate) token: Option<TokenSource>,
    /// The selected context, if any.
    pub(crate) context: Option<ContextName>,
    /// The fleet the selected context names, when the context also supplied the certificate; the
    /// certificate must name the same fleet.
    pub(crate) fleet: Option<String>,
    /// One line per override, for stderr.
    pub(crate) warnings: Vec<String>,
}

/// Why settings couldn't be resolved.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum ResolveError {
    #[error(
        "no client identity: nothing names a TLS directory or --{flag}\n  hint: pass --tls-dir \
         <dir> or set {TLS_DIR_VAR} (a directory with ca.crt, client.crt, and client.key), give \
         the context a tls_dir, or pass --insecure-plaintext for a plaintext node"
    )]
    NoDirectory { flag: &'static str },
    #[error(
        "--as {principal} needs {TLS_DIR_VAR} to find client.{principal}.crt and \
         client.{principal}.key; set it, or pass --tls-dir, or --cert and --key"
    )]
    PrincipalNeedsDirectory { principal: String },
    #[error(
        "no client identity for principal '{principal}': looked for {} and {}\n  \
         hint: create it with `fleetring cert client {principal} --dir {}`",
        cert.display(), key.display(), dir.display()
    )]
    NoPrincipal {
        principal: String,
        cert: PathBuf,
        key: PathBuf,
        dir: PathBuf,
    },
    #[error("'{principal}' isn't a principal name: {reason}")]
    BadPrincipal { principal: String, reason: String },
    #[error("can't read the token from {origin}: {reason}")]
    TokenFile { origin: String, reason: String },
}

/// One step of the precedence: what a source names.
struct Layer {
    control: Option<Vec<String>>,
    control_from: Source,
    tls_dir: Option<PathBuf>,
    ca: Option<PathBuf>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    principal: Option<String>,
    token: Option<TokenSource>,
    /// The context this step is, if it is one.
    context: Option<ContextName>,
    /// The agent whose identity this step's directory, principal and token are.
    agent: Option<String>,
    kind: Kind,
}

/// What a step of the precedence is, for the warnings that name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Flags,
    Variables,
    Context,
}

impl Layer {
    fn flags(flags: &TargetFlags) -> Self {
        let (control, from) = match (&flags.control, &flags.config) {
            (Some(control), _) => (Some(control.clone()), Source::Flag("--control")),
            (None, Some(config)) => (Some(config.clone()), Source::Flag("--config")),
            (None, None) => (None, Source::Default),
        };
        Self {
            control: control.map(|control| vec![control]),
            control_from: from,
            tls_dir: flags.tls_dir.clone(),
            ca: flags.ca.clone(),
            cert: flags.cert.clone(),
            key: flags.key.clone(),
            principal: flags.principal.clone(),
            token: flags.token.clone(),
            context: None,
            agent: flags.agent.clone(),
            kind: Kind::Flags,
        }
    }

    fn variables(env: &EnvSnapshot) -> Self {
        let token = match (&env.token, &env.token_file) {
            (Some(token), _) => Some(TokenSource::Value(token.clone())),
            (None, Some(path)) if path == Path::new("-") => Some(TokenSource::Stdin),
            (None, Some(path)) => Some(TokenSource::File(path.clone())),
            (None, None) => None,
        };
        Self {
            control: env.control.clone().map(|control| vec![control]),
            control_from: Source::Variable(CONTROL_VAR),
            tls_dir: env.tls_dir.clone(),
            ca: env.ca.clone(),
            cert: env.cert.clone(),
            key: env.key.clone(),
            principal: env.principal.clone(),
            token,
            context: None,
            agent: env.agent.clone(),
            kind: Kind::Variables,
        }
    }

    fn context(name: &ContextName, context: &Context) -> Self {
        Self {
            control: (!context.control.is_empty()).then(|| context.control.clone()),
            control_from: Source::Context(name.clone()),
            tls_dir: context.tls_dir.clone(),
            ca: context.ca.clone(),
            cert: context.cert.clone(),
            key: context.key.clone(),
            principal: context.principal.clone(),
            token: context.token_file.clone().map(TokenSource::File),
            context: Some(name.clone()),
            agent: None,
            kind: Kind::Context,
        }
    }

    /// Whether this step names TLS material: a directory, an authority, a certificate or a key.
    /// A step with only a principal or a token supplies no identity of its own.
    fn has_material(&self) -> bool {
        self.tls_dir.is_some() || self.ca.is_some() || self.cert.is_some() || self.key.is_some()
    }

    /// Whether this step names an authority or a directory.
    fn names_trust(&self) -> bool {
        self.tls_dir.is_some() || self.ca.is_some()
    }

    /// Whether this step names nothing of an identity but where to find an authority.
    fn is_trust_only(&self) -> bool {
        self.names_trust()
            && self.cert.is_none()
            && self.key.is_none()
            && self.principal.is_none()
            && self.token.is_none()
    }

    /// Whether this step is the environment next to flags that name an identity, and adds only
    /// what they pair with: an authority and a token.
    fn is_supporting(&self, unit: &Self) -> bool {
        self.kind == Kind::Variables
            && unit.kind == Kind::Flags
            && self.cert.is_none()
            && self.key.is_none()
            && self.principal.is_none()
    }

    /// Whether this step names any part of an identity.
    fn has_identity(&self) -> bool {
        self.has_material() || self.principal.is_some() || self.token.is_some()
    }

    /// The parts of the identity this step sets, as the user wrote them.
    fn parts(&self) -> Vec<&'static str> {
        let names = match self.kind {
            Kind::Flags => [
                "--tls-dir",
                "--ca",
                "--cert",
                "--key",
                "--as",
                "--token",
                "--token-file",
            ],
            Kind::Variables => [
                TLS_DIR_VAR,
                CA_VAR,
                CERT_VAR,
                KEY_VAR,
                AS_VAR,
                TOKEN_VAR,
                TOKEN_FILE_VAR,
            ],
            Kind::Context => [
                "tls_dir",
                "ca",
                "cert",
                "key",
                "as",
                "token_file",
                "token_file",
            ],
        };
        let token = match &self.token {
            None => None,
            Some(TokenSource::Value(_)) => Some(names[5]),
            Some(TokenSource::File(_) | TokenSource::Stdin) => Some(names[6]),
        };
        // An agent's identity is one setting as the user wrote it, not the three it fills in.
        if self.agent.is_some() {
            let agent = if self.kind == Kind::Flags {
                "--agent"
            } else {
                AGENT_VAR
            };
            return std::iter::once(agent)
                .chain(self.ca.as_ref().map(|_| names[1]))
                .collect();
        }
        [
            self.tls_dir.as_ref().map(|_| names[0]),
            self.ca.as_ref().map(|_| names[1]),
            self.cert.as_ref().map(|_| names[2]),
            self.key.as_ref().map(|_| names[3]),
            self.principal.as_ref().map(|_| names[4]),
            token,
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// This step in words: where it is and what it sets.
    fn describe(&self) -> String {
        let parts = self.parts().join(", ");
        match (&self.context, self.kind) {
            (Some(name), _) => format!("context '{name}' ({parts})"),
            (None, Kind::Flags) => format!("the command line ({parts})"),
            (None, _) => format!("the environment ({parts})"),
        }
    }

    /// Whether every identity part this step sets is set the same way by `other`, so that
    /// `other` replaces nothing of it.
    fn is_covered_by(&self, other: &Self) -> bool {
        let same =
            |mine: &Option<PathBuf>, theirs: &Option<PathBuf>| mine.is_none() || mine == theirs;
        same(&self.tls_dir, &other.tls_dir)
            && same(&self.ca, &other.ca)
            && same(&self.cert, &other.cert)
            && same(&self.key, &other.key)
            && (self.principal.is_none() || self.principal == other.principal)
            && (self.token.is_none() || self.token == other.token)
    }
}

/// Which of the identity's files is being picked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    Ca,
    Cert,
    Key,
}

impl Part {
    fn flag(self) -> &'static str {
        match self {
            Self::Ca => "ca",
            Self::Cert => "cert",
            Self::Key => "key",
        }
    }

    fn default_name(self) -> &'static str {
        match self {
            Self::Ca => "ca.crt",
            Self::Cert => "client.crt",
            Self::Key => "client.key",
        }
    }

    fn explicit(self, layer: &Layer) -> Option<&PathBuf> {
        match self {
            Self::Ca => layer.ca.as_ref(),
            Self::Cert => layer.cert.as_ref(),
            Self::Key => layer.key.as_ref(),
        }
    }
}

/// The identity file `part`, from the layer `unit` alone (ADR-0043, point 4.4): a principal named
/// by a layer above it (`principal_above`) picks the principal's file under the unit's TLS
/// directory; otherwise the unit's explicit path; otherwise its own principal's file; otherwise
/// the directory's default; and for the authority only, with neither of those, the nearest step
/// `below` that names one.
fn pick(
    unit: &Layer,
    below: &[Layer],
    principal_above: Option<&str>,
    part: Part,
) -> Result<PathBuf, ResolveError> {
    let dir = unit.tls_dir.as_deref();
    let from_principal = |principal: &str| -> Result<PathBuf, ResolveError> {
        let (cert, key) = principal_files(principal, dir)?;
        Ok(if part == Part::Cert { cert } else { key })
    };
    if part != Part::Ca
        && let Some(principal) = principal_above
    {
        return from_principal(principal);
    }
    if let Some(path) = part.explicit(unit) {
        return Ok(path.clone());
    }
    if part != Part::Ca
        && let Some(principal) = &unit.principal
    {
        return from_principal(principal);
    }
    if let Some(dir) = dir {
        return Ok(dir.join(part.default_name()));
    }
    // An identity of explicit certificate and key names no authority: the trust anchor comes
    // from the nearest step below that names one. It cannot pair the certificate with another
    // fleet's authority, because a certificate must name the fleet its authority names.
    if part == Part::Ca {
        for lower in below {
            if let Some(ca) = &lower.ca {
                return Ok(ca.clone());
            }
            if let Some(dir) = &lower.tls_dir {
                return Ok(dir.join(part.default_name()));
            }
        }
    }
    Err(ResolveError::NoDirectory { flag: part.flag() })
}

/// The certificate and key `cert client <principal>` writes under `dir`.
fn principal_files(
    principal: &str,
    dir: Option<&Path>,
) -> Result<(PathBuf, PathBuf), ResolveError> {
    let name: Principal = principal
        .parse()
        .map_err(
            |error: fleetringd::tls::NameError| ResolveError::BadPrincipal {
                principal: principal.to_owned(),
                reason: error.to_string(),
            },
        )?;
    let Some(dir) = dir else {
        return Err(ResolveError::PrincipalNeedsDirectory {
            principal: name.to_string(),
        });
    };
    let cert = dir.join(format!("client.{name}.crt"));
    let key = dir.join(format!("client.{name}.key"));
    if cert.is_file() && key.is_file() {
        return Ok((cert, key));
    }
    Err(ResolveError::NoPrincipal {
        principal: name.to_string(),
        cert,
        key,
        dir: dir.to_owned(),
    })
}

/// Applies the precedence. `context` carries the selected context and how it was selected:
/// [`SelectedBy::Flag`] sits above the variables, the others below. `flags` holds only what the
/// command line gave.
///
/// The control address and the other plain settings resolve one by one. The identity resolves as
/// a unit (ADR-0043, point 4.4, as amended): the highest layer that names any TLS material (a
/// directory, an authority, a certificate or a key) supplies all of it, and the lower layers
/// supply none, neither files nor a token. A layer above that one that names only a principal
/// (`--as`) picks among the unit's files, and a token at or above the unit's layer is sent.
///
/// Nothing here fails: a control address that is missing leaves `control` empty, since a command
/// that names a data node by address needs none, and an identity that can't be found becomes
/// [`SecurityFiles::Unresolved`], with the error that says why.
pub(crate) fn resolve(
    flags: &TargetFlags,
    env: &EnvSnapshot,
    context: Option<(&ContextName, &Context, SelectedBy)>,
) -> Resolved {
    let mut layers = vec![Layer::flags(flags)];
    let mut below = None;
    if let Some((name, selected, by)) = context {
        match by {
            SelectedBy::Flag => layers.push(Layer::context(name, selected)),
            SelectedBy::Variable | SelectedBy::Current => {
                below = Some(Layer::context(name, selected));
            }
        }
    }
    layers.push(Layer::variables(env));
    layers.extend(below);

    let (control, control_from) = layers
        .iter()
        .find_map(|layer| {
            layer
                .control
                .as_ref()
                .map(|control| (control.clone(), layer.control_from.clone()))
        })
        .unwrap_or_else(|| (Vec::new(), Source::Default));

    // The layer that supplies the identity, and the ones above it.
    let unit = layers.iter().position(Layer::has_material);
    let kept = unit.map_or(layers.len(), |unit| unit + 1);
    let mut token = layers[..kept].iter().find_map(|layer| layer.token.clone());
    // Flags that name an identity are often used with a token from the environment, which is
    // theirs to pair; a context's own token is not, and stays with its context. Nor is an
    // agent's, which `FLEETRING_AGENT` names: it belongs with the agent's certificate, never
    // with one the flags name.
    if token.is_none()
        && let Some(unit) = unit
        && layers[unit].kind == Kind::Flags
    {
        token = layers[kept..]
            .iter()
            .find(|layer| layer.kind == Kind::Variables)
            .filter(|layer| layer.agent.is_none())
            .and_then(|layer| layer.token.clone());
    }

    let (security, fleet) = match flags.wire {
        Wire::Plaintext => (SecurityFiles::Plaintext, None),
        Wire::Tls => match identity(&layers, unit) {
            Ok(files) => {
                let fleet = unit
                    .and_then(|unit| layers[unit].context.as_ref())
                    .and(context)
                    .and_then(|(_, selected, _)| selected.fleet.clone());
                (SecurityFiles::Tls(files), fleet)
            }
            Err(error) => (SecurityFiles::Unresolved(error.to_string()), None),
        },
    };
    let mut warnings = displaced(&layers, unit);
    warnings.extend(overrides(flags, env, context, &layers, unit));
    Resolved {
        control,
        control_from,
        security,
        token,
        context: context.map(|(name, _, _)| name.clone()),
        fleet,
        warnings,
    }
}

/// The identity files the layer `unit` supplies.
fn identity(layers: &[Layer], unit: Option<usize>) -> Result<IdentityFiles, ResolveError> {
    let Some(unit) = unit else {
        // Nothing names a directory or a file.
        return Err(
            match layers.iter().find_map(|layer| layer.principal.clone()) {
                Some(principal) => ResolveError::PrincipalNeedsDirectory { principal },
                None => ResolveError::NoDirectory { flag: "cert" },
            },
        );
    };
    let (above, below) = (&layers[..unit], &layers[unit + 1..]);
    let unit = &layers[unit];
    let principal_above = above.iter().find_map(|layer| layer.principal.as_deref());
    // The certificate first: a missing principal file is the likeliest mistake, and the error
    // names the files it looked for.
    let cert = pick(unit, below, principal_above, Part::Cert)?;
    let key = pick(unit, below, principal_above, Part::Key)?;
    let ca = pick(unit, below, principal_above, Part::Ca)?;
    Ok(IdentityFiles { ca, cert, key })
}

/// One warning for each lower layer whose identity the unit's layer replaces (ADR-0043, point
/// 4.4): it names both. A lower layer that names the same things the unit does is not
/// mentioned, so a sourced dev `env` file next to the `dev` context says nothing.
fn displaced(layers: &[Layer], unit: Option<usize>) -> Vec<String> {
    let Some(unit) = unit else {
        return Vec::new();
    };
    // `--agent` names a whole identity on purpose, so the one it replaces is no surprise; the
    // variable may be left over in a shell, so it still warns.
    if layers[unit].kind == Kind::Flags && layers[unit].agent.is_some() {
        return Vec::new();
    }
    layers[unit + 1..]
        .iter()
        .filter(|lower| {
            lower.has_identity()
                && !lower.is_covered_by(&layers[unit])
                // A step that only names a trust anchor, for an identity that names none, is
                // used rather than replaced.
                && !(lower.is_trust_only() && !layers[unit].names_trust())
                && !lower.is_supporting(&layers[unit])
        })
        .map(|lower| {
            format!(
                "warning: the identity from {} replaces the identity from {}",
                layers[unit].describe(),
                lower.describe()
            )
        })
        .collect()
}

/// One variable that may override a field of the selected context.
struct Field {
    variable: &'static str,
    /// The context's field the variable stands for.
    name: &'static str,
    /// Whether the variable is set.
    set: bool,
    /// What the context's field holds, if it is set.
    held: Option<String>,
    /// Whether the variable's value differs from what the context holds.
    differs: Differs,
    /// Whether the command line names this field itself, which outranks both.
    flagged: Flagged,
    /// Whether the field is part of the identity, which resolves as a unit.
    part: Setting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Differs {
    Yes,
    No,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flagged {
    Yes,
    No,
}

/// Whether a [`Field`] is the control address or part of the identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Setting {
    Control,
    Identity,
}

impl Differs {
    fn between<T: PartialEq>(variable: &Option<T>, held: &Option<T>) -> Self {
        if variable == held {
            Self::No
        } else {
            Self::Yes
        }
    }
}

impl From<bool> for Flagged {
    fn from(flagged: bool) -> Self {
        if flagged { Self::Yes } else { Self::No }
    }
}

/// The warnings for variables that override the selected context, or that its `--context` flag
/// outranks (ADR-0043, point 4.5): the control address, the principal and the token. The rest of
/// the identity is covered by [`displaced`]. A variable that holds what the context holds
/// overrides nothing, and is not mentioned.
fn overrides(
    flags: &TargetFlags,
    env: &EnvSnapshot,
    context: Option<(&ContextName, &Context, SelectedBy)>,
    layers: &[Layer],
    unit: Option<usize>,
) -> Vec<String> {
    let Some((name, selected, by)) = context else {
        return Vec::new();
    };
    // When a layer above the context or the variables supplied the identity, theirs is
    // replaced, which `displaced` has said.
    let below_unit = |wanted: &dyn Fn(&Layer) -> bool| {
        matches!(
            (unit, layers.iter().position(wanted)),
            (Some(unit), Some(at)) if at > unit
        )
    };
    let context_displaced = below_unit(&|layer| layer.context.as_ref() == Some(name))
        || below_unit(&|layer| layer.kind == Kind::Variables);
    let shown = |path: &Option<PathBuf>| path.as_ref().map(|path| path.display().to_string());
    let control = (!selected.control.is_empty()).then(|| selected.control.clone());
    let token_flagged = Flagged::from(flags.token.is_some());
    let fields = [
        Field {
            variable: CONTROL_VAR,
            name: "control",
            set: env.control.is_some(),
            held: control.as_ref().map(|control| control.join(",")),
            differs: Differs::between(&env.control.clone().map(|control| vec![control]), &control),
            flagged: Flagged::from(flags.control.is_some() || flags.config.is_some()),
            part: Setting::Control,
        },
        Field {
            variable: AS_VAR,
            name: "as",
            set: env.principal.is_some(),
            held: selected.principal.clone(),
            differs: Differs::between(&env.principal, &selected.principal),
            flagged: Flagged::from(flags.principal.is_some()),
            part: Setting::Identity,
        },
        // A token is not a file, so it is never what the context's `token_file` holds.
        Field {
            variable: TOKEN_VAR,
            name: "token_file",
            set: env.token.is_some(),
            held: shown(&selected.token_file),
            differs: Differs::Yes,
            flagged: token_flagged,
            part: Setting::Identity,
        },
        Field {
            variable: TOKEN_FILE_VAR,
            name: "token_file",
            set: env.token_file.is_some(),
            held: shown(&selected.token_file),
            differs: Differs::between(&env.token_file, &selected.token_file),
            flagged: token_flagged,
            part: Setting::Identity,
        },
    ];
    fields
        .into_iter()
        .filter_map(|field| {
            let held = field.held?;
            if !field.set
                || field.flagged == Flagged::Yes
                || field.differs == Differs::No
                || (field.part == Setting::Identity && context_displaced)
            {
                return None;
            }
            Some(match by {
                SelectedBy::Flag => format!(
                    "warning: {} is ignored: --context {name} sets {}",
                    field.variable, field.name
                ),
                SelectedBy::Variable | SelectedBy::Current => format!(
                    "warning: {} overrides context '{name}' ({} {held})",
                    field.variable, field.name
                ),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn name(text: &str) -> ContextName {
        text.parse().expect("a context name")
    }

    fn path(text: &str) -> Option<PathBuf> {
        Some(PathBuf::from(text))
    }

    fn explicit_identity(prefix: &str) -> Context {
        Context {
            ca: path(&format!("{prefix}/ca.crt")),
            cert: path(&format!("{prefix}/c.crt")),
            key: path(&format!("{prefix}/c.key")),
            ..Context::default()
        }
    }

    fn with_control(addresses: &[&str]) -> Context {
        Context {
            control: addresses
                .iter()
                .map(|address| (*address).to_owned())
                .collect(),
            ..explicit_identity("/ctx")
        }
    }

    /// Flags that name a whole identity, so tests of the control address resolve.
    fn identified() -> TargetFlags {
        TargetFlags {
            ca: path("/f/ca.crt"),
            cert: path("/f/c.crt"),
            key: path("/f/c.key"),
            ..TargetFlags::default()
        }
    }

    fn run(
        flags: &TargetFlags,
        env: &EnvSnapshot,
        context: Option<(&ContextName, &Context, SelectedBy)>,
    ) -> Resolved {
        resolve(flags, env, context)
    }

    #[test]
    fn field_flag_beats_context_flag_beats_variable_beats_current_context() {
        let (flagged, current) = (name("flagged"), name("current"));
        let from_flag_context = with_control(&["flagctx:1"]);
        let from_current = with_control(&["current:1"]);
        let env = EnvSnapshot {
            control: Some("var:1".to_owned()),
            ..EnvSnapshot::default()
        };
        let flags = TargetFlags {
            control: Some("flag:1".to_owned()),
            ..identified()
        };

        // `--context` names the context: flag, context, variable.
        let by_flag = Some((&flagged, &from_flag_context, SelectedBy::Flag));
        let resolved = run(&flags, &env, by_flag);
        assert_eq!(
            (resolved.control, resolved.control_from),
            (vec!["flag:1".to_owned()], Source::Flag("--control"))
        );
        let resolved = run(&identified(), &env, by_flag);
        assert_eq!(
            (resolved.control, resolved.control_from),
            (
                vec!["flagctx:1".to_owned()],
                Source::Context(flagged.clone())
            )
        );
        // The variable beats a context that didn't come from the flag.
        let by_current = Some((&current, &from_current, SelectedBy::Current));
        let resolved = run(&identified(), &env, by_current);
        assert_eq!(
            (resolved.control, resolved.control_from),
            (vec!["var:1".to_owned()], Source::Variable(CONTROL_VAR))
        );
        // Without the variable, the current context.
        let resolved = run(&identified(), &EnvSnapshot::default(), by_current);
        assert_eq!(
            (resolved.control, resolved.control_from),
            (vec!["current:1".to_owned()], Source::Context(current))
        );
        // With nothing at all there is no control address, and that isn't an error here.
        let resolved = run(&identified(), &EnvSnapshot::default(), None);
        assert_eq!(
            (resolved.control, resolved.control_from),
            (Vec::new(), Source::Default)
        );
    }

    #[test]
    fn config_counts_as_the_field_flag() {
        let home = name("home");
        let context = with_control(&["ctx:1"]);
        let env = EnvSnapshot {
            control: Some("var:1".to_owned()),
            ..EnvSnapshot::default()
        };
        let flags = TargetFlags {
            config: Some("cfg:1".to_owned()),
            ..identified()
        };
        let resolved = run(&flags, &env, Some((&home, &context, SelectedBy::Flag)));
        assert_eq!(
            (resolved.control, resolved.control_from),
            (vec!["cfg:1".to_owned()], Source::Flag("--config"))
        );
    }

    #[test]
    fn a_variable_beats_the_fleetring_context_variables_context() {
        let chosen = name("chosen");
        let context = with_control(&["ctx:1", "ctx:2"]);
        let env = EnvSnapshot {
            control: Some("var:1".to_owned()),
            context: Some("chosen".to_owned()),
            ..EnvSnapshot::default()
        };
        let resolved = run(
            &identified(),
            &env,
            Some((&chosen, &context, SelectedBy::Variable)),
        );
        assert_eq!(resolved.control, ["var:1"]);
        let resolved = run(
            &identified(),
            &EnvSnapshot::default(),
            Some((&chosen, &context, SelectedBy::Variable)),
        );
        assert_eq!(resolved.control, ["ctx:1", "ctx:2"], "a list, in order");
    }

    #[test]
    fn the_selected_context_is_used_and_never_merged() {
        // The context names a control address and nothing else: no token, no principal.
        let home = name("home");
        let context = with_control(&["ctx:1"]);
        let resolved = run(
            &TargetFlags::default(),
            &EnvSnapshot::default(),
            Some((&home, &context, SelectedBy::Current)),
        );
        assert_eq!(resolved.token, None);
        assert_eq!(resolved.context, Some(home));
        assert_eq!(
            resolved.security,
            SecurityFiles::Tls(IdentityFiles {
                ca: PathBuf::from("/ctx/ca.crt"),
                cert: PathBuf::from("/ctx/c.crt"),
                key: PathBuf::from("/ctx/c.key"),
            })
        );
        // A context that names no identity doesn't borrow one from anywhere.
        let bare = Context {
            control: vec!["ctx:1".to_owned()],
            ..Context::default()
        };
        let error = unresolved(&resolve(
            &TargetFlags::default(),
            &EnvSnapshot::default(),
            Some((&name("bare"), &bare, SelectedBy::Current)),
        ));
        assert!(
            error.starts_with(
                "no client identity: nothing names a TLS directory or --cert\n  hint:"
            ),
            "{error}"
        );
    }

    /// A directory with `ca.crt` and the files of the principals named.
    fn tls_dir(principals: &[&str]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("a directory");
        for name in principals {
            for extension in ["crt", "key"] {
                fs::write(dir.path().join(format!("client.{name}.{extension}")), "")
                    .expect("a file");
            }
        }
        dir
    }

    /// Why the identity couldn't be found.
    fn unresolved(resolved: &Resolved) -> String {
        match &resolved.security {
            SecurityFiles::Unresolved(why) => why.clone(),
            other => panic!("the identity was found: {other:?}"),
        }
    }

    fn tls(resolved: &Resolved) -> &IdentityFiles {
        match &resolved.security {
            SecurityFiles::Tls(files) => files,
            other => panic!("not a TLS identity: {other:?}"),
        }
    }

    #[test]
    fn identity_explicit_cert_beats_principal_at_the_same_step() {
        let dir = tls_dir(&["app"]);
        let flags = TargetFlags {
            tls_dir: Some(dir.path().to_owned()),
            cert: path("/c.crt"),
            principal: Some("app".to_owned()),
            ..TargetFlags::default()
        };
        let resolved = run(&flags, &EnvSnapshot::default(), None);
        let files = tls(&resolved);
        // The certificate is the explicit one; the key, which nothing explicit names, is the
        // principal's; the authority is the directory's.
        assert_eq!(files.cert, Path::new("/c.crt"));
        assert_eq!(files.key, dir.path().join("client.app.key"));
        assert_eq!(files.ca, dir.path().join("ca.crt"));
    }

    #[test]
    fn identity_principal_flag_beats_context_cert() {
        let dir = tls_dir(&["app"]);
        let home = name("home");
        let context = Context {
            cert: path("/ctx/c.crt"),
            key: path("/ctx/c.key"),
            ..Context::default()
        };
        let flags = TargetFlags {
            tls_dir: Some(dir.path().to_owned()),
            principal: Some("app".to_owned()),
            ..TargetFlags::default()
        };
        let resolved = run(
            &flags,
            &EnvSnapshot::default(),
            Some((&home, &context, SelectedBy::Flag)),
        );
        let files = tls(&resolved);
        assert_eq!(files.cert, dir.path().join("client.app.crt"));
        assert_eq!(files.key, dir.path().join("client.app.key"));
    }

    #[test]
    fn identity_context_principal_beats_tls_dir_client() {
        let dir = tls_dir(&["ops"]);
        fs::write(dir.path().join("client.crt"), "").expect("a file");
        let home = name("home");
        let context = Context {
            tls_dir: Some(dir.path().to_owned()),
            principal: Some("ops".to_owned()),
            ..Context::default()
        };
        let resolved = run(
            &TargetFlags::default(),
            &EnvSnapshot::default(),
            Some((&home, &context, SelectedBy::Current)),
        );
        assert_eq!(tls(&resolved).cert, dir.path().join("client.ops.crt"));
        // Without a principal anywhere, the directory's own client.crt.
        let plain = Context {
            principal: None,
            ..context
        };
        let resolved = run(
            &TargetFlags::default(),
            &EnvSnapshot::default(),
            Some((&home, &plain, SelectedBy::Current)),
        );
        assert_eq!(tls(&resolved).cert, dir.path().join("client.crt"));
        assert_eq!(tls(&resolved).key, dir.path().join("client.key"));
    }

    #[test]
    fn identity_tls_dir_from_variable_beats_context() {
        let (variable_dir, context_dir) = (tls_dir(&["ops"]), tls_dir(&["ops"]));
        let home = name("home");
        let context = Context {
            tls_dir: Some(context_dir.path().to_owned()),
            principal: Some("ops".to_owned()),
            ..Context::default()
        };
        let env = EnvSnapshot {
            tls_dir: Some(variable_dir.path().to_owned()),
            ..EnvSnapshot::default()
        };
        let resolved = run(
            &TargetFlags::default(),
            &env,
            Some((&home, &context, SelectedBy::Current)),
        );
        assert_eq!(tls(&resolved).ca, variable_dir.path().join("ca.crt"));
        // The context's `as` belongs to the identity the variable replaced: the directory's
        // own client.crt is used, not the context's principal's.
        assert_eq!(tls(&resolved).cert, variable_dir.path().join("client.crt"));
        // `--context` outranks the variable.
        let resolved = run(
            &TargetFlags::default(),
            &env,
            Some((&home, &context, SelectedBy::Flag)),
        );
        assert_eq!(tls(&resolved).ca, context_dir.path().join("ca.crt"));
        // And the flag outranks both.
        let flags = TargetFlags {
            tls_dir: Some(variable_dir.path().to_owned()),
            ..TargetFlags::default()
        };
        let resolved = run(
            &flags,
            &EnvSnapshot::default(),
            Some((&home, &context, SelectedBy::Flag)),
        );
        assert_eq!(tls(&resolved).ca, variable_dir.path().join("ca.crt"));
    }

    #[test]
    fn the_fleet_is_checked_only_when_the_context_supplied_the_certificate() {
        let home = name("home");
        let context = Context {
            fleet: Some("home".to_owned()),
            ..explicit_identity("/ctx")
        };
        let selected = Some((&home, &context, SelectedBy::Current));
        let resolved = run(&TargetFlags::default(), &EnvSnapshot::default(), selected);
        assert_eq!(resolved.fleet.as_deref(), Some("home"));
        // A certificate from a flag or variable is a deliberate override: nothing to check.
        let flags = TargetFlags {
            cert: path("/other.crt"),
            ..TargetFlags::default()
        };
        let resolved = run(&flags, &EnvSnapshot::default(), selected);
        assert_eq!(resolved.fleet, None);
    }

    #[test]
    fn the_directory_and_principal_errors_name_the_files() {
        let dir = tls_dir(&[]);
        fs::write(dir.path().join("client.half.crt"), "").expect("a file");
        for principal in ["nobody", "half"] {
            let flags = TargetFlags {
                tls_dir: Some(dir.path().to_owned()),
                principal: Some(principal.to_owned()),
                ..TargetFlags::default()
            };
            let error = unresolved(&resolve(&flags, &EnvSnapshot::default(), None));
            let cert = dir.path().join(format!("client.{principal}.crt"));
            let key = dir.path().join(format!("client.{principal}.key"));
            assert!(
                error.starts_with(&format!(
                    "no client identity for principal '{principal}': looked for {} and {}",
                    cert.display(),
                    key.display()
                )),
                "{error}"
            );
            assert!(
                error.contains(&format!(
                    "hint: create it with `fleetring cert client {principal} --dir"
                )),
                "{error}"
            );
        }
        let flags = TargetFlags {
            principal: Some("app".to_owned()),
            ..TargetFlags::default()
        };
        let error = unresolved(&resolve(&flags, &EnvSnapshot::default(), None));
        assert!(
            error.starts_with("--as app needs FLEETRING_TLS_DIR to find client.app.crt"),
            "{error}"
        );
        let error = unresolved(&resolve(
            &TargetFlags::default(),
            &EnvSnapshot::default(),
            None,
        ));
        assert!(
            error.starts_with(
                "no client identity: nothing names a TLS directory or --cert\n  hint:"
            ),
            "{error}"
        );
    }

    #[test]
    fn plaintext_needs_no_identity() {
        let flags = TargetFlags {
            wire: Wire::Plaintext,
            control: Some("c:1".to_owned()),
            ..TargetFlags::default()
        };
        let resolved = run(&flags, &EnvSnapshot::default(), None);
        assert_eq!(resolved.security, SecurityFiles::Plaintext);
    }

    #[test]
    fn token_file_from_context_is_read() {
        let dir = tempfile::tempdir().expect("a directory");
        let file = dir.path().join("token");
        fs::write(&file, "  secret-token\n").expect("a file");
        let home = name("home");
        let context = Context {
            token_file: Some(file.clone()),
            ..explicit_identity("/ctx")
        };
        let resolved = run(
            &TargetFlags::default(),
            &EnvSnapshot::default(),
            Some((&home, &context, SelectedBy::Current)),
        );
        assert_eq!(resolved.token, Some(TokenSource::File(file)));
        let token = resolved.token.expect("a token").read().expect("it reads");
        assert_eq!(token, "secret-token");
    }

    #[test]
    fn token_flag_beats_context_token_file() {
        let home = name("home");
        let context = Context {
            token_file: path("/ctx/token"),
            ..explicit_identity("/ctx")
        };
        let flags = TargetFlags {
            token: Some(TokenSource::Value("flag-token".to_owned())),
            ..TargetFlags::default()
        };
        let env = EnvSnapshot {
            token: Some("var-token".to_owned()),
            ..EnvSnapshot::default()
        };
        let current = Some((&home, &context, SelectedBy::Current));
        assert_eq!(
            run(&flags, &env, current).token,
            Some(TokenSource::Value("flag-token".to_owned()))
        );
        // The variable beats the current context's file; `--context` beats the variable.
        assert_eq!(
            run(&TargetFlags::default(), &env, current).token,
            Some(TokenSource::Value("var-token".to_owned()))
        );
        assert_eq!(
            run(
                &TargetFlags::default(),
                &env,
                Some((&home, &context, SelectedBy::Flag))
            )
            .token,
            Some(TokenSource::File(PathBuf::from("/ctx/token")))
        );
        // `-` reads stdin.
        let env = EnvSnapshot {
            token_file: path("-"),
            ..EnvSnapshot::default()
        };
        let plaintext = TargetFlags {
            wire: Wire::Plaintext,
            ..TargetFlags::default()
        };
        assert_eq!(run(&plaintext, &env, None).token, Some(TokenSource::Stdin));
    }

    #[test]
    fn a_token_file_that_is_missing_or_empty_is_an_error() {
        let dir = tempfile::tempdir().expect("a directory");
        let empty = dir.path().join("empty");
        fs::write(&empty, "\n").expect("a file");
        for file in [empty, dir.path().join("missing")] {
            let error = TokenSource::File(file.clone())
                .read()
                .expect_err("no token");
            assert!(
                error.to_string().contains(&file.display().to_string()),
                "{error}"
            );
        }
    }

    fn overridden_by(env: &EnvSnapshot, by: SelectedBy) -> Vec<String> {
        let home = name("home");
        let context = Context {
            control: vec!["10.0.0.2:7400".to_owned()],
            tls_dir: path("/ctx/tls"),
            principal: Some("ops".to_owned()),
            token_file: path("/ctx/token"),
            ..Context::default()
        };
        let flags = TargetFlags {
            wire: Wire::Plaintext,
            ..TargetFlags::default()
        };
        run(&flags, env, Some((&home, &context, by))).warnings
    }

    #[test]
    fn an_overriding_variable_warns_once_with_the_context_named() {
        let env = EnvSnapshot {
            control: Some("127.0.0.1:7400".to_owned()),
            principal: Some("admin".to_owned()),
            tls_dir: path("/dev/tls"),
            // A variable for a field the context doesn't set overrides nothing.
            ca: path("/dev/ca.crt"),
            ..EnvSnapshot::default()
        };
        assert_eq!(
            overridden_by(&env, SelectedBy::Current),
            [
                "warning: the identity from the environment (FLEETRING_TLS_DIR, FLEETRING_CA, \
                 FLEETRING_AS) replaces the identity from context 'home' (tls_dir, as, \
                 token_file)",
                "warning: FLEETRING_CONTROL overrides context 'home' (control 10.0.0.2:7400)",
            ]
        );
        // `FLEETRING_CONTEXT`'s context is overridden the same way.
        assert_eq!(overridden_by(&env, SelectedBy::Variable).len(), 2);
        // A token overrides the context's token file.
        let env = EnvSnapshot {
            token: Some("t".to_owned()),
            ..EnvSnapshot::default()
        };
        assert_eq!(
            overridden_by(&env, SelectedBy::Current),
            ["warning: FLEETRING_TOKEN overrides context 'home' (token_file /ctx/token)"]
        );
        // A variable that holds what the context holds overrides nothing.
        let env = EnvSnapshot {
            control: Some("10.0.0.2:7400".to_owned()),
            ..EnvSnapshot::default()
        };
        assert_eq!(
            overridden_by(&env, SelectedBy::Current),
            Vec::<String>::new()
        );
        // No variables, no warnings.
        assert!(overridden_by(&EnvSnapshot::default(), SelectedBy::Current).is_empty());
    }

    #[test]
    fn a_variable_outranked_by_the_context_flag_warns_that_it_is_ignored() {
        let env = EnvSnapshot {
            control: Some("127.0.0.1:7400".to_owned()),
            ca: path("/dev/ca.crt"),
            ..EnvSnapshot::default()
        };
        assert_eq!(
            overridden_by(&env, SelectedBy::Flag),
            [
                "warning: the identity from context 'home' (tls_dir, as, token_file) replaces \
                 the identity from the environment (FLEETRING_CA)",
                "warning: FLEETRING_CONTROL is ignored: --context home sets control",
            ]
        );
    }

    #[test]
    fn a_variable_outranked_by_its_own_flag_is_not_an_override_of_the_context() {
        let home = name("home");
        let context = Context {
            control: vec!["10.0.0.2:7400".to_owned()],
            ..Context::default()
        };
        let env = EnvSnapshot {
            control: Some("127.0.0.1:7400".to_owned()),
            ..EnvSnapshot::default()
        };
        let flags = TargetFlags {
            control: Some("flag:1".to_owned()),
            ..identified()
        };
        for by in [SelectedBy::Flag, SelectedBy::Current] {
            let resolved = run(&flags, &env, Some((&home, &context, by)));
            assert!(
                resolved.warnings.is_empty(),
                "{by:?}: {:?}",
                resolved.warnings
            );
        }
    }

    /// A prod context with explicit files and a token, as a sourced dev `env` file meets it.
    fn prod() -> Context {
        Context {
            fleet: Some("prod".to_owned()),
            control: vec!["10.0.0.2:7400".to_owned()],
            ca: path("/prod/ca.crt"),
            cert: path("/prod/client.ops.crt"),
            key: path("/prod/client.ops.key"),
            token_file: path("/prod/token"),
            ..Context::default()
        }
    }

    #[test]
    fn a_dev_env_over_a_prod_context_sends_nothing_of_prod() {
        let dev = tls_dir(&["admin"]);
        let prod_name = name("prod");
        let env = EnvSnapshot {
            control: Some("127.0.0.1:7400".to_owned()),
            tls_dir: Some(dev.path().to_owned()),
            principal: Some("admin".to_owned()),
            ..EnvSnapshot::default()
        };
        let context = prod();
        let resolved = run(
            &TargetFlags::default(),
            &env,
            Some((&prod_name, &context, SelectedBy::Current)),
        );
        // The whole identity is the variables': none of prod's files, and not its token.
        let files = tls(&resolved);
        assert_eq!(files.ca, dev.path().join("ca.crt"));
        assert_eq!(files.cert, dev.path().join("client.admin.crt"));
        assert_eq!(files.key, dev.path().join("client.admin.key"));
        assert_eq!(resolved.token, None);
        // And it isn't prod's fleet to check.
        assert_eq!(resolved.fleet, None);
        assert_eq!(resolved.control, ["127.0.0.1:7400"]);
        // The warning names both.
        assert_eq!(
            resolved.warnings,
            [
                "warning: the identity from the environment (FLEETRING_TLS_DIR, FLEETRING_AS) \
                 replaces the identity from context 'prod' (ca, cert, key, token_file)",
                "warning: FLEETRING_CONTROL overrides context 'prod' (control 10.0.0.2:7400)",
            ]
        );
    }

    #[test]
    fn a_tls_dir_flag_over_a_context_with_explicit_files_wins_whole() {
        let dir = tls_dir(&[]);
        let prod_name = name("prod");
        let context = prod();
        let flags = TargetFlags {
            tls_dir: Some(dir.path().to_owned()),
            ..TargetFlags::default()
        };
        for by in [SelectedBy::Flag, SelectedBy::Current] {
            let resolved = run(
                &flags,
                &EnvSnapshot::default(),
                Some((&prod_name, &context, by)),
            );
            assert_eq!(
                tls(&resolved),
                &IdentityFiles {
                    ca: dir.path().join("ca.crt"),
                    cert: dir.path().join("client.crt"),
                    key: dir.path().join("client.key"),
                },
                "{by:?}"
            );
            assert_eq!(resolved.token, None, "{by:?}");
            assert_eq!(
                resolved.warnings.len(),
                1,
                "{by:?}: {:?}",
                resolved.warnings
            );
            assert!(
                resolved.warnings[0].starts_with(
                    "warning: the identity from the command line (--tls-dir) replaces the \
                     identity from context 'prod' ("
                ),
                "{by:?}: {:?}",
                resolved.warnings
            );
        }
    }

    #[test]
    fn a_token_is_sent_only_from_the_layer_of_the_identity_or_above() {
        let prod_name = name("prod");
        let context = prod();
        let current = Some((&prod_name, &context, SelectedBy::Current));
        // The context's identity and its token, with nothing above.
        let resolved = run(&TargetFlags::default(), &EnvSnapshot::default(), current);
        assert_eq!(
            resolved.token,
            Some(TokenSource::File(PathBuf::from("/prod/token")))
        );
        // A token above the identity is kept, and replaces the context's.
        let env = EnvSnapshot {
            token: Some("var-token".to_owned()),
            ..EnvSnapshot::default()
        };
        assert_eq!(
            run(&TargetFlags::default(), &env, current).token,
            Some(TokenSource::Value("var-token".to_owned()))
        );
        // The context's token is dropped with the identity that replaced the context's. A token
        // from the environment is kept for flags that name an identity: it is theirs to pair.
        let flags = TargetFlags {
            ca: path("/f/ca.crt"),
            cert: path("/f/c.crt"),
            key: path("/f/c.key"),
            ..TargetFlags::default()
        };
        assert_eq!(
            run(&flags, &env, current).token,
            Some(TokenSource::Value("var-token".to_owned()))
        );
        assert_eq!(run(&flags, &EnvSnapshot::default(), current).token, None);
        // But a context chosen by `--context` has an identity of its own, and an environment
        // token below it is not sent.
        let own = Context {
            token_file: None,
            ..prod()
        };
        assert_eq!(
            run(
                &TargetFlags::default(),
                &env,
                Some((&prod_name, &own, SelectedBy::Flag))
            )
            .token,
            None
        );
    }

    #[test]
    fn a_principal_above_the_identity_picks_among_its_files() {
        let dir = tls_dir(&["reader", "ops"]);
        let home = name("home");
        let context = Context {
            tls_dir: Some(dir.path().to_owned()),
            principal: Some("ops".to_owned()),
            ..Context::default()
        };
        let flags = TargetFlags {
            principal: Some("reader".to_owned()),
            ..TargetFlags::default()
        };
        let resolved = run(
            &flags,
            &EnvSnapshot::default(),
            Some((&home, &context, SelectedBy::Current)),
        );
        assert_eq!(tls(&resolved).cert, dir.path().join("client.reader.crt"));
        assert_eq!(tls(&resolved).ca, dir.path().join("ca.crt"));
        // `--as` takes nothing from the context but its directory: no warning.
        assert!(resolved.warnings.is_empty(), "{:?}", resolved.warnings);
    }

    #[test]
    fn a_lower_layer_that_names_the_same_things_is_not_mentioned() {
        // A sourced dev `env` file next to the `dev` context it matches.
        let dir = tls_dir(&["admin"]);
        let dev = name("dev");
        let context = Context {
            control: vec!["127.0.0.1:7400".to_owned()],
            tls_dir: Some(dir.path().to_owned()),
            principal: Some("admin".to_owned()),
            ..Context::default()
        };
        let env = EnvSnapshot {
            control: Some("127.0.0.1:7400".to_owned()),
            tls_dir: Some(dir.path().to_owned()),
            principal: Some("admin".to_owned()),
            ..EnvSnapshot::default()
        };
        let resolved = run(
            &TargetFlags::default(),
            &env,
            Some((&dev, &context, SelectedBy::Current)),
        );
        assert!(resolved.warnings.is_empty(), "{:?}", resolved.warnings);
    }

    #[test]
    fn an_explicit_certificate_and_key_take_their_authority_from_the_step_below() {
        let dir = tls_dir(&[]);
        let flags = TargetFlags {
            cert: path("/f/c.crt"),
            key: path("/f/c.key"),
            ..TargetFlags::default()
        };
        // The directory only says where the authority is: it is used, not replaced.
        let env = EnvSnapshot {
            tls_dir: Some(dir.path().to_owned()),
            ..EnvSnapshot::default()
        };
        let resolved = run(&flags, &env, None);
        assert_eq!(
            tls(&resolved),
            &IdentityFiles {
                ca: dir.path().join("ca.crt"),
                cert: PathBuf::from("/f/c.crt"),
                key: PathBuf::from("/f/c.key"),
            }
        );
        assert!(resolved.warnings.is_empty(), "{:?}", resolved.warnings);
        // A lower step's own certificate is replaced, not borrowed.
        let prod_name = name("prod");
        let context = prod();
        let resolved = run(
            &flags,
            &EnvSnapshot::default(),
            Some((&prod_name, &context, SelectedBy::Current)),
        );
        assert_eq!(tls(&resolved).ca, Path::new("/prod/ca.crt"));
        assert_eq!(tls(&resolved).cert, Path::new("/f/c.crt"));
        assert_eq!(resolved.token, None);
        assert_eq!(resolved.warnings.len(), 1, "{:?}", resolved.warnings);
    }

    #[test]
    fn an_agents_identity_is_named_as_the_agent_and_the_flag_draws_no_warning() {
        let agent = tls_dir(&["agent-42"]);
        let flags = TargetFlags {
            tls_dir: Some(agent.path().to_owned()),
            principal: Some("agent-42".to_owned()),
            token: Some(TokenSource::File(agent.path().join("token"))),
            agent: Some("42".to_owned()),
            ..TargetFlags::default()
        };
        let dev = tls_dir(&["admin"]);
        let env = EnvSnapshot {
            tls_dir: Some(dev.path().to_owned()),
            principal: Some("admin".to_owned()),
            ..EnvSnapshot::default()
        };
        let resolved = run(&flags, &env, None);
        assert_eq!(
            tls(&resolved).cert,
            agent.path().join("client.agent-42.crt")
        );
        assert!(resolved.warnings.is_empty(), "{:?}", resolved.warnings);
        // The variable may be left over in a shell, so replacing a context's identity warns.
        let env = EnvSnapshot {
            tls_dir: Some(agent.path().to_owned()),
            principal: Some("agent-42".to_owned()),
            token_file: Some(agent.path().join("token")),
            agent: Some("42".to_owned()),
            ..EnvSnapshot::default()
        };
        let context = Context {
            tls_dir: Some(dev.path().to_owned()),
            principal: Some("admin".to_owned()),
            ..Context::default()
        };
        let resolved = run(
            &TargetFlags::default(),
            &env,
            Some((&name("dev"), &context, SelectedBy::Current)),
        );
        assert_eq!(
            resolved.warnings,
            [format!(
                "warning: the identity from the environment ({AGENT_VAR}) replaces the identity \
                 from context 'dev' (tls_dir, as)"
            )]
        );
    }

    #[test]
    fn an_agents_token_from_the_environment_never_rides_a_certificate_the_flags_name() {
        let agent = tls_dir(&["agent-42"]);
        let other = tls_dir(&["ops"]);
        let flags = TargetFlags {
            tls_dir: Some(other.path().to_owned()),
            principal: Some("ops".to_owned()),
            ..TargetFlags::default()
        };
        let env = EnvSnapshot {
            tls_dir: Some(agent.path().to_owned()),
            principal: Some("agent-42".to_owned()),
            token_file: Some(agent.path().join("token")),
            agent: Some("42".to_owned()),
            ..EnvSnapshot::default()
        };
        let resolved = run(&flags, &env, None);
        assert_eq!(tls(&resolved).cert, other.path().join("client.ops.crt"));
        assert_eq!(resolved.token, None);
        // A plain token variable still pairs with the flags' identity.
        let env = EnvSnapshot {
            token_file: Some(agent.path().join("token")),
            ..EnvSnapshot::default()
        };
        let resolved = run(&flags, &env, None);
        assert_eq!(
            resolved.token,
            Some(TokenSource::File(agent.path().join("token")))
        );
    }

    #[test]
    fn the_snapshot_treats_an_empty_variable_as_unset() {
        let vars = std::collections::BTreeMap::from([
            (CONTROL_VAR, ""),
            (AS_VAR, "ops"),
            (TOKEN_FILE_VAR, "/t"),
        ]);
        let env = EnvSnapshot::from_lookup(&|name| vars.get(name).map(OsString::from));
        assert_eq!(env.control, None);
        assert_eq!(env.principal.as_deref(), Some("ops"));
        assert_eq!(env.token_file, path("/t"));
    }
}
