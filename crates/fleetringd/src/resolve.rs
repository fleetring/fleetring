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
        "pass --{flag} (or set {TLS_DIR_VAR} to a directory with ca.crt, client.crt, and \
         client.key, or give the context a tls_dir), or --insecure-plaintext"
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
        }
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

/// A picked file, and the context that supplied it, if one did.
type Picked<'a> = (PathBuf, Option<&'a ContextName>);

/// The identity file `part`: at the first step that names it, its explicit path, or, for the
/// certificate and key, the principal's file under the TLS directory; else the directory's
/// default.
fn pick<'a>(
    layers: &'a [Layer],
    part: Part,
    dir: Option<&'a (PathBuf, Option<&'a ContextName>)>,
) -> Result<Picked<'a>, ResolveError> {
    for layer in layers {
        if let Some(path) = part.explicit(layer) {
            return Ok((path.clone(), layer.context.as_ref()));
        }
        if part != Part::Ca
            && let Some(principal) = &layer.principal
        {
            let (cert, key) = principal_files(principal, dir.map(|(dir, _)| dir.as_path()))?;
            let path = if part == Part::Cert { cert } else { key };
            return Ok((path, layer.context.as_ref()));
        }
    }
    match dir {
        Some((dir, origin)) => Ok((dir.join(part.default_name()), *origin)),
        None => Err(ResolveError::NoDirectory { flag: part.flag() }),
    }
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
                below = Some(Layer::context(name, selected))
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
    let token = layers.iter().find_map(|layer| layer.token.clone());

    let (security, fleet) = match identity(flags.wire, &layers, context) {
        Ok(found) => found,
        Err(error) => (SecurityFiles::Unresolved(error.to_string()), None),
    };
    Resolved {
        control,
        control_from,
        security,
        token,
        context: context.map(|(name, _, _)| name.clone()),
        fleet,
        warnings: warnings(flags, env, context),
    }
}

/// The identity files, and the fleet the selected context names if it supplied the certificate.
fn identity(
    wire: Wire,
    layers: &[Layer],
    context: Option<(&ContextName, &Context, SelectedBy)>,
) -> Result<(SecurityFiles, Option<String>), ResolveError> {
    Ok(match wire {
        Wire::Plaintext => (SecurityFiles::Plaintext, None),
        Wire::Tls => {
            let dir = layers.iter().find_map(|layer| {
                layer
                    .tls_dir
                    .as_ref()
                    .map(|dir| (dir.clone(), layer.context.as_ref()))
            });
            // The principal's files first: a missing one is the likeliest mistake, and the
            // error names the files it looked for.
            let (cert, cert_from) = pick(layers, Part::Cert, dir.as_ref())?;
            let (key, _) = pick(layers, Part::Key, dir.as_ref())?;
            let (ca, _) = pick(layers, Part::Ca, dir.as_ref())?;
            let fleet = cert_from
                .and(context)
                .and_then(|(_, selected, _)| selected.fleet.clone());
            (SecurityFiles::Tls(IdentityFiles { ca, cert, key }), fleet)
        }
    })
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
/// outranks (ADR-0043, point 4.5). A variable that holds what the context holds overrides
/// nothing, and is not mentioned.
fn warnings(
    flags: &TargetFlags,
    env: &EnvSnapshot,
    context: Option<(&ContextName, &Context, SelectedBy)>,
) -> Vec<String> {
    let Some((name, selected, by)) = context else {
        return Vec::new();
    };
    let shown = |path: &Option<PathBuf>| path.as_ref().map(|path| path.display().to_string());
    let control = (!selected.control.is_empty()).then(|| selected.control.clone());
    let control_flagged = Flagged::from(flags.control.is_some() || flags.config.is_some());
    let token_flagged = Flagged::from(flags.token.is_some());
    let fields = [
        Field {
            variable: CONTROL_VAR,
            name: "control",
            set: env.control.is_some(),
            held: control.as_ref().map(|control| control.join(",")),
            differs: Differs::between(&env.control.clone().map(|control| vec![control]), &control),
            flagged: control_flagged,
        },
        Field {
            variable: TLS_DIR_VAR,
            name: "tls_dir",
            set: env.tls_dir.is_some(),
            held: shown(&selected.tls_dir),
            differs: Differs::between(&env.tls_dir, &selected.tls_dir),
            flagged: Flagged::from(flags.tls_dir.is_some()),
        },
        Field {
            variable: CA_VAR,
            name: "ca",
            set: env.ca.is_some(),
            held: shown(&selected.ca),
            differs: Differs::between(&env.ca, &selected.ca),
            flagged: Flagged::from(flags.ca.is_some()),
        },
        Field {
            variable: CERT_VAR,
            name: "cert",
            set: env.cert.is_some(),
            held: shown(&selected.cert),
            differs: Differs::between(&env.cert, &selected.cert),
            flagged: Flagged::from(flags.cert.is_some()),
        },
        Field {
            variable: KEY_VAR,
            name: "key",
            set: env.key.is_some(),
            held: shown(&selected.key),
            differs: Differs::between(&env.key, &selected.key),
            flagged: Flagged::from(flags.key.is_some()),
        },
        Field {
            variable: AS_VAR,
            name: "as",
            set: env.principal.is_some(),
            held: selected.principal.clone(),
            differs: Differs::between(&env.principal, &selected.principal),
            flagged: Flagged::from(flags.principal.is_some()),
        },
        // A token is not a file, so it is never what the context's `token_file` holds.
        Field {
            variable: TOKEN_VAR,
            name: "token_file",
            set: env.token.is_some(),
            held: shown(&selected.token_file),
            differs: Differs::Yes,
            flagged: token_flagged,
        },
        Field {
            variable: TOKEN_FILE_VAR,
            name: "token_file",
            set: env.token_file.is_some(),
            held: shown(&selected.token_file),
            differs: Differs::between(&env.token_file, &selected.token_file),
            flagged: token_flagged,
        },
    ];
    fields
        .into_iter()
        .filter_map(|field| {
            let held = field.held?;
            if !field.set || field.flagged == Flagged::Yes || field.differs == Differs::No {
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
            error.starts_with("pass --cert (or set FLEETRING_TLS_DIR"),
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
        assert_eq!(
            tls(&resolved).cert,
            variable_dir.path().join("client.ops.crt")
        );
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
            error.starts_with("pass --cert (or set FLEETRING_TLS_DIR"),
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
                "warning: FLEETRING_CONTROL overrides context 'home' (control 10.0.0.2:7400)",
                "warning: FLEETRING_TLS_DIR overrides context 'home' (tls_dir /ctx/tls)",
                "warning: FLEETRING_AS overrides context 'home' (as ops)",
            ]
        );
        // `FLEETRING_CONTEXT`'s context is overridden the same way.
        assert_eq!(overridden_by(&env, SelectedBy::Variable).len(), 3);
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
            ["warning: FLEETRING_CONTROL is ignored: --context home sets control"]
        );
    }

    #[test]
    fn a_variable_outranked_by_its_own_flag_is_not_an_override_of_the_context() {
        let home = name("home");
        let context = with_control(&["10.0.0.2:7400"]);
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
