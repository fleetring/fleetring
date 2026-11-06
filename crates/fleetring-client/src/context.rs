//! Contexts (ADR-0043, point 4): names for everything a client command needs to reach one fleet
//! as one principal.
//!
//! A context holds names and paths, never a secret: a token is referenced by file. The contexts
//! live in one TOML file, [`FILE`], which [`ContextStore`] reads and replaces under a lock, so
//! concurrent writers (a `fleetring dev` start and a `fleetring switch`) never lose each other's
//! change.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The contexts file's name, in the configuration directory.
pub const FILE: &str = "contexts.toml";

/// The format version this release reads and writes.
pub const FORMAT_VERSION: u32 = 1;

/// What the file starts with.
const HEADER: &str = "# Written by fleetring. Edit it with `fleetring context`.\n";

/// The longest context name.
const MAX_NAME: usize = 63;

/// The name dev mode gives its context (ADR-0043, point 5.1).
pub const DEV_NAME: &str = "dev";

/// A context's name: 1 to 63 of `[a-z0-9._-]`, starting with `[a-z0-9]`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ContextName(String);

impl ContextName {
    /// The name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ContextName {
    type Err = ContextError;

    fn from_str(text: &str) -> Result<Self, ContextError> {
        let mut bytes = text.bytes();
        let starts_well = bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit());
        let rest_is_well = bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        });
        if starts_well && rest_is_well && text.len() <= MAX_NAME {
            Ok(Self(text.to_owned()))
        } else {
            Err(ContextError::InvalidName {
                name: text.to_owned(),
            })
        }
    }
}

impl TryFrom<String> for ContextName {
    type Error = ContextError;

    fn try_from(text: String) -> Result<Self, ContextError> {
        text.parse()
    }
}

impl From<ContextName> for String {
    fn from(name: ContextName) -> Self {
        name.0
    }
}

impl fmt::Display for ContextName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Everything a command needs to reach one fleet as one principal.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Context {
    /// The fleet's name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fleet: Option<String>,
    /// The control nodes' addresses, `host:port`, tried in order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub control: Vec<String>,
    /// A directory with `ca.crt` and the client certificates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_dir: Option<PathBuf>,
    /// The trusted authority certificate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca: Option<PathBuf>,
    /// The client certificate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert: Option<PathBuf>,
    /// The client certificate's private key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<PathBuf>,
    /// The principal to act as, which picks `client.<as>.crt` under `tls_dir`.
    #[serde(default, rename = "as", skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    /// A file holding a bearer token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_file: Option<PathBuf>,
    /// Set only by dev mode: the dev directory this context points at (ADR-0043, point 5.1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dev_dir: Option<PathBuf>,
    /// Where `--agent` identities live for this context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents_dir: Option<PathBuf>,
}

/// The contexts file: which context is current, which was before it, and every context.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contexts {
    /// The format version; [`FORMAT_VERSION`].
    pub version: u32,
    /// The context commands use unless something more specific is given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<ContextName>,
    /// The context `switch -` goes back to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<ContextName>,
    /// Every context, by name.
    #[serde(default)]
    pub contexts: BTreeMap<ContextName, Context>,
    /// The file these were read from, for error messages. Empty for contexts built in memory.
    #[serde(skip)]
    path: PathBuf,
}

impl PartialEq for Contexts {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
            && self.current == other.current
            && self.previous == other.previous
            && self.contexts == other.contexts
    }
}

impl Eq for Contexts {}

/// Whether the context a command removed was the current one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasCurrent {
    /// It was current, so now no context is.
    Yes,
    /// It was not.
    No,
}

/// What [`Contexts::upsert_dev`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DevContextOutcome {
    /// The `dev` context is current.
    Current,
    /// The `dev` context was written, and another context is current.
    NotCurrent {
        /// The context that is current.
        current: ContextName,
    },
    /// A `dev` context without `dev_dir` exists; it was left alone.
    Kept,
}

impl Contexts {
    /// No contexts, at the current format version.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            version: FORMAT_VERSION,
            current: None,
            previous: None,
            contexts: BTreeMap::new(),
            path: PathBuf::new(),
        }
    }

    /// The current context, if `current` names one that exists.
    #[must_use]
    pub fn current(&self) -> Option<(&ContextName, &Context)> {
        self.contexts.get_key_value(self.current.as_ref()?)
    }

    fn unknown(&self, name: &ContextName) -> ContextError {
        ContextError::Unknown {
            name: name.to_string(),
            path: self.path.clone(),
        }
    }

    /// Makes `name` current, recording the old current one as `previous` if it differs.
    ///
    /// # Errors
    ///
    /// [`ContextError::Unknown`] if there is no such context.
    pub fn use_context(&mut self, name: &ContextName) -> Result<(), ContextError> {
        if !self.contexts.contains_key(name) {
            return Err(self.unknown(name));
        }
        if self.current.as_ref() != Some(name) {
            if let Some(old) = self.current.take() {
                self.previous = Some(old);
            }
            self.current = Some(name.clone());
        }
        Ok(())
    }

    /// Goes back to `previous`, swapping the two, and returns the context now current.
    ///
    /// # Errors
    ///
    /// [`ContextError::NoPrevious`] if there is no previous context, or it no longer exists.
    pub fn use_previous(&mut self) -> Result<ContextName, ContextError> {
        let previous = self
            .previous
            .clone()
            .filter(|name| self.contexts.contains_key(name))
            .ok_or(ContextError::NoPrevious)?;
        self.previous = self.current.replace(previous.clone());
        Ok(previous)
    }

    /// Adds `context` as `name`; the first context added becomes current.
    ///
    /// # Errors
    ///
    /// [`ContextError::Exists`] if `name` is taken.
    pub fn add(&mut self, name: ContextName, context: Context) -> Result<(), ContextError> {
        if self.contexts.contains_key(&name) {
            return Err(ContextError::Exists { name });
        }
        if self.current().is_none() {
            self.current = Some(name.clone());
        }
        self.contexts.insert(name, context);
        Ok(())
    }

    /// Removes `name`; clears `current` and `previous` if they named it.
    ///
    /// # Errors
    ///
    /// [`ContextError::Unknown`] if there is no such context.
    pub fn remove(&mut self, name: &ContextName) -> Result<WasCurrent, ContextError> {
        if self.contexts.remove(name).is_none() {
            return Err(self.unknown(name));
        }
        if self.previous.as_ref() == Some(name) {
            self.previous = None;
        }
        if self.current.as_ref() == Some(name) {
            self.current = None;
            Ok(WasCurrent::Yes)
        } else {
            Ok(WasCurrent::No)
        }
    }

    /// Writes dev mode's context (ADR-0043, point 5.1) and makes it current if none is.
    ///
    /// It overwrites a context named `dev` only if dev mode wrote it, which `dev_dir` marks; the
    /// user's own `dev` context is left alone.
    pub fn upsert_dev(&mut self, context: Context) -> DevContextOutcome {
        let name = ContextName(DEV_NAME.to_owned());
        if self
            .contexts
            .get(&name)
            .is_some_and(|existing| existing.dev_dir.is_none())
        {
            return DevContextOutcome::Kept;
        }
        self.contexts.insert(name.clone(), context);
        match self.current() {
            None => {
                self.current = Some(name);
                DevContextOutcome::Current
            }
            Some((current, _)) if *current == name => DevContextOutcome::Current,
            Some((current, _)) => DevContextOutcome::NotCurrent {
                current: current.clone(),
            },
        }
    }
}

/// How [`select`] picked a context.
///
/// A [`Flag`](SelectedBy::Flag) sits above the field variables in the precedence; the other two
/// below them (`docs/cli.md`, section 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedBy {
    /// The `--context` flag.
    Flag,
    /// The `FLEETRING_CONTEXT` variable.
    Variable,
    /// The file's `current`.
    Current,
}

/// A context [`select`] picked, and how.
pub type Selected<'a> = (&'a ContextName, &'a Context, SelectedBy);

/// Picks the context: `flag`, else `variable` (`FLEETRING_CONTEXT`), else the current one.
///
/// # Errors
///
/// [`ContextError::Unknown`], naming the context and the file, if the flag or the variable names
/// a context that doesn't exist; [`ContextError::InvalidName`] if the variable isn't a name.
pub fn select<'a>(
    contexts: &'a Contexts,
    flag: Option<&ContextName>,
    variable: Option<&str>,
) -> Result<Option<Selected<'a>>, ContextError> {
    let named = |name: &ContextName, by: SelectedBy| {
        contexts
            .contexts
            .get_key_value(name)
            .map(|(name, context)| (name, context, by))
            .ok_or_else(|| contexts.unknown(name))
    };
    if let Some(name) = flag {
        return named(name, SelectedBy::Flag).map(Some);
    }
    if let Some(text) = variable.filter(|text| !text.is_empty()) {
        return named(&text.parse()?, SelectedBy::Variable).map(Some);
    }
    Ok(contexts
        .current()
        .map(|(name, context)| (name, context, SelectedBy::Current)))
}

/// The contexts file under a directory, locked for each read-modify-write.
#[derive(Debug, Clone)]
pub struct ContextStore {
    path: PathBuf,
}

impl ContextStore {
    /// The store for the file [`FILE`] in `dir`, the configuration directory.
    #[must_use]
    pub fn at(dir: &Path) -> Self {
        Self {
            path: dir.join(FILE),
        }
    }

    /// The file's path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn lock_path(&self) -> PathBuf {
        let mut name = self.path.clone().into_os_string();
        name.push(".lock");
        PathBuf::from(name)
    }

    fn io(&self, source: io::Error) -> ContextError {
        ContextError::Io {
            path: self.path.clone(),
            source,
        }
    }

    /// Reads the file. A missing file is [`Contexts::empty`].
    ///
    /// # Errors
    ///
    /// [`ContextError::Io`] if it can't be read, [`ContextError::Parse`] if it isn't the format,
    /// and [`ContextError::Version`] if its `version` isn't [`FORMAT_VERSION`].
    pub fn load(&self) -> Result<Contexts, ContextError> {
        let text = match fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(Contexts {
                    path: self.path.clone(),
                    ..Contexts::empty()
                });
            }
            Err(error) => return Err(self.io(error)),
        };
        let parse = |source| ContextError::Parse {
            path: self.path.clone(),
            source,
        };
        // The version is checked first, so a file from a later release says so rather than
        // failing on a key this release doesn't know.
        #[derive(Deserialize)]
        struct Header {
            version: Option<u32>,
        }
        let header: Header = toml::from_str(&text).map_err(parse)?;
        if let Some(found) = header.version.filter(|found| *found != FORMAT_VERSION) {
            return Err(ContextError::Version {
                path: self.path.clone(),
                found,
            });
        }
        let mut contexts: Contexts = toml::from_str(&text).map_err(parse)?;
        contexts.path.clone_from(&self.path);
        Ok(contexts)
    }

    /// Locks `contexts.toml.lock` exclusively, loads the file, applies `change`, and writes the
    /// result to a temporary file beside the store (`0600`), synced, then renames it over the
    /// store. Creates the directory `0700`. Writes nothing if `change` fails. Returns `change`'s
    /// result.
    ///
    /// # Errors
    ///
    /// `change`'s error, or [`ContextError::Io`], [`ContextError::Parse`] or
    /// [`ContextError::Version`] from loading or writing.
    pub fn update<T>(
        &self,
        change: impl FnOnce(&mut Contexts) -> Result<T, ContextError>,
    ) -> Result<T, ContextError> {
        let dir = self.path.parent().unwrap_or_else(|| Path::new("."));
        create_private_dir(dir).map_err(|error| self.io(error))?;
        let lock = open_private(&self.lock_path(), Truncate::No).map_err(|error| self.io(error))?;
        lock.lock().map_err(|error| self.io(error))?;
        // The lock is released when `lock` is dropped, on every path out.
        let mut contexts = self.load()?;
        let result = change(&mut contexts)?;
        self.write(&contexts).map_err(|error| self.io(error))?;
        Ok(result)
    }

    fn write(&self, contexts: &Contexts) -> io::Result<()> {
        let text = toml::to_string(contexts).map_err(io::Error::other)?;
        let mut temporary = self.path.clone().into_os_string();
        temporary.push(".tmp");
        let temporary = PathBuf::from(temporary);
        let mut file = open_private(&temporary, Truncate::Yes)?;
        file.write_all(HEADER.as_bytes())?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, &self.path)
    }
}

/// Whether [`open_private`] empties a file that exists.
#[derive(Debug, Clone, Copy)]
enum Truncate {
    Yes,
    No,
}

/// Opens `path` for writing, creating it `0600` on Unix.
fn open_private(path: &Path, truncate: Truncate) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create(true)
        .truncate(matches!(truncate, Truncate::Yes));
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        // A file an earlier version or a user created may be looser than the mode above.
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

/// Creates `dir` and its missing parents, `0700` on Unix.
fn create_private_dir(dir: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder.create(dir)
}

/// Why a context couldn't be read, changed or selected.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ContextError {
    /// No context has this name.
    #[error("context '{name}' doesn't exist in {path}")]
    Unknown {
        /// The name asked for.
        name: String,
        /// The contexts file.
        path: PathBuf,
    },
    /// A context has this name already.
    #[error("context '{name}' already exists")]
    Exists {
        /// The name.
        name: ContextName,
    },
    /// The text isn't a context name.
    #[error(
        "'{name}' isn't a context name: use 1 to 63 lowercase letters, digits, '-', '_' or '.'"
    )]
    InvalidName {
        /// The text.
        name: String,
    },
    /// `switch -` with nothing to go back to.
    #[error("no previous context to go back to")]
    NoPrevious,
    /// The file is from another format version.
    #[error("{path} has format version {found}; this release reads version {FORMAT_VERSION}")]
    Version {
        /// The contexts file.
        path: PathBuf,
        /// The version it names.
        found: u32,
    },
    /// The file isn't valid.
    #[error("can't parse {path}")]
    Parse {
        /// The contexts file.
        path: PathBuf,
        /// What the parser said.
        #[source]
        source: toml::de::Error,
    },
    /// The file, its lock or its directory can't be read or written.
    #[error("can't read or write {path}")]
    Io {
        /// The contexts file.
        path: PathBuf,
        /// The failure.
        #[source]
        source: io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(text: &str) -> ContextName {
        text.parse().expect("a context name")
    }

    fn with_control(address: &str) -> Context {
        Context {
            control: vec![address.to_owned()],
            ..Context::default()
        }
    }

    fn two() -> Contexts {
        let mut contexts = Contexts::empty();
        contexts.add(name("a"), with_control("a:1")).expect("add a");
        contexts.add(name("b"), with_control("b:1")).expect("add b");
        contexts
    }

    #[test]
    fn names_are_one_to_63_lowercase_characters_starting_with_a_letter_or_digit() {
        for good in ["a", "0", "dev", "home.lab", "a-b_c.d", &"x".repeat(63)] {
            assert!(good.parse::<ContextName>().is_ok(), "{good}");
        }
        for bad in [
            "",
            "-",
            "-a",
            "_a",
            ".a",
            "A",
            "aB",
            "a b",
            "a/b",
            "é",
            &"x".repeat(64),
        ] {
            let error = bad.parse::<ContextName>().expect_err(bad);
            assert!(
                matches!(&error, ContextError::InvalidName { name } if name == bad),
                "{bad}: {error}"
            );
        }
    }

    /// ADR-0043, point 4.3.
    const EXAMPLE: &str = r#"
# Written by fleetring. Edit it with `fleetring context`.
version = 1
current = "dev"
previous = "home"

[contexts.dev]
fleet = "dev"
control = ["127.0.0.1:7400"]
tls_dir = "/home/me/.fleetring/tls"
as = "admin"
dev_dir = "/home/me/.fleetring"          # written by dev mode; see point 5

[contexts.home]
fleet = "home"
control = ["10.0.0.2:7400", "10.0.0.3:7400"]
ca = "/home/me/.config/fleetring/tls/home/ca.crt"
cert = "/home/me/.config/fleetring/tls/home/client.ops.crt"
key = "/home/me/.config/fleetring/tls/home/client.ops.key"
token_file = "/home/me/.config/fleetring/tokens/home"
"#;

    #[test]
    fn the_adrs_example_file_round_trips() {
        let parsed: Contexts = toml::from_str(EXAMPLE).expect("the example parses");
        assert_eq!(parsed.current, Some(name("dev")));
        assert_eq!(parsed.previous, Some(name("home")));
        let home = &parsed.contexts[&name("home")];
        assert_eq!(home.control, ["10.0.0.2:7400", "10.0.0.3:7400"]);
        assert_eq!(
            home.token_file.as_deref(),
            Some(Path::new("/home/me/.config/fleetring/tokens/home"))
        );
        let dev = &parsed.contexts[&name("dev")];
        assert_eq!(dev.principal.as_deref(), Some("admin"));
        assert_eq!(
            dev.dev_dir.as_deref(),
            Some(Path::new("/home/me/.fleetring"))
        );

        let written = toml::to_string(&parsed).expect("it serializes");
        let again: Contexts = toml::from_str(&written).expect("it parses again");
        assert_eq!(again, parsed, "{written}");
        // Absent fields are left out, not written as empty values.
        assert!(!written.contains("agents_dir"), "{written}");
    }

    #[test]
    fn an_unknown_key_is_refused() {
        for text in [
            "version = 1\nsurprise = 1\n",
            "version = 1\n[contexts.a]\ncontrol = [\"a:1\"]\ncontrol_node = \"x\"\n",
        ] {
            let error = toml::from_str::<Contexts>(text).expect_err("an unknown key");
            assert!(error.to_string().contains("unknown field"), "{error}");
        }
    }

    #[test]
    fn use_context_and_use_previous_record_and_swap() {
        let mut contexts = two();
        assert_eq!(contexts.current, Some(name("a")));
        contexts.use_context(&name("b")).expect("use b");
        assert_eq!(
            (&contexts.current, &contexts.previous),
            (&Some(name("b")), &Some(name("a")))
        );
        // Using the current context again changes nothing.
        contexts.use_context(&name("b")).expect("use b again");
        assert_eq!(contexts.previous, Some(name("a")));
        assert_eq!(contexts.use_previous().expect("back"), name("a"));
        assert_eq!(
            (&contexts.current, &contexts.previous),
            (&Some(name("a")), &Some(name("b")))
        );
        assert_eq!(contexts.use_previous().expect("forth"), name("b"));

        let missing = contexts.use_context(&name("nope")).expect_err("unknown");
        assert!(matches!(missing, ContextError::Unknown { .. }), "{missing}");
        assert_eq!(
            contexts.current,
            Some(name("b")),
            "a failed use changes nothing"
        );
    }

    #[test]
    fn going_back_with_no_previous_context_is_an_error() {
        let mut contexts = two();
        assert!(matches!(
            contexts.use_previous(),
            Err(ContextError::NoPrevious)
        ));
        // A previous context that was removed by hand is as good as none.
        contexts.previous = Some(name("gone"));
        assert!(matches!(
            contexts.use_previous(),
            Err(ContextError::NoPrevious)
        ));
    }

    #[test]
    fn the_first_context_added_becomes_current_and_a_duplicate_is_refused() {
        let mut contexts = Contexts::empty();
        contexts.add(name("a"), Context::default()).expect("add a");
        contexts.add(name("b"), Context::default()).expect("add b");
        assert_eq!(contexts.current, Some(name("a")));
        let error = contexts
            .add(name("a"), Context::default())
            .expect_err("a taken name");
        assert!(matches!(error, ContextError::Exists { .. }), "{error}");
    }

    #[test]
    fn removing_the_current_context_clears_it() {
        let mut contexts = two();
        contexts.use_context(&name("b")).expect("use b");
        assert_eq!(
            contexts.remove(&name("b")).expect("remove"),
            WasCurrent::Yes
        );
        assert_eq!(contexts.current, None);
        assert!(contexts.current().is_none());
        assert_eq!(contexts.remove(&name("a")).expect("remove"), WasCurrent::No);
        assert_eq!(contexts.previous, None, "previous named a, which is gone");
        assert!(matches!(
            contexts.remove(&name("a")),
            Err(ContextError::Unknown { .. })
        ));
    }

    fn dev_context(dir: &str) -> Context {
        Context {
            fleet: Some("dev".to_owned()),
            control: vec!["127.0.0.1:7400".to_owned()],
            dev_dir: Some(PathBuf::from(dir)),
            ..Context::default()
        }
    }

    #[test]
    fn upsert_dev_makes_dev_current_only_if_none_is() {
        let mut contexts = Contexts::empty();
        assert_eq!(
            contexts.upsert_dev(dev_context("/d1")),
            DevContextOutcome::Current
        );
        assert_eq!(contexts.current, Some(name("dev")));
        // Again: dev wrote it, so it is overwritten, and it stays current.
        assert_eq!(
            contexts.upsert_dev(dev_context("/d2")),
            DevContextOutcome::Current
        );
        assert_eq!(
            contexts.contexts[&name("dev")].dev_dir.as_deref(),
            Some(Path::new("/d2"))
        );

        let mut contexts = two();
        assert_eq!(
            contexts.upsert_dev(dev_context("/d")),
            DevContextOutcome::NotCurrent { current: name("a") }
        );
        assert_eq!(contexts.current, Some(name("a")));
        assert!(contexts.contexts.contains_key(&name("dev")));
    }

    #[test]
    fn upsert_dev_leaves_a_users_own_dev_context_alone() {
        let mut contexts = Contexts::empty();
        contexts
            .add(name("dev"), with_control("mine:1"))
            .expect("add dev");
        contexts.current = None;
        let before = contexts.clone();
        assert_eq!(
            contexts.upsert_dev(dev_context("/d")),
            DevContextOutcome::Kept
        );
        assert_eq!(contexts, before, "nothing changed, not even `current`");
    }

    #[test]
    fn select_prefers_the_flag_then_the_variable_then_the_current_context() {
        let mut contexts = two();
        contexts.add(name("c"), Context::default()).expect("add c");
        let picked = |flag: Option<&str>, variable: Option<&str>| {
            let flag = flag.map(name);
            select(&contexts, flag.as_ref(), variable)
                .expect("a selection")
                .map(|(name, _, by)| (name.to_string(), by))
        };
        assert_eq!(picked(None, None), Some(("a".into(), SelectedBy::Current)));
        assert_eq!(
            picked(None, Some("b")),
            Some(("b".into(), SelectedBy::Variable))
        );
        assert_eq!(
            picked(Some("c"), Some("b")),
            Some(("c".into(), SelectedBy::Flag))
        );
        // An empty variable is unset.
        assert_eq!(
            picked(None, Some("")),
            Some(("a".into(), SelectedBy::Current))
        );
        assert_eq!(
            select(&Contexts::empty(), None, None).expect("a selection"),
            None
        );
    }

    #[test]
    fn select_refuses_an_unknown_name_and_names_it_and_the_file() {
        let dir = tempfile::tempdir().expect("a directory");
        let store = ContextStore::at(dir.path());
        store.update(|_| Ok(())).expect("an empty file");
        let contexts = store.load().expect("load");
        for (flag, variable) in [(Some("nope"), None), (None, Some("nope"))] {
            let flag = flag.map(name);
            let error = select(&contexts, flag.as_ref(), variable).expect_err("unknown");
            assert_eq!(
                error.to_string(),
                format!("context 'nope' doesn't exist in {}", store.path().display())
            );
        }
        let error = select(&contexts, None, Some("Not A Name")).expect_err("invalid");
        assert!(matches!(error, ContextError::InvalidName { .. }), "{error}");
    }

    #[test]
    fn a_missing_file_is_empty() {
        let dir = tempfile::tempdir().expect("a directory");
        let loaded = ContextStore::at(dir.path()).load().expect("load");
        assert_eq!(loaded, Contexts::empty());
        assert!(!dir.path().join(FILE).exists(), "loading writes nothing");
    }

    #[test]
    fn a_change_is_written_and_read_back() {
        let dir = tempfile::tempdir().expect("a directory");
        let store = ContextStore::at(&dir.path().join("config"));
        store
            .update(|contexts| contexts.add(name("a"), with_control("a:1")))
            .expect("add");
        let text = fs::read_to_string(store.path()).expect("the file");
        assert!(text.starts_with(HEADER), "{text}");
        let loaded = store.load().expect("load");
        assert_eq!(loaded.current, Some(name("a")));
        assert_eq!(loaded.contexts[&name("a")].control, ["a:1"]);
    }

    #[test]
    fn a_failed_change_writes_nothing() {
        let dir = tempfile::tempdir().expect("a directory");
        let store = ContextStore::at(dir.path());
        store
            .update(|contexts| contexts.add(name("a"), Context::default()))
            .expect("add");
        let before = fs::read(store.path()).expect("the file");
        let error = store
            .update(|contexts| contexts.use_context(&name("nope")))
            .expect_err("unknown");
        assert!(matches!(error, ContextError::Unknown { .. }), "{error}");
        assert_eq!(fs::read(store.path()).expect("the file"), before);
    }

    #[test]
    fn version_2_is_refused_with_the_files_path() {
        let dir = tempfile::tempdir().expect("a directory");
        let store = ContextStore::at(dir.path());
        // Keys this release doesn't know don't hide the version.
        fs::write(store.path(), "version = 2\nsomething_new = true\n").expect("a file");
        for error in [
            store.load().expect_err("version 2"),
            store.update(|_| Ok(())).expect_err("version 2"),
        ] {
            assert!(
                matches!(&error, ContextError::Version { found: 2, .. }),
                "{error}"
            );
            assert_eq!(
                error.to_string(),
                format!(
                    "{} has format version 2; this release reads version 1",
                    store.path().display()
                )
            );
        }
        assert_eq!(
            fs::read_to_string(store.path()).expect("the file"),
            "version = 2\nsomething_new = true\n",
            "a refused file is left as it was"
        );
    }

    #[test]
    fn a_file_that_isnt_the_format_names_itself() {
        let dir = tempfile::tempdir().expect("a directory");
        let store = ContextStore::at(dir.path());
        for text in [
            "not toml at all [",
            "current = \"a\"\n",
            "version = 1\nx = 1\n",
        ] {
            fs::write(store.path(), text).expect("a file");
            let error = store.load().expect_err(text);
            assert!(
                matches!(error, ContextError::Parse { .. }),
                "{text}: {error}"
            );
            assert!(
                error
                    .to_string()
                    .contains(&store.path().display().to_string()),
                "{error}"
            );
        }
    }

    #[test]
    fn two_threads_updating_lose_neither_change() {
        const EACH: usize = 25;
        let dir = tempfile::tempdir().expect("a directory");
        let store = ContextStore::at(dir.path());
        std::thread::scope(|scope| {
            for thread in ["t", "u"] {
                let store = &store;
                scope.spawn(move || {
                    for number in 0..EACH {
                        let added = name(&format!("{thread}{number}"));
                        store
                            .update(|contexts| contexts.add(added, Context::default()))
                            .expect("update");
                    }
                });
            }
        });
        let loaded = store.load().expect("load");
        assert_eq!(loaded.contexts.len(), 2 * EACH, "{loaded:?}");
    }

    #[cfg(unix)]
    #[test]
    fn the_file_is_0600_and_the_directory_0700() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("a directory");
        let config = dir.path().join("a").join("fleetring");
        let store = ContextStore::at(&config);
        // Written twice: the rename must not widen the mode.
        for _ in 0..2 {
            store.update(|_| Ok(())).expect("update");
        }
        let mode = |path: &Path| fs::metadata(path).expect("metadata").permissions().mode() & 0o777;
        assert_eq!(mode(store.path()), 0o600);
        assert_eq!(mode(&config), 0o700);
        assert_eq!(mode(&store.lock_path()), 0o600);
        assert!(
            !config.join("contexts.toml.tmp").exists(),
            "the temporary file was renamed away"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_looser_file_is_tightened_by_the_next_write() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("a directory");
        let store = ContextStore::at(dir.path());
        fs::write(store.path(), "version = 1\n").expect("a file");
        fs::set_permissions(store.path(), fs::Permissions::from_mode(0o644)).expect("chmod");
        store.update(|_| Ok(())).expect("update");
        let mode = fs::metadata(store.path())
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}
