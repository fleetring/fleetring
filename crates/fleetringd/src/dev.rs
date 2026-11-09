//! Dev mode (ADR-0039): one node on loopback, with a local authority and an admin principal that
//! `fleetring dev` creates once and reuses.
//!
//! [`prepare`] creates whatever the directory lacks and loads the configuration it holds; the
//! binary then runs that configuration as it runs any other. With `dev = true`, validation keeps
//! every address on loopback, however the node is started.

use std::ffi::{OsStr, OsString};
use std::fmt::{self, Write as _};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read as _, Seek as _, Write as _};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fleetring_control::Timestamp;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject as _;
use serde::{Deserialize, Serialize};
use x509_parser::prelude::{FromDer as _, X509Certificate};

use crate::config::{AdminListener, AdminTls, Config, ConfigError, NodeRole, Plaintext};
use crate::tls::{Authority, AuthorityError, DEFAULT_AUTHORITY_LIFETIME, Validity};
use crate::upgrade::{PROCESS_FILE, ProcessFile, Running, running};

/// The port dev mode serves gRPC on unless `--port` says otherwise; the admin listener takes the
/// next one.
pub const DEFAULT_PORT: u16 = 7400;

/// The fleet dev mode's authority issues for.
pub const FLEET: &str = "dev";

/// The dev node's ID.
pub const NODE: &str = "dev";

/// The principal the CLI acts as, which the generated configuration gives every scope.
pub const PRINCIPAL: &str = "admin";

/// The configuration file, in the dev directory.
pub const CONFIG_FILE: &str = "fleetringd.toml";

/// The file that points the CLI at the dev node, in the dev directory.
pub const ENV_FILE: &str = "env";

/// The directory with the authority and the certificates, in the dev directory.
pub const TLS_DIR: &str = "tls";

/// The daemon's data directory, in the dev directory.
pub const DATA_DIR: &str = "data";

/// Where the dev sink writes each database's changes, in the dev directory.
const CHANGES_DIR: &str = "changes";

/// The dev directory's name, in the home directory.
const DIR_NAME: &str = ".fleetring";

/// The log a detached dev node writes its output to, in the dev directory.
pub const LOG_FILE: &str = "fleetring.log";

/// The log as it was before the last rotation, in the dev directory.
pub const LOG_FILE_OLD: &str = "fleetring.log.1";

/// The lock `dev -d` holds while it starts a node, so that two at once start one, in the dev
/// directory.
pub const START_LOCK: &str = "fleetring.start.lock";

/// Where agents' identities live, in the dev directory.
pub const AGENTS_DIR: &str = "agents";

/// The variable a detached child is started with; internal, not a CLI setting.
pub const DETACHED_VAR: &str = "FLEETRING_DEV_DETACHED";

/// A log over this many bytes is renamed to [`LOG_FILE_OLD`] when a detached node starts.
const LOG_ROTATE_BYTES: u64 = 16 * 1024 * 1024;

/// The variables a supervisor hands the processes it starts. A detached node's environment has
/// none, so that it neither looks supervised nor talks to a supervisor that didn't start it.
const SUPERVISOR_VARS: [&str; 9] = [
    "INVOCATION_ID",
    "JOURNAL_STREAM",
    "NOTIFY_SOCKET",
    "LISTEN_PID",
    "LISTEN_FDS",
    "LISTEN_FDNAMES",
    "WATCHDOG_PID",
    "WATCHDOG_USEC",
    "XPC_SERVICE_NAME",
];

/// How often `dev -d` checks whether the node it started is ready.
const READY_POLL: Duration = Duration::from_millis(100);

/// How long one readiness probe may take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// How many of the log's last lines a failed start shows.
const TAIL_LINES: usize = 20;

/// How much of the log's end a failed start reads to find them.
const TAIL_BYTES: u64 = 64 * 1024;

/// How long dev mode's node and admin certificates last: a year, since nobody renews
/// certificates on a laptop.
const LEAF_LIFETIME: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// The names the node certificate carries: every way a client on this machine dials loopback.
const LOOPBACK_NAMES: [&str; 3] = ["127.0.0.1", "::1", "localhost"];

/// The scopes the admin principal gets: every database, every action, and the fleet operations.
const ADMIN_SCOPES: &str = r#"["admin:*", "stream:*", "fleet"]"#;

/// Whether a node runs in dev mode: `dev = true` in its configuration (ADR-0039).
///
/// In dev mode, validation refuses any address that isn't loopback, a role other than `both`,
/// and plaintext, and the control node tells clients, so `fleetringd status` says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(from = "bool")]
#[non_exhaustive]
pub enum DevMode {
    /// The default: a node of a real fleet.
    #[default]
    Off,
    /// `dev = true`: one node on loopback, for development only.
    On,
}

impl From<bool> for DevMode {
    fn from(on: bool) -> Self {
        if on { Self::On } else { Self::Off }
    }
}

/// Checks the rules dev mode adds: every address loopback, both roles, and TLS.
///
/// # Errors
///
/// [`ConfigError::Invalid`] naming the first field that breaks one.
pub(crate) fn validate(config: &Config) -> Result<(), ConfigError> {
    if config.dev == DevMode::Off {
        return Ok(());
    }
    let loopback = |field: &'static str, address: SocketAddr| {
        if address.ip().is_loopback() {
            Ok(())
        } else {
            Err(ConfigError::Invalid {
                field,
                reason: format!(
                    "{address} isn't a loopback address, and dev mode serves loopback only \
                     (ADR-0039)"
                ),
            })
        }
    };
    loopback("listen", config.listen)?;
    if let Some(advertise) = config.node.advertise {
        loopback("node.advertise", advertise)?;
    }
    if let Some(control) = config.control.address {
        loopback("control.address", control)?;
    }
    if config.admin.enabled == AdminListener::Enabled {
        loopback("admin.listen", config.admin.listen)?;
    }
    if config.role != NodeRole::Both {
        return Err(ConfigError::Invalid {
            field: "role",
            reason: "dev mode runs one node with both roles".to_owned(),
        });
    }
    if config.insecure_plaintext == Plaintext::Allowed {
        return Err(ConfigError::Invalid {
            field: "insecure_plaintext",
            reason: "dev mode always runs over TLS, with its own authority".to_owned(),
        });
    }
    Ok(())
}

/// The error returned when the dev directory can't be prepared.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DevError {
    /// No home directory to put `.fleetring` in.
    #[error("can't find the home directory: set {variable}, or pass --data-dir")]
    NoHome {
        /// The environment variable that names it on this platform.
        variable: &'static str,
    },
    /// The port leaves no room for the admin listener's, or is 0.
    #[error(
        "port {port} can't be used: dev mode serves on it and its admin listener on the next \
         one, so it must be between 1 and 65534"
    )]
    Port {
        /// The port asked for.
        port: u16,
    },
    /// The directory's path can't be written into a configuration file.
    #[error(
        "{path} isn't valid UTF-8, which the configuration file needs; pass another --data-dir"
    )]
    NotUtf8 {
        /// The path.
        path: PathBuf,
    },
    /// A file or directory can't be read, created, or written.
    #[error("can't {action} {path}")]
    File {
        /// What was attempted.
        action: &'static str,
        /// The file or directory.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// One file of a certificate and key pair exists without the other.
    #[error("{present} exists without {missing}; {fix}")]
    Incomplete {
        /// The file that exists.
        present: PathBuf,
        /// The file that doesn't.
        missing: PathBuf,
        /// What to do about it.
        fix: String,
    },
    /// Certificates exist, but the authority that issued them doesn't.
    #[error(
        "{dir} holds certificates but no authority (ca.crt and ca.key), and dev mode never \
         creates an authority beside certificates another one issued; restore them, or remove \
         {dir} to start over with a new authority, after which every certificate and token the \
         old one issued stops working (the databases are kept)"
    )]
    NoAuthority {
        /// The TLS directory.
        dir: PathBuf,
    },
    /// The authority in the directory is another fleet's.
    #[error(
        "{path} is the authority of the fleet '{fleet}', not dev mode's, and dev mode issues \
         nothing from another fleet's authority; pass another --data-dir"
    )]
    ForeignAuthority {
        /// The authority certificate.
        path: PathBuf,
        /// The fleet it names.
        fleet: String,
    },
    /// The directory holds something dev mode never writes, and so isn't a dev directory.
    #[error(
        "{dir} isn't a dev mode directory: it holds {entry}, which dev mode never writes, so \
         `fleetring dev` won't touch it; pass an empty or new --data-dir"
    )]
    NotDevDir {
        /// The directory.
        dir: PathBuf,
        /// The first entry dev mode doesn't recognize.
        entry: PathBuf,
    },
    /// A certificate has expired.
    #[error(
        "{path} expired at {at}; remove it and its key, and the next start issues new ones from \
         the same authority"
    )]
    Expired {
        /// The certificate.
        path: PathBuf,
        /// When it expired, in RFC 3339.
        at: String,
    },
    /// A certificate can't be parsed.
    #[error("{path} isn't a certificate: {reason}")]
    Certificate {
        /// The file.
        path: PathBuf,
        /// What is wrong with it.
        reason: String,
    },
    /// A name dev mode uses isn't valid; a bug, since the names are fixed.
    #[error("invalid name '{name}': {reason}")]
    Name {
        /// The name.
        name: &'static str,
        /// What is wrong with it.
        reason: String,
    },
    /// The authority can't be created, loaded, or used.
    #[error(transparent)]
    Authority(#[from] AuthorityError),
    /// The configuration file is invalid.
    #[error("invalid dev mode configuration {path}")]
    Config {
        /// The file.
        path: PathBuf,
        /// What is wrong with it.
        source: ConfigError,
    },
    /// The configuration file doesn't say `dev = true`.
    #[error(
        "{path} isn't a dev mode configuration: it lacks `dev = true`, so `fleetring dev` won't \
         run it; run it with `fleetring serve --config`, or pass another --data-dir"
    )]
    NotDev {
        /// The file.
        path: PathBuf,
    },
    /// `--port` disagrees with the existing configuration.
    #[error(
        "{path} listens on {listen}, not port {port}; edit its `listen`, or start without --port"
    )]
    PortMismatch {
        /// The configuration file.
        path: PathBuf,
        /// The address it listens on.
        listen: SocketAddr,
        /// The port asked for.
        port: u16,
    },
    /// The configuration listens on port 0, which no client could be pointed at.
    #[error(
        "{path} listens on port 0, so the CLI couldn't find the node; set a port in its `listen`"
    )]
    NoPort {
        /// The configuration file.
        path: PathBuf,
    },
    /// The node `dev -d` started exited before it was ready.
    #[error("the dev node exited before it was ready; the last lines of {log}:\n{tail}")]
    Exited {
        /// The log it wrote.
        log: PathBuf,
        /// The log's last lines.
        tail: String,
    },
    /// The node `dev -d` started wasn't ready in time; it may still be starting.
    #[error(
        "the dev node wasn't ready within {timeout:?}; it may still be starting: see {log}, or \
         stop it with `fleetring dev stop`"
    )]
    NotReady {
        /// The log it writes.
        log: PathBuf,
        /// How long `dev -d` waited.
        timeout: Duration,
    },
    /// The directory has no configuration, so dev mode hasn't prepared it.
    #[error(
        "{} has no {CONFIG_FILE}, so it isn't a dev directory dev mode has prepared; start the \
         dev node with `fleetring dev -d` first",
        dir.display()
    )]
    NoConfig {
        /// The directory.
        dir: PathBuf,
    },
    /// Running the dev node in the background needs Unix.
    #[error(
        "running the dev node in the background isn't available on Windows yet; run `fleetring \
         dev` in its own terminal, and press Ctrl-C there to stop it"
    )]
    Unsupported,
}

/// What [`prepare`] created rather than found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Created {
    /// `tls/ca.crt` and `tls/ca.key`.
    Authority,
    /// `tls/node.dev.crt` and `tls/node.dev.key`.
    NodeCertificate,
    /// `tls/client.admin.crt` and `tls/client.admin.key`.
    AdminCertificate,
    /// `fleetringd.toml`.
    Config,
}

impl fmt::Display for Created {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Authority => "the authority",
            Self::NodeCertificate => "the node certificate",
            Self::AdminCertificate => "the admin certificate",
            Self::Config => CONFIG_FILE,
        })
    }
}

/// What happened to the `dev` context when the node started (ADR-0043, point 5.1), for the
/// banner.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DevContextLine {
    /// The `dev` context was written and is current.
    Current,
    /// The `dev` context was written, and another context is current.
    NotCurrent {
        /// The context that is current.
        current: String,
    },
    /// A `dev` context the user made exists; it was left alone.
    Kept,
    /// The contexts file couldn't be written.
    NotWritten {
        /// Why.
        reason: String,
    },
}

impl DevContextLine {
    /// The banner's text for its context line.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::Current => "dev (current)".to_owned(),
            Self::NotCurrent { current } => {
                format!("dev (not current: '{current}' is; run fleetring switch dev)")
            }
            Self::Kept => {
                "dev (left alone: you made a context named dev; dev mode keeps it)".to_owned()
            }
            Self::NotWritten { reason } => {
                format!("not written ({reason}); load the env file below to point the CLI at it")
            }
        }
    }
}

/// A prepared dev directory: its configuration, loaded and checked, and what was created.
#[derive(Debug)]
pub struct Prepared {
    dir: PathBuf,
    config: Config,
    created: Vec<Created>,
}

impl Prepared {
    /// Returns the dev directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the configuration the node runs.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns what this run created, in the order it did; empty when everything was reused.
    #[must_use]
    pub fn created(&self) -> &[Created] {
        &self.created
    }

    /// Returns the file that points the CLI at the node.
    #[must_use]
    pub fn env_file(&self) -> PathBuf {
        self.dir.join(ENV_FILE)
    }

    /// The banner `fleetring dev` prints once the node serves on `listen`, with its admin
    /// listener on `admin`: that it is dev mode, where it is, which context points the CLI at it
    /// (`context`), and what to try.
    #[must_use]
    pub fn banner(
        &self,
        listen: SocketAddr,
        admin: Option<SocketAddr>,
        context: &DevContextLine,
    ) -> String {
        let mut text = String::from(
            "fleetring is running in dev mode: one node on loopback with a local authority, \
             for development only.\n\n",
        );
        let _ = writeln!(text, "  address    {listen}");
        if let Some(admin) = admin {
            let _ = writeln!(
                text,
                "  admin      http://{admin}/metrics, /healthz, /readyz"
            );
        }
        let _ = writeln!(text, "  data dir   {}", self.dir.display());
        let _ = writeln!(text, "  state      {}", self.state());
        let _ = writeln!(text, "  context    {}", context.describe());
        let _ = write!(text, "\nTry it from another terminal:\n\n");
        if matches!(context, DevContextLine::NotWritten { .. }) {
            let _ = writeln!(
                text,
                "  . {}",
                shell_quote(&self.env_file().display().to_string())
            );
        }
        let _ = write!(
            text,
            "  fleetring db create notes\n  fleetring status\n\n\
             Ctrl-C stops it; the next start reuses everything in {}.\n",
            self.dir.display(),
        );
        text
    }

    /// What was created and what reused, in a few words.
    fn state(&self) -> String {
        if self.created.is_empty() {
            return "reused the authority, the certificates, and the configuration".to_owned();
        }
        let names: Vec<String> = self.created.iter().map(ToString::to_string).collect();
        let created = match names.as_slice() {
            [one] => one.clone(),
            [first, second] => format!("{first} and {second}"),
            [rest @ .., last] => format!("{}, and {last}", rest.join(", ")),
            [] => String::new(),
        };
        if self.created.len() == 4 {
            format!("created {created}")
        } else {
            format!("created {created}; reused the rest")
        }
    }
}

/// The dev directory unless `--data-dir` names another: `.fleetring` in the home directory,
/// `$HOME` on Unix and `%USERPROFILE%` on Windows.
///
/// # Errors
///
/// [`DevError::NoHome`] if the variable is unset or empty.
pub fn default_dir() -> Result<PathBuf, DevError> {
    default_dir_in(&|name| std::env::var_os(name))
}

/// [`default_dir`], with the environment read through `env`, so a test needn't touch the
/// process's.
///
/// # Errors
///
/// [`DevError::NoHome`] if the variable is unset or empty.
pub fn default_dir_in(env: &dyn Fn(&str) -> Option<OsString>) -> Result<PathBuf, DevError> {
    let variable = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
    match env(variable) {
        Some(home) if !home.is_empty() => Ok(PathBuf::from(home).join(DIR_NAME)),
        _ => Err(DevError::NoHome { variable }),
    }
}

/// A dev directory that passed [`check_dev_dir`], with its configuration and its authority as
/// they were checked. Only [`check_dev_dir`] makes one, so a mint from a dev authority can't
/// happen without the check (ADR-0043, point 8.1).
#[derive(Debug)]
pub struct DevChecked {
    dir: PathBuf,
    config: Config,
    authority: Authority,
}

impl DevChecked {
    /// Returns the dev directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the directory's configuration, which says `dev = true` and passed validation.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the directory's authority, for the fleet [`FLEET`].
    #[must_use]
    pub fn authority(&self) -> &Authority {
        &self.authority
    }

    /// Returns the directory with the authority and the certificates.
    #[must_use]
    pub fn tls_dir(&self) -> PathBuf {
        self.dir.join(TLS_DIR)
    }
}

/// Checks, reading only, that `dir` is a dev directory dev mode prepared (ADR-0039, point 3):
/// its `fleetringd.toml` exists, says `dev = true`, and passes validation, so every address is
/// loopback; its `tls/` holds only the files dev mode writes; and its authority is for the fleet
/// [`FLEET`]. It creates nothing. Run it immediately before minting from the directory's
/// authority, whether or not a node holds the directory, since a context's `dev_dir` only says
/// where to look (ADR-0043, point 8.1).
///
/// # Errors
///
/// [`DevError::NoConfig`] if there is no configuration, and otherwise the [`DevError`] naming
/// the check that failed.
pub fn check_dev_dir(dir: &Path) -> Result<DevChecked, DevError> {
    let path = dir.join(CONFIG_FILE);
    if !exists(&path)? {
        return Err(DevError::NoConfig {
            dir: dir.to_owned(),
        });
    }
    let config = load_dev_config(&path, None)?;
    let tls = dir.join(TLS_DIR);
    check_entries(&tls, &TLS_FILES, dir)?;
    let authority = Authority::load(&tls)?;
    if authority.fleet().as_str() != FLEET {
        return Err(DevError::ForeignAuthority {
            path: tls.join("ca.crt"),
            fleet: authority.fleet().to_string(),
        });
    }
    Ok(DevChecked {
        dir: dir.to_owned(),
        config,
        authority,
    })
}

/// Creates whatever `dir` lacks for a dev node, then loads its configuration.
///
/// On a first run it creates, under `dir`: an authority for the fleet [`FLEET`], a certificate
/// for the node [`NODE`] at loopback, one for the principal [`PRINCIPAL`], and a configuration
/// that listens on `127.0.0.1:<port>`, [`DEFAULT_PORT`] unless `port` is given. Certificates
/// are valid from `now`. Later runs reuse every file and never overwrite one; a pair with a
/// file missing is completed only where that is safe. The CLI's environment file is rewritten
/// from the configuration every time, so it follows an edited port.
///
/// It refuses a directory that isn't dev mode's before it writes anything: one whose
/// `fleetringd.toml` lacks `dev = true`, one without that file that holds anything but `tls/`,
/// `data/`, `changes/`, `env`, `agents/`, and a detached node's log and start lock (what a
/// partial dev state leaves), one whose `tls/` holds a
/// file dev mode never writes, and one whose authority is another fleet's.
///
/// On Unix, directories are created `0700` and private keys `0600`.
///
/// # Errors
///
/// [`DevError`] if `port` is 0 or 65535, a file can't be read or written, the directory isn't
/// dev mode's, the state is incomplete in a way that can't be completed safely, a certificate
/// expired, or the configuration is invalid, isn't a dev mode one, or disagrees with `port`.
pub fn prepare(dir: &Path, port: Option<u16>, now: Timestamp) -> Result<Prepared, DevError> {
    if let Some(port) = port {
        check_port(port)?;
    }
    // Everything that could refuse the directory is checked before anything is written to it,
    // so a directory that isn't dev mode's is left exactly as it was.
    let path = dir.join(CONFIG_FILE);
    let existing = check_existing(dir, &path, port)?;
    let tls = dir.join(TLS_DIR);
    let loaded = match pair(&tls, "ca")? {
        Pair::Present => {
            let authority = Authority::load(&tls)?;
            if authority.fleet().as_str() != FLEET {
                return Err(DevError::ForeignAuthority {
                    path: tls.join("ca.crt"),
                    fleet: authority.fleet().to_string(),
                });
            }
            Some(authority)
        }
        Pair::Absent => {
            for stem in [node_stem(), client_stem()] {
                for extension in ["crt", "key"] {
                    if exists(&tls.join(format!("{stem}.{extension}")))? {
                        return Err(DevError::NoAuthority { dir: tls });
                    }
                }
            }
            None
        }
    };
    let node_stem = node_stem();
    let client_stem = client_stem();
    let node = pair(&tls, &node_stem)?;
    let client = pair(&tls, &client_stem)?;
    if node == Pair::Present {
        check_unexpired(&tls.join(format!("{node_stem}.crt")), now)?;
    }
    if client == Pair::Present {
        check_unexpired(&tls.join(format!("{client_stem}.crt")), now)?;
    }

    for path in [
        dir.to_owned(),
        tls.clone(),
        dir.join(DATA_DIR),
        dir.join(CHANGES_DIR),
    ] {
        create_private_dir(&path)?;
    }
    let mut created = Vec::new();
    let authority = match loaded {
        Some(authority) => authority,
        None => {
            let authority =
                Authority::create(name(FLEET)?, Validity::new(now, DEFAULT_AUTHORITY_LIFETIME))?;
            authority.write(&tls)?;
            created.push(Created::Authority);
            authority
        }
    };
    let leaf = Validity::new(now, LEAF_LIFETIME);
    if node == Pair::Absent {
        let names: Vec<String> = LOOPBACK_NAMES.iter().map(|&name| name.to_owned()).collect();
        authority
            .issue_node(&name(NODE)?, &names, leaf)?
            .write(&tls, &node_stem)?;
        created.push(Created::NodeCertificate);
    }
    if client == Pair::Absent {
        authority
            .issue_client(&name(PRINCIPAL)?, leaf)?
            .write(&tls, &client_stem)?;
        created.push(Created::AdminCertificate);
    }
    let config = match existing {
        Some(config) => config,
        None => {
            let text = config_text(dir, port.unwrap_or(DEFAULT_PORT))?;
            write_new(&path, &text)?;
            created.push(Created::Config);
            load_dev_config(&path, port)?
        }
    };
    let Some(control) = config.control_target() else {
        return Err(DevError::NoPort { path });
    };
    let env = dir.join(ENV_FILE);
    fs::write(&env, env_text(&tls, control)).map_err(|source| DevError::File {
        action: "write",
        path: env,
        source,
    })?;
    Ok(Prepared {
        dir: dir.to_owned(),
        config,
        created,
    })
}

/// The entries a dev directory may hold before it has a configuration: what an interrupted or
/// partly removed dev mode state leaves.
const PARTIAL_ENTRIES: [&str; 8] = [
    TLS_DIR,
    DATA_DIR,
    CHANGES_DIR,
    ENV_FILE,
    LOG_FILE,
    LOG_FILE_OLD,
    START_LOCK,
    AGENTS_DIR,
];

/// The files dev mode writes into its TLS directory, and the only ones it may hold.
const TLS_FILES: [&str; 6] = [
    "ca.crt",
    "ca.key",
    "node.dev.crt",
    "node.dev.key",
    "client.admin.crt",
    "client.admin.key",
];

/// Checks, without writing anything, that `dir` is dev mode's: its configuration, if it has
/// one, says `dev = true` and agrees with `port`, which it then returns; without one, it holds
/// nothing but a partial dev state.
fn check_existing(dir: &Path, path: &Path, port: Option<u16>) -> Result<Option<Config>, DevError> {
    if exists(path)? {
        let config = load_dev_config(path, port)?;
        if config.control_target().is_none() {
            return Err(DevError::NoPort {
                path: path.to_owned(),
            });
        }
        check_entries(&dir.join(TLS_DIR), &TLS_FILES, dir)?;
        return Ok(Some(config));
    }
    check_entries(dir, &PARTIAL_ENTRIES, dir)?;
    check_entries(&dir.join(TLS_DIR), &TLS_FILES, dir)?;
    Ok(None)
}

/// Refuses `within` if it holds an entry `allowed` doesn't list. A missing directory holds
/// nothing.
fn check_entries(within: &Path, allowed: &[&str], dir: &Path) -> Result<(), DevError> {
    let entries = match fs::read_dir(within) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(DevError::File {
                action: "read",
                path: within.to_owned(),
                source,
            });
        }
    };
    for entry in entries {
        let entry = entry.map_err(|source| DevError::File {
            action: "read",
            path: within.to_owned(),
            source,
        })?;
        let name = entry.file_name();
        if !name.to_str().is_some_and(|name| allowed.contains(&name)) {
            return Err(DevError::NotDevDir {
                dir: dir.to_owned(),
                entry: entry.path(),
            });
        }
    }
    Ok(())
}

/// Loads the configuration at `path`, and refuses one that isn't dev mode's or listens on
/// another port than `port`.
fn load_dev_config(path: &Path, port: Option<u16>) -> Result<Config, DevError> {
    let config = Config::load(path).map_err(|source| DevError::Config {
        path: path.to_owned(),
        source,
    })?;
    if config.dev != DevMode::On {
        return Err(DevError::NotDev {
            path: path.to_owned(),
        });
    }
    if let Some(port) = port
        && config.listen.port() != port
    {
        return Err(DevError::PortMismatch {
            path: path.to_owned(),
            listen: config.listen,
            port,
        });
    }
    Ok(config)
}

fn check_port(port: u16) -> Result<(), DevError> {
    if port == 0 || port == u16::MAX {
        return Err(DevError::Port { port });
    }
    Ok(())
}

fn node_stem() -> String {
    format!("node.{NODE}")
}

fn client_stem() -> String {
    format!("client.{PRINCIPAL}")
}

/// A name dev mode uses, parsed, or the error naming it.
fn name<T>(text: &'static str) -> Result<T, DevError>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    text.parse().map_err(|error: T::Err| DevError::Name {
        name: text,
        reason: error.to_string(),
    })
}

/// Whether a certificate's two files exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pair {
    Present,
    Absent,
}

/// Looks for `<stem>.crt` and `<stem>.key` in `dir`.
///
/// # Errors
///
/// [`DevError::Incomplete`] if one exists without the other, or [`DevError::File`] if either
/// can't be checked.
fn pair(dir: &Path, stem: &str) -> Result<Pair, DevError> {
    let crt = dir.join(format!("{stem}.crt"));
    let key = dir.join(format!("{stem}.key"));
    let fix = |present: &Path| {
        if stem == "ca" {
            format!(
                "dev mode never replaces an authority: restore the missing file, or remove {} \
                 to start over with a new authority, after which every certificate and token the \
                 old one issued stops working (the databases are kept)",
                dir.display()
            )
        } else {
            format!(
                "remove {}, and the next start issues a new pair from the same authority",
                present.display()
            )
        }
    };
    match (exists(&crt)?, exists(&key)?) {
        (true, true) => Ok(Pair::Present),
        (false, false) => Ok(Pair::Absent),
        (true, false) => Err(DevError::Incomplete {
            fix: fix(&crt),
            present: crt,
            missing: key,
        }),
        (false, true) => Err(DevError::Incomplete {
            fix: fix(&key),
            present: key,
            missing: crt,
        }),
    }
}

fn exists(path: &Path) -> Result<bool, DevError> {
    path.try_exists().map_err(|source| DevError::File {
        action: "check",
        path: path.to_owned(),
        source,
    })
}

/// Refuses a certificate that has expired by `now`.
fn check_unexpired(path: &Path, now: Timestamp) -> Result<(), DevError> {
    let pem = fs::read(path).map_err(|source| DevError::File {
        action: "read",
        path: path.to_owned(),
        source,
    })?;
    let invalid = |reason: &str| DevError::Certificate {
        path: path.to_owned(),
        reason: reason.to_owned(),
    };
    let der = CertificateDer::from_pem_slice(&pem).map_err(|_| invalid("not PEM"))?;
    let (_, certificate) =
        X509Certificate::from_der(der.as_ref()).map_err(|_| invalid("not X.509"))?;
    let not_after = certificate.validity().not_after;
    let expired = u64::try_from(not_after.timestamp()).map_or(true, |seconds| {
        seconds.saturating_mul(1_000) <= now.as_millis()
    });
    if expired {
        return Err(DevError::Expired {
            path: path.to_owned(),
            at: not_after.to_string(),
        });
    }
    Ok(())
}

/// Creates `path` and its missing parents, `0700` on Unix; an existing directory is left as it
/// is.
fn create_private_dir(path: &Path) -> Result<(), DevError> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder.create(path).map_err(|source| DevError::File {
        action: "create",
        path: path.to_owned(),
        source,
    })
}

/// Writes a file that must not exist yet.
fn write_new(path: &Path, contents: &str) -> Result<(), DevError> {
    let file_error = |action, source| DevError::File {
        action,
        path: path.to_owned(),
        source,
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| file_error("create", source))?;
    file.write_all(contents.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|source| file_error("write", source))
}

/// The configuration a first run writes into `dir`, listening on `port`.
fn config_text(dir: &Path, port: u16) -> Result<String, DevError> {
    check_port(port)?;
    let path = |name: &str| -> Result<String, DevError> {
        let path = dir.join(name);
        path.to_str()
            .map(toml_string)
            .ok_or(DevError::NotUtf8 { path: path.clone() })
    };
    let tls = dir.join(TLS_DIR);
    let in_tls = |name: String| -> Result<String, DevError> {
        let path = tls.join(name);
        path.to_str()
            .map(toml_string)
            .ok_or(DevError::NotUtf8 { path: path.clone() })
    };
    let admin = port + 1;
    Ok(format!(
        "# fleetring dev mode (ADR-0039): one node on loopback with a local authority, for\n\
         # development only. `fleetring dev` wrote this file and never rewrites it. Edit it if you\n\
         # like, but every address must stay loopback: with `dev = true`, the daemon refuses any\n\
         # other.\n\
         dev = true\n\
         data_dir = {data_dir}\n\
         listen = \"127.0.0.1:{port}\"\n\
         role = \"both\"\n\
         \n\
         [node]\n\
         id = \"{NODE}\"\n\
         \n\
         [membership]\n\
         # A stop keeps the node's databases placed on it, for the next start.\n\
         on_shutdown = \"stay\"\n\
         \n\
         [replication]\n\
         # One node, so a database created without --replication-factor gets one replica.\n\
         replication_factor = 1\n\
         \n\
         [fork]\n\
         # A small disk margin beside a fork's copy: a laptop's disk is rarely 95% free.\n\
         admission_min_free_bytes = 67108864\n\
         admission_min_free_percent = 0\n\
         \n\
         [tls]\n\
         ca = {ca}\n\
         cert = {cert}\n\
         key = {key}\n\
         \n\
         # The principal the CLI acts as, with every scope.\n\
         [[principal]]\n\
         name = \"{PRINCIPAL}\"\n\
         scopes = {ADMIN_SCOPES}\n\
         \n\
         [admin]\n\
         listen = \"127.0.0.1:{admin}\"\n\
         \n\
         # Every database's row changes, one JSON line per transaction, in changes/<database>.ndjson.\n\
         [[changes.sink]]\n\
         name = \"dev\"\n\
         kind = \"file\"\n\
         databases = [\"*\"]\n\
         path = {changes}\n",
        data_dir = path(DATA_DIR)?,
        ca = in_tls("ca.crt".to_owned())?,
        cert = in_tls(format!("{}.crt", node_stem()))?,
        key = in_tls(format!("{}.key", node_stem()))?,
        changes = path(CHANGES_DIR)?,
    ))
}

/// The environment file: the control address, the TLS directory, and the principal, so the
/// commands need no flags. FR-056 replaces it with a `dev` context.
fn env_text(tls: &Path, control: SocketAddr) -> String {
    format!(
        "# Points the fleetring commands at the dev mode node (ADR-0039). Load it with `. <this file>`.\n\
         # `fleetring dev` rewrites it from fleetringd.toml at every start.\n\
         export FLEETRING_CONTROL={}\n\
         export FLEETRING_TLS_DIR={}\n\
         export FLEETRING_AS={PRINCIPAL}\n",
        shell_quote(&control.to_string()),
        shell_quote(&tls.display().to_string()),
    )
}

/// `text` as one word for a POSIX shell.
fn shell_quote(text: &str) -> String {
    let plain = !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"/._-:~+,@%".contains(&byte));
    if plain {
        text.to_owned()
    } else {
        format!("'{}'", text.replace('\'', r"'\''"))
    }
}

/// `text` as a TOML basic string.
fn toml_string(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for character in text.chars() {
        match character {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            control if control.is_control() => {
                let _ = write!(quoted, "\\u{:04X}", u32::from(control));
            }
            other => quoted.push(other),
        }
    }
    quoted.push('"');
    quoted
}

/// Whether this process is a node `dev -d` started: [`DETACHED_VAR`] is set and not empty.
#[must_use]
pub fn started_detached() -> bool {
    std::env::var_os(DETACHED_VAR).is_some_and(|value| !value.is_empty())
}

/// What [`detach`] found or did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Detached {
    /// It started a node, which is now ready.
    Started {
        /// The node's process ID.
        pid: u32,
    },
    /// A node already ran from the directory, so it started none.
    AlreadyRunning {
        /// The running node's process ID.
        pid: u32,
    },
}

impl Detached {
    /// The node's process ID, started or found.
    #[must_use]
    pub fn pid(self) -> u32 {
        match self {
            Self::Started { pid } | Self::AlreadyRunning { pid } => pid,
        }
    }
}

/// Starts a dev node in the background from `prepared`, unless one already runs from its
/// directory, and waits until it is ready (ADR-0043, point 5.2).
///
/// The node is `exe` run as `dev --data-dir <dir>`, with `--port <port>` if given and `argv[0]`
/// set to `arg0`, with stdin from `/dev/null`, stdout and stderr appended to [`LOG_FILE`] in the
/// dev directory, [`DETACHED_VAR`] set, and no supervisor's variables. It is ready once its
/// process file names it and its admin listener's `/readyz` answers 200, or, without an admin
/// listener, once its `listen` address accepts a connection. A start that would find the log over
/// 16 MiB first renames it to [`LOG_FILE_OLD`].
///
/// # Errors
///
/// [`DevError::Exited`] if the node exits first, [`DevError::NotReady`] if it isn't ready within
/// `timeout` (it is left running), [`DevError::File`] if the start lock, the log, or the process
/// file can't be used or the binary can't be started, and [`DevError::Unsupported`] at once on
/// Windows.
pub async fn detach(
    prepared: &Prepared,
    exe: &Path,
    arg0: &OsStr,
    port: Option<u16>,
    timeout: Duration,
) -> Result<Detached, DevError> {
    // Windows has no SIGTERM to drain a detached node with, nor `setsid` (ADR-0043, point 5.6).
    if cfg!(not(unix)) {
        return Err(DevError::Unsupported);
    }
    // Held until the node is ready or has failed, so that a second `dev -d` waits for this one
    // and then finds its node running. Closing the file releases it.
    let _start_lock = lock_start(prepared.dir())?;
    detach_locked(prepared, exe, arg0, port, timeout).await
}

/// Prepares `dir` and starts its dev node in the background, unless one already runs from it,
/// all under the start lock, so that two at once prepare and start one node (ADR-0043, point
/// 8.1). It refuses a directory that isn't dev mode's before it creates anything in it, the start
/// lock included. Certificates it creates are valid from `now`.
///
/// # Errors
///
/// As [`prepare`] and [`detach`].
pub async fn start_detached(
    dir: &Path,
    exe: &Path,
    arg0: &OsStr,
    timeout: Duration,
    now: Timestamp,
) -> Result<(Prepared, Detached), DevError> {
    if cfg!(not(unix)) {
        return Err(DevError::Unsupported);
    }
    check_existing(dir, &dir.join(CONFIG_FILE), None)?;
    create_private_dir(dir)?;
    let _start_lock = lock_start(dir)?;
    let prepared = prepare(dir, None, now)?;
    let detached = detach_locked(&prepared, exe, arg0, None, timeout).await?;
    Ok((prepared, detached))
}

/// Opens and locks [`START_LOCK`] in `dir`, waiting for another holder; dropping the file
/// releases it.
fn lock_start(dir: &Path) -> Result<File, DevError> {
    let lock_path = dir.join(START_LOCK);
    let start_lock = open_private(
        &lock_path,
        OpenOptions::new().write(true).create(true).truncate(false),
    )?;
    start_lock.lock().map_err(|source| DevError::File {
        action: "lock",
        path: lock_path,
        source,
    })?;
    Ok(start_lock)
}

/// [`detach`]'s work, once the start lock is held.
async fn detach_locked(
    prepared: &Prepared,
    exe: &Path,
    arg0: &OsStr,
    port: Option<u16>,
    timeout: Duration,
) -> Result<Detached, DevError> {
    let dir = prepared.dir();
    let data_dir = &prepared.config().data_dir;
    if let Running::Yes(file) = running_in(data_dir)? {
        return Ok(Detached::AlreadyRunning { pid: file.pid });
    }
    let log_path = dir.join(LOG_FILE);
    rotate(&log_path, &dir.join(LOG_FILE_OLD))?;
    let log = open_private(&log_path, OpenOptions::new().append(true).create(true))?;
    let mut child = spawn(exe, arg0, dir, port, log)?;
    let pid = child.id();
    let probe = Probe::of(prepared.config());
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let exited = child.try_wait().map_err(|source| DevError::File {
            action: "wait for",
            path: exe.to_owned(),
            source,
        })?;
        if exited.is_some() {
            return Err(DevError::Exited {
                tail: tail(&log_path),
                log: log_path,
            });
        }
        if probe.ready(data_dir, pid).await {
            // Dropping the handle neither kills nor waits for the node.
            drop(child);
            return Ok(Detached::Started { pid });
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(DevError::NotReady {
                log: log_path,
                timeout,
            });
        }
        tokio::time::sleep(READY_POLL).await;
    }
}

/// Starts the detached node, writing its output to `log`.
fn spawn(
    exe: &Path,
    arg0: &OsStr,
    dir: &Path,
    port: Option<u16>,
    log: File,
) -> Result<std::process::Child, DevError> {
    let failed = |action, source| DevError::File {
        action,
        path: exe.to_owned(),
        source,
    };
    let stderr = log
        .try_clone()
        .map_err(|source| failed("open the log for", source))?;
    let mut command = std::process::Command::new(exe);
    #[cfg(unix)]
    std::os::unix::process::CommandExt::arg0(&mut command, arg0);
    #[cfg(not(unix))]
    let _ = arg0;
    command.arg("dev").arg("--data-dir").arg(dir);
    if let Some(port) = port {
        command.arg("--port").arg(port.to_string());
    }
    command.env(DETACHED_VAR, "1");
    for name in SUPERVISOR_VARS {
        command.env_remove(name);
    }
    command
        .stdin(std::process::Stdio::null())
        .stdout(log)
        .stderr(stderr)
        .spawn()
        .map_err(|source| failed("start", source))
}

/// Opens `path` with `options`, `0600` on Unix if it is created.
fn open_private(path: &Path, options: &mut OpenOptions) -> Result<File, DevError> {
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(options, 0o600);
    options.open(path).map_err(|source| DevError::File {
        action: "open",
        path: path.to_owned(),
        source,
    })
}

/// Renames `log` to `old` if it is over [`LOG_ROTATE_BYTES`].
fn rotate(log: &Path, old: &Path) -> Result<(), DevError> {
    match fs::metadata(log) {
        Ok(metadata) if metadata.len() > LOG_ROTATE_BYTES => {
            fs::rename(log, old).map_err(|source| DevError::File {
                action: "rotate",
                path: log.to_owned(),
                source,
            })
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(DevError::File {
            action: "check",
            path: log.to_owned(),
            source,
        }),
    }
}

/// The last [`TAIL_LINES`] lines of the log at `path`, indented, or why they can't be read.
fn tail(path: &Path) -> String {
    let read = || -> io::Result<String> {
        let mut file = File::open(path)?;
        let length = file.metadata()?.len();
        file.seek(io::SeekFrom::Start(length.saturating_sub(TAIL_BYTES)))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    };
    match read() {
        Ok(text) => {
            let lines: Vec<&str> = text.lines().collect();
            let last = lines.get(lines.len().saturating_sub(TAIL_LINES)..);
            last.unwrap_or_default()
                .iter()
                .map(|line| format!("  {line}"))
                .collect::<Vec<_>>()
                .join("\n")
        }
        Err(error) => format!("  (can't read it: {error})"),
    }
}

/// How `dev -d` tells that the node it started serves.
enum Probe {
    /// `GET /readyz` on the admin listener answers 200.
    Readyz {
        /// Boxed: the client is large, and there is one probe per `dev -d`.
        client: Box<crate::https::HttpClient>,
        uri: hyper::Uri,
    },
    /// The node's `listen` address accepts a connection.
    Connect(SocketAddr),
}

impl Probe {
    /// The probe for a node running `config`: its admin listener's `/readyz`, over HTTPS if the
    /// listener serves it; a connection to `listen` if it has no admin listener.
    fn of(config: &Config) -> Self {
        let connect = Self::Connect(config.listen);
        if config.admin.enabled != AdminListener::Enabled || config.admin.listen.port() == 0 {
            return connect;
        }
        let (scheme, ca, policy) = match config.admin.tls {
            AdminTls::Off => ("http", None, crate::https::HttpPolicy::AllowHttp),
            AdminTls::NodeIdentity => (
                "https",
                config.tls.as_ref().map(|tls| tls.ca.as_path()),
                crate::https::HttpPolicy::HttpsOnly,
            ),
        };
        let uri = format!("{scheme}://{}/readyz", config.admin.listen).parse();
        match (crate::https::client(ca, None, policy), uri) {
            (Ok(client), Ok(uri)) => Self::Readyz {
                client: Box::new(client),
                uri,
            },
            _ => connect,
        }
    }

    /// Whether the node `pid`, running from `data_dir`, is ready: its process file names it and
    /// the probe succeeds.
    async fn ready(&self, data_dir: &Path, pid: u32) -> bool {
        if !matches!(ProcessFile::read(data_dir), Ok(Some(file)) if file.pid == pid) {
            return false;
        }
        match self {
            Self::Readyz { client, uri } => {
                let Ok(request) = hyper::Request::get(uri.clone())
                    .body(http_body_util::Full::new(hyper::body::Bytes::new()))
                else {
                    return false;
                };
                matches!(
                    tokio::time::timeout(PROBE_TIMEOUT, client.request(request)).await,
                    Ok(Ok(response)) if response.status() == hyper::StatusCode::OK
                )
            }
            Self::Connect(address) => matches!(
                tokio::time::timeout(PROBE_TIMEOUT, tokio::net::TcpStream::connect(address)).await,
                Ok(Ok(_))
            ),
        }
    }
}

/// [`running`] for `data_dir`, with its error as a [`DevError`].
fn running_in(data_dir: &Path) -> Result<Running, DevError> {
    running(data_dir).map_err(|source| DevError::File {
        action: "read",
        path: data_dir.join(PROCESS_FILE),
        source,
    })
}

/// A dev node's state, for `fleetring dev status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DevStatus {
    /// Whether a node runs from the directory.
    pub running: bool,
    /// The running node's process ID.
    pub pid: Option<u32>,
    /// The address the running node serves on.
    pub address: Option<SocketAddr>,
    /// The running node's release.
    pub release: Option<String>,
    /// The dev directory.
    pub data_dir: PathBuf,
    /// The log a detached node writes, if there is one.
    pub log: Option<PathBuf>,
}

/// The state of the dev node of the dev directory `dir`: whether it runs, and what it says about
/// itself if it does. A directory with no configuration has no node running.
///
/// # Errors
///
/// [`DevError`] if the directory's configuration can't be read or isn't dev mode's, or its
/// process file can't be read.
pub fn status(dir: &Path) -> Result<DevStatus, DevError> {
    let path = dir.join(CONFIG_FILE);
    let config = if exists(&path)? {
        Some(load_dev_config(&path, None)?)
    } else {
        None
    };
    let data_dir = config
        .as_ref()
        .map_or_else(|| dir.join(DATA_DIR), |config| config.data_dir.clone());
    let log = dir.join(LOG_FILE);
    let log = exists(&log)?.then_some(log);
    Ok(match running_in(&data_dir)? {
        Running::Yes(file) => DevStatus {
            running: true,
            pid: Some(file.pid),
            address: config.map(|config| config.listen),
            release: Some(file.version),
            data_dir: dir.to_owned(),
            log,
        },
        Running::No | Running::Stale { .. } => DevStatus {
            running: false,
            pid: None,
            address: None,
            release: None,
            data_dir: dir.to_owned(),
            log,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: Timestamp = Timestamp::from_millis(1_790_000_000_000);

    fn prepared(dir: &Path) -> Prepared {
        prepare(dir, Some(17_400), NOW).expect("a dev directory")
    }

    #[test]
    fn a_first_run_creates_everything_and_a_second_reuses_it() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("dev");
        let first = prepared(&dir);
        assert_eq!(
            first.created(),
            [
                Created::Authority,
                Created::NodeCertificate,
                Created::AdminCertificate,
                Created::Config
            ]
        );
        assert_eq!(first.config().dev, DevMode::On);
        assert_eq!(first.config().listen.to_string(), "127.0.0.1:17400");
        assert_eq!(first.config().admin.listen.to_string(), "127.0.0.1:17401");
        // A fork keeps a small disk margin on a dev node.
        assert_eq!(first.config().fork.admission_min_free_bytes, 64 << 20);
        assert_eq!(first.config().fork.admission_min_free_percent, 0);
        let second = prepare(&dir, None, NOW).expect("the same directory");
        assert!(second.created().is_empty(), "{:?}", second.created());
        assert_eq!(second.config(), first.config());
        let env = fs::read_to_string(dir.join(ENV_FILE)).expect("the env file");
        assert!(
            env.contains("export FLEETRING_CONTROL=127.0.0.1:17400\n"),
            "{env}"
        );
        assert!(env.contains("export FLEETRING_AS=admin\n"), "{env}");
    }

    #[cfg(unix)]
    #[test]
    fn keys_are_private_and_so_are_directories() {
        use std::os::unix::fs::PermissionsExt as _;
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("dev");
        prepared(&dir);
        let mode =
            |path: PathBuf| fs::metadata(&path).expect("metadata").permissions().mode() & 0o777;
        for sub in ["", TLS_DIR, DATA_DIR, CHANGES_DIR] {
            assert_eq!(mode(dir.join(sub)), 0o700, "{sub}");
        }
        for key in ["ca.key", "node.dev.key", "client.admin.key"] {
            assert_eq!(mode(dir.join(TLS_DIR).join(key)), 0o600, "{key}");
        }
    }

    #[test]
    fn a_missing_leaf_certificate_is_issued_from_the_existing_authority() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("dev");
        prepared(&dir);
        let tls = dir.join(TLS_DIR);
        let authority = fs::read(tls.join("ca.crt")).expect("ca.crt");
        for name in ["node.dev.crt", "node.dev.key"] {
            fs::remove_file(tls.join(name)).expect("removed");
        }
        let again = prepare(&dir, None, NOW).expect("completed");
        assert_eq!(again.created(), [Created::NodeCertificate]);
        assert_eq!(fs::read(tls.join("ca.crt")).expect("ca.crt"), authority);
    }

    #[test]
    fn an_incomplete_pair_fails_and_names_both_files() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("dev");
        prepared(&dir);
        let tls = dir.join(TLS_DIR);
        fs::remove_file(tls.join("ca.key")).expect("removed");
        let error = prepare(&dir, None, NOW).expect_err("no ca.key").to_string();
        assert!(error.contains("ca.crt exists without"), "{error}");
        assert!(error.contains("never replaces an authority"), "{error}");
        assert!(tls.join("ca.crt").exists(), "the authority is untouched");
    }

    #[test]
    fn certificates_without_their_authority_are_refused() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("dev");
        prepared(&dir);
        let tls = dir.join(TLS_DIR);
        for name in ["ca.crt", "ca.key"] {
            fs::remove_file(tls.join(name)).expect("removed");
        }
        let error = prepare(&dir, None, NOW).expect_err("orphaned").to_string();
        assert!(error.contains("no authority"), "{error}");
        assert!(!tls.join("ca.crt").exists(), "no new authority was created");
    }

    #[test]
    fn an_expired_certificate_is_named() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("dev");
        prepared(&dir);
        let later = Timestamp::from_millis(NOW.as_millis() + 2 * 365 * 24 * 60 * 60 * 1_000);
        let error = prepare(&dir, None, later).expect_err("expired").to_string();
        assert!(error.contains("node.dev.crt expired at"), "{error}");
    }

    #[test]
    fn the_port_must_match_the_configuration_and_leave_room_for_the_admin_listener() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("dev");
        prepared(&dir);
        let error = prepare(&dir, Some(17_500), NOW).expect_err("another port");
        assert!(
            matches!(error, DevError::PortMismatch { port: 17_500, .. }),
            "{error}"
        );
        for port in [0, u16::MAX] {
            let error = prepare(&root.path().join("other"), Some(port), NOW).expect_err("bad");
            assert!(matches!(error, DevError::Port { .. }), "{error}");
        }
    }

    #[test]
    fn a_configuration_without_dev_is_refused() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("dev");
        prepared(&dir);
        let path = dir.join(CONFIG_FILE);
        let text = fs::read_to_string(&path).expect("config");
        fs::write(&path, text.replace("dev = true\n", "")).expect("edited");
        let error = prepare(&dir, None, NOW).expect_err("not dev");
        assert!(matches!(error, DevError::NotDev { .. }), "{error}");
    }

    /// Every entry under `dir`, with its length and modification time.
    fn listing(dir: &Path) -> Vec<(PathBuf, u64, std::time::SystemTime)> {
        let mut found = Vec::new();
        let mut pending = vec![dir.to_owned()];
        while let Some(next) = pending.pop() {
            for entry in fs::read_dir(&next).expect("a directory") {
                let path = entry.expect("an entry").path();
                let metadata = fs::metadata(&path).expect("metadata");
                if metadata.is_dir() {
                    pending.push(path.clone());
                }
                found.push((path, metadata.len(), metadata.modified().expect("an mtime")));
            }
        }
        found.sort();
        found
    }

    /// A real node's directory: its own configuration, and its fleet's authority beside it.
    fn real_node_dir(root: &Path) -> PathBuf {
        let dir = root.join("node");
        let tls = dir.join(TLS_DIR);
        Authority::create(
            "home".parse().expect("a fleet"),
            Validity::new(NOW, DEFAULT_AUTHORITY_LIFETIME),
        )
        .expect("an authority")
        .write(&tls)
        .expect("written");
        // The path goes inside TOML strings, so it is escaped as one (Windows' backslashes).
        let quoted = toml_string(&dir.display().to_string());
        let escaped = &quoted[1..quoted.len() - 1];
        let config = generated()
            .replace("dev = true\n", "")
            .replace("/tmp/fleetring-dev", escaped);
        fs::write(dir.join(CONFIG_FILE), config).expect("a config");
        dir
    }

    #[test]
    fn a_real_nodes_directory_is_refused_before_anything_is_written() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = real_node_dir(root.path());
        let before = listing(&dir);
        let error = prepare(&dir, None, NOW).expect_err("not dev");
        assert!(matches!(error, DevError::NotDev { .. }), "{error}");
        assert_eq!(listing(&dir), before, "the directory changed");

        // Without its configuration, the authority's fleet gives it away.
        fs::remove_file(dir.join(CONFIG_FILE)).expect("removed");
        let before = listing(&dir);
        let error = prepare(&dir, Some(17_400), NOW).expect_err("another fleet");
        assert!(
            matches!(error, DevError::ForeignAuthority { .. }),
            "{error}"
        );
        assert_eq!(listing(&dir), before, "the directory changed");
    }

    #[test]
    fn a_directory_with_anything_but_a_partial_dev_state_is_refused() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("other");
        fs::create_dir_all(dir.join(TLS_DIR)).expect("a directory");
        fs::write(dir.join(TLS_DIR).join("node.node-1.key"), "").expect("a file");
        let before = listing(&dir);
        let error = prepare(&dir, None, NOW).expect_err("a stranger's tls");
        assert!(matches!(error, DevError::NotDevDir { .. }), "{error}");
        assert!(error.to_string().contains("node.node-1.key"), "{error}");
        assert_eq!(listing(&dir), before, "the directory changed");

        fs::remove_dir_all(dir.join(TLS_DIR)).expect("removed");
        fs::write(dir.join("notes.db"), "").expect("a file");
        let error = prepare(&dir, None, NOW).expect_err("a stranger's file");
        assert!(matches!(error, DevError::NotDevDir { .. }), "{error}");

        // What a partial dev state leaves is completed.
        fs::remove_file(dir.join("notes.db")).expect("removed");
        for sub in [DATA_DIR, CHANGES_DIR] {
            fs::create_dir_all(dir.join(sub)).expect("a directory");
        }
        fs::write(dir.join(ENV_FILE), "").expect("a file");
        let completed = prepare(&dir, Some(17_400), NOW).expect("a partial dev state");
        assert_eq!(completed.created().len(), 4);
    }

    #[test]
    fn a_partial_state_may_hold_the_log_the_start_lock_and_agents() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("dev");
        fs::create_dir_all(dir.join(AGENTS_DIR)).expect("a directory");
        for file in [LOG_FILE, LOG_FILE_OLD, START_LOCK] {
            fs::write(dir.join(file), "").expect("a file");
        }
        let completed = prepare(&dir, Some(17_400), NOW).expect("a partial dev state");
        assert_eq!(completed.created().len(), 4);
        // Anything else is still refused.
        let other = root.path().join("other");
        fs::create_dir_all(&other).expect("a directory");
        fs::write(other.join("fleetring.log.2"), "").expect("a file");
        let error = prepare(&other, Some(17_400), NOW).expect_err("a stranger's file");
        assert!(matches!(error, DevError::NotDevDir { .. }), "{error}");
    }

    #[test]
    fn a_log_rotates_only_past_its_limit() {
        let root = tempfile::tempdir().expect("a directory");
        let log = root.path().join(LOG_FILE);
        let old = root.path().join(LOG_FILE_OLD);
        rotate(&log, &old).expect("nothing to rotate");
        assert!(!old.exists());
        // Each handle is closed before the rename: Windows renames no open file.
        let sized = |length| {
            File::options()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&log)
                .and_then(|file| file.set_len(length))
                .expect("sized");
        };
        sized(LOG_ROTATE_BYTES);
        rotate(&log, &old).expect("at the limit");
        assert!(log.exists() && !old.exists());
        sized(LOG_ROTATE_BYTES + 1);
        rotate(&log, &old).expect("past the limit");
        assert!(!log.exists());
        assert_eq!(
            fs::metadata(&old).expect("rotated").len(),
            LOG_ROTATE_BYTES + 1
        );
    }

    #[test]
    fn a_failed_start_shows_the_logs_last_lines() {
        let root = tempfile::tempdir().expect("a directory");
        let log = root.path().join(LOG_FILE);
        let text: String = (1..=30).map(|line| format!("line {line}\n")).collect();
        fs::write(&log, text).expect("a log");
        let last = tail(&log);
        assert!(last.starts_with("  line 11\n"), "{last}");
        assert!(last.ends_with("  line 30"), "{last}");
        assert_eq!(last.lines().count(), TAIL_LINES);
        assert!(tail(&root.path().join("none")).contains("can't read it"));
    }

    #[test]
    fn a_directory_without_a_node_is_not_running() {
        let root = tempfile::tempdir().expect("a directory");
        let missing = status(&root.path().join("none")).expect("a status");
        assert!(!missing.running && missing.pid.is_none() && missing.log.is_none());
        let dir = root.path().join("dev");
        prepared(&dir);
        fs::write(dir.join(LOG_FILE), "").expect("a log");
        let stopped = status(&dir).expect("a status");
        assert!(!stopped.running, "{stopped:?}");
        assert_eq!(stopped.log, Some(dir.join(LOG_FILE)));
        assert_eq!(stopped.data_dir, dir);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_refuses_to_detach() {
        let root = tempfile::tempdir().expect("a directory");
        let prepared = prepared(&root.path().join("dev"));
        let error = detach(
            &prepared,
            Path::new("fleetring.exe"),
            OsStr::new("fleetring"),
            None,
            Duration::from_secs(1),
        )
        .await
        .expect_err("not on Windows");
        assert!(matches!(error, DevError::Unsupported), "{error}");
        assert!(
            error
                .to_string()
                .contains("run `fleetring dev` in its own terminal"),
            "{error}"
        );
    }

    fn generated() -> String {
        config_text(Path::new("/tmp/fleetring-dev"), DEFAULT_PORT).expect("a config")
    }

    #[test]
    fn databases_on_a_dev_node_get_one_replica_unless_their_creation_says_otherwise() {
        let config = Config::from_toml(&generated()).expect("the generated config");
        assert_eq!(
            config.replication.replication_factor,
            fleetring::ReplicationFactor::ONE
        );
    }

    #[test]
    fn dev_mode_refuses_every_address_that_isnt_loopback() {
        let base = generated();
        assert!(Config::from_toml(&base).is_ok(), "{base}");
        let cases = [
            ("listen", base.replace("127.0.0.1:7400", "0.0.0.0:7400")),
            ("listen", base.replace("127.0.0.1:7400", "192.0.2.1:7400")),
            (
                "admin.listen",
                base.replace("127.0.0.1:7401", "0.0.0.0:7401"),
            ),
            (
                "node.advertise",
                base.replace(
                    "id = \"dev\"",
                    "id = \"dev\"\nadvertise = \"192.0.2.1:7400\"",
                ),
            ),
            (
                "role",
                base.replace(
                    "role = \"both\"",
                    "role = \"data\"\n\n[control]\naddress = \"127.0.0.1:1\"",
                ),
            ),
        ];
        for (field, text) in cases {
            let error = Config::from_toml(&text).expect_err(field);
            let ConfigError::Invalid { field: named, .. } = &error else {
                panic!("{field}: {error}");
            };
            assert_eq!(*named, field, "{error}");
        }
        // The same addresses are fine outside dev mode, given TLS on the admin listener.
        let real = base
            .replace("dev = true\n", "")
            .replace("127.0.0.1:7400", "0.0.0.0:7400");
        assert!(Config::from_toml(&real).is_ok());
        let ipv6 = base.replace("127.0.0.1:7400", "[::1]:7400");
        assert!(Config::from_toml(&ipv6).is_ok());
    }

    #[test]
    fn plaintext_is_refused_in_dev_mode() {
        let text = generated().replace("dev = true\n", "dev = true\ninsecure_plaintext = true\n");
        let error = Config::from_toml(&text).expect_err("plaintext");
        assert!(error.to_string().contains("insecure_plaintext"), "{error}");
    }

    #[test]
    fn paths_and_words_are_quoted_for_toml_and_the_shell() {
        assert_eq!(toml_string(r#"C:\a "b""#), r#""C:\\a \"b\"""#);
        assert_eq!(toml_string("a\tb"), "\"a\\u0009b\"");
        assert_eq!(
            shell_quote("/home/me/.fleetring/tls"),
            "/home/me/.fleetring/tls"
        );
        assert_eq!(shell_quote("/home/o'neil/x y"), r"'/home/o'\''neil/x y'");
        assert_eq!(shell_quote(""), "''");
    }

    fn banner_with(prepared: &Prepared, context: &DevContextLine) -> String {
        prepared.banner(
            "127.0.0.1:17400".parse().expect("an address"),
            Some("127.0.0.1:17401".parse().expect("an address")),
            context,
        )
    }

    #[test]
    fn the_banner_says_dev_mode_which_context_and_what_to_try() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("dev");
        let prepared = prepared(&dir);
        let banner = banner_with(&prepared, &DevContextLine::Current);
        assert!(
            banner.starts_with("fleetring is running in dev mode"),
            "{banner}"
        );
        assert!(banner.contains("address    127.0.0.1:17400"), "{banner}");
        assert!(banner.contains("context    dev (current)\n"), "{banner}");
        assert!(
            banner.contains(
                "Try it from another terminal:\n\n  fleetring db create notes\n  fleetring status\n"
            ),
            "{banner}"
        );
        // The env file is for the case where no context was written.
        assert!(!banner.contains("env"), "{banner}");
        assert!(
            banner.contains(
                "created the authority, the node certificate, the admin certificate, and \
                 fleetringd.toml\n"
            ),
            "{banner}"
        );
    }

    #[test]
    fn the_banner_names_the_current_context_when_dev_is_not_current() {
        let root = tempfile::tempdir().expect("a directory");
        let prepared = prepared(&root.path().join("dev"));
        let banner = banner_with(
            &prepared,
            &DevContextLine::NotCurrent {
                current: "home".to_owned(),
            },
        );
        assert!(
            banner.contains("context    dev (not current: 'home' is; run fleetring switch dev)\n"),
            "{banner}"
        );
    }

    #[test]
    fn the_banner_points_at_the_env_file_only_when_the_context_was_not_written() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("dev");
        let prepared = prepared(&dir);
        let banner = banner_with(
            &prepared,
            &DevContextLine::NotWritten {
                reason: "can't read or write /ro/contexts.toml".to_owned(),
            },
        );
        assert!(
            banner.contains("context    not written (can't read or write /ro/contexts.toml)"),
            "{banner}"
        );
        assert!(
            banner.contains(&format!(
                "  . {}\n",
                shell_quote(&dir.join(ENV_FILE).display().to_string())
            )),
            "{banner}"
        );
        let kept = banner_with(&prepared, &DevContextLine::Kept);
        assert!(kept.contains("context    dev (left alone"), "{kept}");
        assert!(!kept.contains(ENV_FILE), "{kept}");
    }
}
