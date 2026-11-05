//! Dev mode (ADR-0039): one node on loopback, with a local authority and an admin principal that
//! `fleetringd dev` creates once and reuses.
//!
//! [`prepare`] creates whatever the directory lacks and loads the configuration it holds; the
//! binary then runs that configuration as it runs any other. With `dev = true`, validation keeps
//! every address on loopback, however the node is started.

use std::fmt::{self, Write as _};
use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fleetring_control::Timestamp;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject as _;
use serde::Deserialize;
use x509_parser::prelude::{FromDer as _, X509Certificate};

use crate::config::{AdminListener, Config, ConfigError, NodeRole, Plaintext};
use crate::tls::{Authority, AuthorityError, DEFAULT_AUTHORITY_LIFETIME, Validity};

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
const DATA_DIR: &str = "data";

/// Where the dev sink writes each database's changes, in the dev directory.
const CHANGES_DIR: &str = "changes";

/// The dev directory's name, in the home directory.
const DIR_NAME: &str = ".fleetring";

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
         `fleetringd dev` won't touch it; pass an empty or new --data-dir"
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
        "{path} isn't a dev mode configuration: it lacks `dev = true`, so `fleetringd dev` won't \
         run it; run it with `fleetringd --config`, or pass another --data-dir"
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

    /// The banner `fleetringd dev` prints once the node serves on `listen`, with its admin
    /// listener on `admin`: that it is dev mode, where it is, and how to point the CLI at it.
    #[must_use]
    pub fn banner(&self, listen: SocketAddr, admin: Option<SocketAddr>) -> String {
        let mut text = String::from(
            "fleetringd is running in dev mode: one node on loopback with a local authority, \
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
        let _ = write!(
            text,
            "\nPoint the CLI at it from another terminal:\n\n  . {}\n  fleetringd status\n\n\
             Ctrl-C stops it; the next start reuses everything in {}.\n",
            shell_quote(&self.env_file().display().to_string()),
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
    let variable = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
    match std::env::var_os(variable) {
        Some(home) if !home.is_empty() => Ok(PathBuf::from(home).join(DIR_NAME)),
        _ => Err(DevError::NoHome { variable }),
    }
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
/// `data/`, `changes/`, and `env` (what a partial dev state leaves), one whose `tls/` holds a
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
const PARTIAL_ENTRIES: [&str; 4] = [TLS_DIR, DATA_DIR, CHANGES_DIR, ENV_FILE];

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
        "# fleetringd dev mode (ADR-0039): one node on loopback with a local authority, for\n\
         # development only. `fleetringd dev` wrote this file and never rewrites it. Edit it if you\n\
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
        "# Points the fleetringd commands at the dev mode node (ADR-0039). Load it with `. <this file>`.\n\
         # `fleetringd dev` rewrites it from fleetringd.toml at every start.\n\
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

    fn generated() -> String {
        config_text(Path::new("/tmp/fleetring-dev"), DEFAULT_PORT).expect("a config")
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

    #[test]
    fn the_banner_says_dev_mode_and_how_to_load_the_env_file() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = root.path().join("dev");
        let prepared = prepared(&dir);
        let banner = prepared.banner(
            "127.0.0.1:17400".parse().expect("an address"),
            Some("127.0.0.1:17401".parse().expect("an address")),
        );
        assert!(
            banner.starts_with("fleetringd is running in dev mode"),
            "{banner}"
        );
        assert!(banner.contains("address    127.0.0.1:17400"), "{banner}");
        assert!(
            banner.contains(&format!(
                ". {}",
                shell_quote(&dir.join(ENV_FILE).display().to_string())
            )),
            "{banner}"
        );
        assert!(
            banner.contains(
                "created the authority, the node certificate, the admin certificate, and \
                 fleetringd.toml\n"
            ),
            "{banner}"
        );
    }
}
