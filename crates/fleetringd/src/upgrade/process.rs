//! A running daemon as `upgrade` finds it: the process file it writes, and the signal that
//! restarts it in place (ADR-0038, point 8).

use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::data_dir::LOCK_FILE;
use crate::upgrade::UpgradeError;

/// 0.6's top-level daemon flags, which mean `serve` until 1.0 when they come first, after any
/// `--log-format` (ADR-0043, point 1.4).
pub const LEGACY_DAEMON_FLAGS: [&str; 8] = [
    "--config",
    "--node-id",
    "--data-dir",
    "--listen",
    "--advertise",
    "--control",
    "--role",
    "--insecure-plaintext",
];

/// Whether `arg` is one of [`LEGACY_DAEMON_FLAGS`], alone or as `--flag=value`.
#[must_use]
pub fn is_legacy_daemon_flag(arg: &str) -> bool {
    let flag = arg.split_once('=').map_or(arg, |(flag, _)| flag);
    LEGACY_DAEMON_FLAGS.contains(&flag)
}

/// The file a daemon writes in its data directory while it runs.
///
/// Its name is kept until 1.0, as are [`RESTARTS_VAR`] and the data directory's lock file: a 0.6
/// upgrader waits on all three when it restarts a node onto a later release, and a 0.6 daemon
/// passes the variable to the binary it executes (ADR-0043, point 1.7).
pub const PROCESS_FILE: &str = "fleetringd.process";

/// What a running daemon says about itself in `<data_dir>/fleetringd.process`: enough for
/// `fleetring upgrade` to restart it in place.
///
/// A 0.6 upgrader reads it too, so fields are only ever added, each optional, and unknown ones are
/// ignored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessFile {
    /// The daemon's process ID.
    pub pid: u32,
    /// The binary it runs, and executes again when it restarts.
    pub exe: PathBuf,
    /// Its release.
    pub version: String,
    /// How many times the process has restarted in place, so that `upgrade` can tell the
    /// restarted daemon from the one it signalled: they share a process ID.
    #[serde(default)]
    pub restarts: u32,
    /// The arguments it was started with, after the program, so that a rollback can tell a node
    /// started with `serve`, which a release before 0.7 can't run. Empty in a file a release
    /// before 0.7 wrote.
    #[serde(default)]
    pub args: Vec<String>,
    /// The supervisor that started it, if one did: `stop` refuses such a node, since the
    /// supervisor would start it again. Absent in a file a release before 0.7 wrote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervisor: Option<Supervisor>,
    /// Set when `fleetring dev -d` started it: `stop` and `dev stop` always stop such a node,
    /// whatever [`supervisor`](Self::supervisor) says (ADR-0043, point 5.5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_by: Option<StartedBy>,
    /// The directory it was started in, which relative paths in [`args`](Self::args) are
    /// relative to: `upgrade` checks the node's command line with a new release there. Absent in
    /// a file a release before 0.8 wrote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
}

/// A service manager that runs a node and restarts it, so that only it should stop the node
/// (ADR-0043, point 5.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Supervisor {
    /// systemd, the system manager or a user's.
    Systemd,
    /// launchd, through a Homebrew service.
    Launchd,
}

impl Supervisor {
    /// The supervisor that started this process, judged from its environment `env` and its
    /// parent `parent`.
    ///
    /// `Systemd` if `INVOCATION_ID` is set and not empty *and* the parent is PID 1 or its `comm`
    /// is `systemd` (the user manager); `Launchd` if `XPC_SERVICE_NAME` starts with
    /// `homebrew.mxcl.` *and* the parent is PID 1. Variables alone are never enough: shells
    /// under gnome-terminal or VS Code inherit `INVOCATION_ID`.
    #[must_use]
    pub fn detect(env: &dyn Fn(&str) -> Option<OsString>, parent: &ParentProcess) -> Option<Self> {
        let parent_is_init = parent.pid == Some(1);
        let invocation = env("INVOCATION_ID").is_some_and(|value| !value.is_empty());
        if invocation && (parent_is_init || parent.comm.as_deref() == Some("systemd")) {
            return Some(Self::Systemd);
        }
        let homebrew = env("XPC_SERVICE_NAME")
            .is_some_and(|name| name.to_string_lossy().starts_with("homebrew.mxcl."));
        (homebrew && parent_is_init).then_some(Self::Launchd)
    }

    /// The command that stops a node this supervisor runs, for a refusal's hint.
    #[must_use]
    pub fn stop_hint(self) -> &'static str {
        match self {
            Self::Systemd => "systemctl stop <unit>",
            Self::Launchd => "brew services stop fleetring",
        }
    }
}

impl fmt::Display for Supervisor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Systemd => "systemd",
            Self::Launchd => "launchd",
        })
    }
}

/// Who started a node, when it wasn't a person or a supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StartedBy {
    /// `fleetring dev -d`, which started it in the background.
    Detach,
}

/// A process's parent, as it was when the process started: enough to tell a supervisor from a
/// shell that inherited its variables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentProcess {
    /// The parent's process ID, if there is one to see.
    pub pid: Option<u32>,
    /// The parent's command name, from `/proc/<pid>/comm`; Linux only.
    pub comm: Option<String>,
}

impl ParentProcess {
    /// This process's parent.
    #[must_use]
    pub fn current() -> Self {
        #[cfg(unix)]
        let pid = rustix::process::getppid()
            .and_then(|pid| u32::try_from(pid.as_raw_nonzero().get()).ok());
        #[cfg(not(unix))]
        let pid = None;
        #[cfg(target_os = "linux")]
        let comm = pid.and_then(|pid| {
            fs::read_to_string(format!("/proc/{pid}/comm"))
                .ok()
                .map(|comm| comm.trim_end().to_owned())
        });
        #[cfg(not(target_os = "linux"))]
        let comm = None;
        Self { pid, comm }
    }
}

/// Whether a daemon runs from a data directory, as its process file and lock say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Running {
    /// There is no process file.
    No,
    /// A process file names a process that no longer holds the directory's lock.
    Stale {
        /// The process ID the file names.
        pid: u32,
    },
    /// A process file names a process, and the directory's lock is held.
    Yes(ProcessFile),
}

/// Whether a daemon runs from `data_dir`: it does if its process file exists and the directory's
/// lock is held. The lock is tried shared and released at once, so this never keeps a daemon
/// from starting for longer than the try.
///
/// # Errors
///
/// If the process file can't be read or parsed, or the lock file can't be opened or tried.
pub fn running(data_dir: &Path) -> io::Result<Running> {
    let Some(file) = ProcessFile::read(data_dir)? else {
        return Ok(Running::No);
    };
    let lock = match File::open(data_dir.join(LOCK_FILE)) {
        Ok(lock) => lock,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(Running::Stale { pid: file.pid });
        }
        Err(error) => return Err(error),
    };
    match lock.try_lock_shared() {
        Ok(()) => {
            let _unlocked = lock.unlock();
            Ok(Running::Stale { pid: file.pid })
        }
        Err(TryLockError::WouldBlock) => Ok(Running::Yes(file)),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

/// The environment variable a restart in place passes to the binary it executes: how many times
/// the process has restarted, for [`ProcessFile::restarts`]. Its name is kept until 1.0, as
/// [`PROCESS_FILE`] says.
pub const RESTARTS_VAR: &str = "FLEETRINGD_RESTARTS";

impl ProcessFile {
    /// Describes this process, running the binary at `exe`, with the restart count
    /// [`RESTARTS_VAR`] carries.
    #[must_use]
    pub fn this_process(exe: PathBuf) -> Self {
        Self {
            pid: std::process::id(),
            exe,
            version: crate::software::VERSION.to_owned(),
            restarts: std::env::var(RESTARTS_VAR)
                .ok()
                .and_then(|restarts| restarts.parse().ok())
                .unwrap_or(0),
            // The daemon's command line, as `ps` shows it. No daemon flag may carry a secret:
            // this file is only as private as `write` makes it.
            args: std::env::args_os()
                .skip(1)
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect(),
            supervisor: Supervisor::detect(
                &|name| std::env::var_os(name),
                &ParentProcess::current(),
            ),
            started_by: crate::dev::started_detached().then_some(StartedBy::Detach),
            cwd: std::env::current_dir().ok(),
        }
    }

    /// Whether the daemon was started with `serve`, after any leading `--log-format` and its
    /// value: a release before 0.7 can't run that command line.
    #[must_use]
    pub fn started_with_serve(&self) -> bool {
        let mut args = self.args.iter().map(String::as_str);
        loop {
            match args.next() {
                Some("--log-format") => {
                    args.next();
                }
                Some(arg) if arg.starts_with("--log-format=") => {}
                first => return first == Some("serve"),
            }
        }
    }

    /// The daemon's arguments if they run a node with `serve`, after any global flags, or with
    /// 0.6's bare daemon flags, after any `--log-format`, as the binary reads them. `None` for a
    /// dev node, for any other command, for a file a release before 0.7 wrote, which records no
    /// arguments, and for arguments that ask for help or the version, or hold a short flag but
    /// `-q`, which a daemon's command line never does. An upgrade runs another binary with these,
    /// perhaps as another user than the one that wrote the file, so it trusts only a command line
    /// that can do nothing but check a node's configuration.
    #[must_use]
    pub fn serve_args(&self) -> Option<&[String]> {
        let suspect = |arg: &String| {
            matches!(arg.as_str(), "--help" | "--version" | "--")
                || (arg.starts_with('-') && !arg.starts_with("--") && arg != "-q")
        };
        if self.args.iter().any(suspect) {
            return None;
        }
        let mut args = self.args.iter().map(String::as_str);
        // 0.6's bare daemon flags mean `serve` only after `--log-format` alone.
        let mut log_format_only = true;
        loop {
            match args.next()? {
                "--log-format" => {
                    args.next();
                }
                "--color" => {
                    args.next();
                    log_format_only = false;
                }
                "-q" | "--quiet" => log_format_only = false,
                arg if arg.starts_with("--log-format=") => {}
                arg if arg.starts_with("--color=") => log_format_only = false,
                "serve" => return Some(&self.args),
                arg if log_format_only && is_legacy_daemon_flag(arg) => return Some(&self.args),
                _ => return None,
            }
        }
    }

    /// Writes the file into `data_dir`, replacing any earlier one.
    ///
    /// On Unix only its owner may read it (mode 0600), whatever the umask: it holds the daemon's
    /// arguments. Whoever acts on it, an upgrade's `SIGHUP` or `stop`'s `SIGTERM`, must be the
    /// daemon's user or root to signal it anyway, as 0.6's upgrader had to be.
    ///
    /// # Errors
    ///
    /// If the file can't be written.
    pub fn write(&self, data_dir: &Path) -> io::Result<()> {
        let path = data_dir.join(PROCESS_FILE);
        let staged = data_dir.join(format!(".{PROCESS_FILE}"));
        // A file left by an earlier writer keeps its mode when truncated, so start afresh.
        match fs::remove_file(&staged) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => {}
        }
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut file = options.open(&staged)?;
        io::Write::write_all(
            &mut file,
            &serde_json::to_vec(self).map_err(io::Error::other)?,
        )?;
        drop(file);
        fs::rename(staged, path)
    }

    /// Removes the file from `data_dir`, if this process wrote it.
    pub fn remove(&self, data_dir: &Path) {
        if Self::read(data_dir).ok().flatten().as_ref() == Some(self) {
            let _gone = fs::remove_file(data_dir.join(PROCESS_FILE));
        }
    }

    /// Reads the file from `data_dir`, or `None` if there is none.
    ///
    /// # Errors
    ///
    /// If it exists but can't be read or parsed.
    pub fn read(data_dir: &Path) -> io::Result<Option<Self>> {
        match fs::read(data_dir.join(PROCESS_FILE)) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(io::Error::other),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// Whether a daemon holds `data_dir`'s lock now.
pub(crate) fn is_running(data_dir: &Path) -> Result<bool, UpgradeError> {
    let path = data_dir.join(LOCK_FILE);
    let lock = match File::options().write(true).open(&path) {
        Ok(lock) => lock,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => return Err(UpgradeError::Io { path, source }),
    };
    match lock.try_lock() {
        Ok(()) => {
            let _unlocked = lock.unlock();
            Ok(false)
        }
        Err(TryLockError::WouldBlock) => Ok(true),
        Err(TryLockError::Error(source)) => Err(UpgradeError::Io { path, source }),
    }
}

/// Asks the daemon `pid` to restart in place, with `SIGHUP`.
#[cfg(unix)]
pub(crate) fn restart(pid: u32) -> Result<(), UpgradeError> {
    use rustix::process::{Pid, Signal, kill_process};
    let target = i32::try_from(pid)
        .ok()
        .and_then(Pid::from_raw)
        .ok_or_else(|| UpgradeError::Restart {
            reason: format!("the process file names pid {pid}, which isn't a process"),
        })?;
    kill_process(target, Signal::HUP).map_err(|error| UpgradeError::Restart {
        reason: format!(
            "can't send SIGHUP to pid {pid}: {error}; is the daemon running as another user?"
        ),
    })
}

/// Restarting in place needs `SIGHUP`, which only Unix has.
#[cfg(not(unix))]
pub(crate) fn restart(_pid: u32) -> Result<(), UpgradeError> {
    Err(UpgradeError::Unsupported {
        reason: "restarting in place needs SIGHUP; restart the service instead",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_process_file_round_trips_and_only_its_writer_removes_it() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        assert_eq!(ProcessFile::read(dir.path())?, None);
        let written = ProcessFile::this_process(PathBuf::from("/usr/local/bin/fleetringd"));
        written.write(dir.path())?;
        assert_eq!(ProcessFile::read(dir.path())?, Some(written.clone()));
        let other = ProcessFile {
            pid: written.pid.wrapping_add(1),
            ..written.clone()
        };
        other.remove(dir.path());
        assert!(ProcessFile::read(dir.path())?.is_some());
        written.remove(dir.path());
        assert_eq!(ProcessFile::read(dir.path())?, None);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn only_its_owner_reads_the_process_file() -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir()?;
        // A world-readable file left where the staged copy goes doesn't lend it its mode.
        let staged = dir.path().join(format!(".{PROCESS_FILE}"));
        fs::write(&staged, b"{}")?;
        fs::set_permissions(&staged, fs::Permissions::from_mode(0o644))?;
        let written = ProcessFile::this_process(PathBuf::from("/usr/local/bin/fleetring"));
        written.write(dir.path())?;
        let mode = fs::metadata(dir.path().join(PROCESS_FILE))?
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "{mode:o}");
        assert_eq!(ProcessFile::read(dir.path())?, Some(written));
        Ok(())
    }

    #[test]
    fn the_names_a_0_6_upgrader_uses_are_kept() {
        assert_eq!(PROCESS_FILE, "fleetringd.process");
        assert_eq!(LOCK_FILE, "fleetringd.lock");
        assert_eq!(RESTARTS_VAR, "FLEETRINGD_RESTARTS");
    }

    #[test]
    fn a_0_6_process_file_reads_and_a_0_6_reader_skips_the_arguments() -> io::Result<()> {
        // What 0.6.0 writes, and what it reads: no `args`, and unknown fields ignored.
        let old: ProcessFile = serde_json::from_str(
            r#"{"pid":7,"exe":"/usr/local/bin/fleetringd","version":"0.6.0","restarts":1}"#,
        )?;
        assert!(old.args.is_empty());
        assert!(!old.started_with_serve());
        #[derive(Deserialize)]
        #[expect(dead_code, reason = "the fields 0.6.0 reads, which must still parse")]
        struct ReadBy06 {
            pid: u32,
            exe: PathBuf,
            version: String,
            #[serde(default)]
            restarts: u32,
        }
        let written = serde_json::to_vec(&ProcessFile {
            args: vec!["serve".into(), "--config".into(), "x".into()],
            ..old
        })?;
        let read: ReadBy06 = serde_json::from_slice(&written)?;
        assert_eq!(read.restarts, 1);
        Ok(())
    }

    #[test]
    fn serve_is_found_after_the_log_format() {
        let file = |args: &[&str]| ProcessFile {
            pid: 1,
            exe: PathBuf::from("fleetring"),
            version: "0.7.0".into(),
            restarts: 0,
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            supervisor: None,
            started_by: None,
            cwd: None,
        };
        for args in [
            &["serve", "--config", "x"][..],
            &["--log-format", "json", "serve", "--config", "x"],
            &["--log-format=json", "serve"],
        ] {
            assert!(file(args).started_with_serve(), "{args:?}");
        }
        for args in [
            &["--config", "x"][..],
            &["--log-format", "json", "--config", "x"],
            &["dev"],
            &["--log-format", "serve"],
            &[],
        ] {
            assert!(!file(args).started_with_serve(), "{args:?}");
        }
        // A node run from a configuration, either way, has arguments to check; a dev node and a
        // file that records none don't.
        for args in [
            &["serve", "--config", "x"][..],
            &["--config", "x"],
            &["--log-format", "json", "--config=x"],
            &["--color", "never", "-q", "serve", "--config", "x"],
        ] {
            assert_eq!(
                file(args).serve_args(),
                Some(&file(args).args[..]),
                "{args:?}"
            );
        }
        for args in [
            &["dev"][..],
            &["--log-format=json", "dev", "--data-dir", "d"],
            &[],
            &["--log-format", "json"],
            // Anything that isn't a node's start: another command, help, the version, a short
            // flag but -q, and 0.6's flags after another global flag.
            &["upgrade", "--software"],
            &["db", "list"],
            &["serve", "--config", "x", "--help"],
            &["serve", "--config", "x", "-h"],
            &["serve", "--config", "x", "-qh"],
            &["--config", "x", "--version"],
            &["--config", "x", "-V"],
            &["serve", "--", "--config", "x"],
            &["--color", "never", "--config", "x"],
            &["-q", "--config", "x"],
        ] {
            assert_eq!(file(args).serve_args(), None, "{args:?}");
        }
    }

    #[test]
    fn the_directory_is_recorded_and_a_0_7_file_has_none() -> io::Result<()> {
        let written = ProcessFile::this_process(PathBuf::from("fleetring"));
        assert_eq!(written.cwd, std::env::current_dir().ok());
        let old: ProcessFile = serde_json::from_str(
            r#"{"pid":7,"exe":"fleetring","version":"0.7.0","restarts":0,"args":["serve"]}"#,
        )?;
        assert_eq!(old.cwd, None);
        Ok(())
    }

    /// An environment holding `vars`, for [`Supervisor::detect`].
    fn env_of(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> {
        let vars: Vec<(String, String)> = vars
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        move |name| {
            vars.iter()
                .find(|(set, _)| set == name)
                .map(|(_, value)| OsString::from(value))
        }
    }

    fn parent(pid: u32, comm: &str) -> ParentProcess {
        ParentProcess {
            pid: Some(pid),
            comm: Some(comm.to_owned()),
        }
    }

    #[test]
    fn an_inherited_invocation_id_with_a_shell_parent_is_not_systemd() {
        let env = env_of(&[("INVOCATION_ID", "0123abcd"), ("JOURNAL_STREAM", "8:42")]);
        assert_eq!(Supervisor::detect(&env, &parent(4242, "bash")), None);
        assert_eq!(
            Supervisor::detect(
                &env,
                &ParentProcess {
                    pid: None,
                    comm: None
                }
            ),
            None
        );
        // Empty is unset.
        let empty = env_of(&[("INVOCATION_ID", "")]);
        assert_eq!(Supervisor::detect(&empty, &parent(1, "systemd")), None);
    }

    #[test]
    fn invocation_id_with_systemd_as_parent_is_systemd() {
        let env = env_of(&[("INVOCATION_ID", "0123abcd")]);
        assert_eq!(
            Supervisor::detect(&env, &parent(1, "systemd")),
            Some(Supervisor::Systemd)
        );
        // A user manager is a `systemd` process that isn't PID 1.
        assert_eq!(
            Supervisor::detect(&env, &parent(1717, "systemd")),
            Some(Supervisor::Systemd)
        );
        // Without the variable, PID 1 as the parent says nothing.
        assert_eq!(
            Supervisor::detect(&env_of(&[]), &parent(1, "systemd")),
            None
        );
        assert_eq!(Supervisor::Systemd.stop_hint(), "systemctl stop <unit>");
    }

    #[test]
    fn xpc_service_name_needs_launchd_as_parent() {
        let env = env_of(&[("XPC_SERVICE_NAME", "homebrew.mxcl.fleetring")]);
        assert_eq!(
            Supervisor::detect(&env, &parent(1, "launchd")),
            Some(Supervisor::Launchd)
        );
        assert_eq!(Supervisor::detect(&env, &parent(4242, "zsh")), None);
        // Terminal.app and other apps set their own service name.
        let terminal = env_of(&[("XPC_SERVICE_NAME", "application.com.apple.Terminal.1")]);
        assert_eq!(Supervisor::detect(&terminal, &parent(1, "launchd")), None);
        assert_eq!(
            Supervisor::Launchd.stop_hint(),
            "brew services stop fleetring"
        );
    }

    #[test]
    fn the_supervisor_and_starter_are_written_in_lowercase_and_only_when_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let plain = ProcessFile {
            supervisor: None,
            started_by: None,
            ..ProcessFile::this_process(PathBuf::from("fleetring"))
        };
        let text = serde_json::to_string(&plain)?;
        assert!(
            !text.contains("supervisor") && !text.contains("started_by"),
            "{text}"
        );
        let marked = ProcessFile {
            supervisor: Some(Supervisor::Systemd),
            started_by: Some(StartedBy::Detach),
            ..plain
        };
        let text = serde_json::to_string(&marked)?;
        assert!(text.contains(r#""supervisor":"systemd""#), "{text}");
        assert!(text.contains(r#""started_by":"detach""#), "{text}");
        assert_eq!(serde_json::from_str::<ProcessFile>(&text)?, marked);
        Ok(())
    }

    #[test]
    fn running_tells_a_live_daemon_from_a_stale_file() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        assert_eq!(running(dir.path())?, Running::No);
        let file = ProcessFile::this_process(PathBuf::from("fleetring"));
        file.write(dir.path())?;
        // No lock file at all: nobody holds it.
        assert_eq!(running(dir.path())?, Running::Stale { pid: file.pid });
        let locked = crate::DataDir::lock(dir.path())?;
        assert_eq!(running(dir.path())?, Running::Yes(file.clone()));
        drop(locked);
        assert_eq!(running(dir.path())?, Running::Stale { pid: file.pid });
        // Trying the lock doesn't keep it.
        let _relocked = crate::DataDir::lock(dir.path())?;
        Ok(())
    }

    #[test]
    fn a_locked_data_directory_has_a_running_daemon() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        assert!(!is_running(dir.path())?);
        let locked = crate::DataDir::lock(dir.path())?;
        assert!(is_running(dir.path())?);
        drop(locked);
        assert!(!is_running(dir.path())?);
        Ok(())
    }
}
