//! A running daemon as `upgrade` finds it: the process file it writes, and the signal that
//! restarts it in place (ADR-0038, point 8).

use std::fs::{self, File, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::data_dir::LOCK_FILE;
use crate::upgrade::UpgradeError;

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
