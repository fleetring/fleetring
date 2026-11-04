//! A running daemon as `upgrade` finds it: the process file it writes, and the signal that
//! restarts it in place (ADR-0038, point 8).

use std::fs::{self, File, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::upgrade::UpgradeError;

/// The file a daemon writes in its data directory while it runs.
pub const PROCESS_FILE: &str = "fleetringd.process";

/// The lock file [`DataDir`](crate::DataDir) holds while a daemon runs.
const LOCK_FILE: &str = "fleetringd.lock";

/// What a running daemon says about itself in `<data_dir>/fleetringd.process`: enough for
/// `fleetringd upgrade` to restart it in place.
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
}

/// The environment variable a restart in place passes to the binary it executes: how many times
/// the process has restarted, for [`ProcessFile::restarts`].
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
        }
    }

    /// Writes the file into `data_dir`, replacing any earlier one.
    ///
    /// # Errors
    ///
    /// If the file can't be written.
    pub fn write(&self, data_dir: &Path) -> io::Result<()> {
        let path = data_dir.join(PROCESS_FILE);
        let staged = data_dir.join(format!(".{PROCESS_FILE}"));
        fs::write(&staged, serde_json::to_vec(self).map_err(io::Error::other)?)?;
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
    let status = std::process::Command::new("kill")
        .arg("-HUP")
        .arg(pid.to_string())
        .status()
        .map_err(|error| UpgradeError::Restart {
            reason: format!("can't run kill: {error}"),
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(UpgradeError::Restart {
            reason: format!(
                "kill -HUP {pid} failed ({status}); is the daemon running as another user?"
            ),
        })
    }
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
