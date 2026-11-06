//! `fleetring stop`: stops a node started in a terminal or a script, through its process file
//! (ADR-0043, point 5.4). `dev stop` is the same for the dev node.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Args;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use fleetringd::upgrade::{PROCESS_FILE, ProcessFile, Running, StartedBy, Supervisor, running};
use fleetringd::{Config, ConfigError};

use crate::progress::{MachineOutput, Progress};
use crate::{parse_ttl, spinner_surroundings};

/// How long `stop` waits for the node to drain unless `--timeout` says otherwise.
pub(crate) const STOP_TIMEOUT: Duration = Duration::from_secs(2 * 60);

/// How often `stop` checks whether the node has released its data directory.
const POLL: Duration = Duration::from_millis(100);

/// The configuration file a directory may hold, whose `data_dir` names the node's.
const CONFIG_FILE: &str = "fleetringd.toml";

/// `fleetring stop`'s flags.
#[derive(Debug, Args)]
#[command(group(
    clap::ArgGroup::new("node")
        .args(["data_dir", "config"])
        .required(true)
))]
pub(crate) struct StopArgs {
    /// The node's data directory, or a directory holding a `fleetringd.toml`, whose `data_dir`
    /// is then used: a dev directory and its `data/` both work.
    #[arg(long, value_name = "DIR")]
    data_dir: Option<PathBuf>,
    /// The node's configuration file, whose `data_dir` is used.
    #[arg(long, value_name = "FILE")]
    config: Option<PathBuf>,
    /// How long to wait for the node to drain and exit: `<n>m` or `<n>s`; 2m by default. It
    /// keeps draining after that.
    #[arg(long, value_parser = parse_ttl)]
    timeout: Option<Duration>,
}

/// Which node to stop, as the command line names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StopTarget {
    /// `--data-dir`: a data directory, or a directory with a configuration file.
    DataDir(PathBuf),
    /// `--config`: a configuration file.
    Config(PathBuf),
}

/// What `stop` stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Stopped {
    /// The process it stopped.
    pub(crate) pid: u32,
}

/// The error returned when a node can't be stopped.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum StopError {
    /// The data directory has no process file.
    #[error("no node is running from {data_dir}: it has no {PROCESS_FILE}")]
    NotRunning {
        /// The data directory.
        data_dir: PathBuf,
    },
    /// The process file names a process that doesn't hold the data directory's lock.
    #[error("the node that wrote {path} (pid {pid}) isn't running")]
    Stale {
        /// The process file.
        path: PathBuf,
        /// The process it names.
        pid: u32,
    },
    /// A supervisor runs the node, and would start it again.
    #[error("{supervisor} runs this node; stop it with `{hint}`")]
    Supervised {
        /// The supervisor.
        supervisor: Supervisor,
        /// The command that stops it.
        hint: &'static str,
    },
    /// The node hasn't released its data directory in time.
    #[error("the node (pid {pid}) is still draining after {timeout:?}; it keeps going")]
    Timeout {
        /// The node's process.
        pid: u32,
        /// How long `stop` waited.
        timeout: Duration,
    },
    /// The signal couldn't be sent.
    #[cfg(unix)]
    #[error("can't send SIGTERM to pid {pid}; is the node running as another user?")]
    Signal {
        /// The node's process.
        pid: u32,
        /// The underlying error.
        source: io::Error,
    },
    /// Stopping a node needs `SIGTERM`, which only Unix has.
    #[error("stopping a node isn't available on Windows yet; press Ctrl-C in its terminal")]
    Unsupported,
    /// A file can't be read.
    #[error("can't read {path}")]
    Io {
        /// The file.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// The configuration file can't be loaded.
    #[error("can't read the node's data directory from {path}")]
    Config {
        /// The file.
        path: PathBuf,
        /// What is wrong with it.
        source: ConfigError,
    },
}

/// Runs `fleetring stop` and returns what it prints.
pub(crate) async fn run(args: StopArgs) -> Result<String, StopError> {
    let target = match (args.data_dir, args.config) {
        (_, Some(config)) => StopTarget::Config(config),
        (Some(dir), None) => StopTarget::DataDir(dir),
        // clap's group requires one.
        (None, None) => StopTarget::DataDir(PathBuf::from(".")),
    };
    let data_dir = data_dir_of(&target)?;
    let progress = Progress::start(
        "stopping the node\u{2026}",
        &spinner_surroundings(MachineOutput::No),
    );
    let stopped = stop(&data_dir, args.timeout.unwrap_or(STOP_TIMEOUT), &progress).await?;
    progress.finish_ok("node stopped");
    Ok(format!("stopped the node (pid {})\n", stopped.pid))
}

/// Finds the data directory: `--config`'s `data_dir`; or `--data-dir` itself if it holds a
/// process file; else its `fleetringd.toml`'s `data_dir`; else itself.
///
/// # Errors
///
/// [`StopError::Config`] if the configuration file can't be loaded, and [`StopError::Io`] if the
/// directory can't be checked.
pub(crate) fn data_dir_of(target: &StopTarget) -> Result<PathBuf, StopError> {
    let config = match target {
        StopTarget::Config(path) => path.clone(),
        StopTarget::DataDir(dir) => {
            let exists = |path: PathBuf| {
                path.try_exists()
                    .map_err(|source| StopError::Io { path, source })
            };
            if exists(dir.join(PROCESS_FILE))? {
                return Ok(dir.clone());
            }
            let config = dir.join(CONFIG_FILE);
            if !exists(config.clone())? {
                return Ok(dir.clone());
            }
            config
        }
    };
    Config::load(&config)
        .map(|loaded| loaded.data_dir)
        .map_err(|source| StopError::Config {
            path: config,
            source,
        })
}

/// Sends SIGTERM to the node running from `data_dir`, which drains it and leaves the fleet as
/// `[membership] on_shutdown` says, and waits until it releases the directory's lock, polling
/// every 100 ms, for `timeout`. It never sends SIGKILL.
///
/// A node a supervisor started is refused, unless `dev -d` started it.
///
/// # Errors
///
/// [`StopError`] if no node runs from the directory, a supervisor runs it, the signal can't be
/// sent, it is still draining after `timeout`, or on Windows.
pub(crate) async fn stop(
    data_dir: &Path,
    timeout: Duration,
    progress: &Progress,
) -> Result<Stopped, StopError> {
    if cfg!(not(unix)) {
        return Err(StopError::Unsupported);
    }
    let file = target(data_dir)?;
    // Read again just before signalling, so that the signal goes to the process the file names
    // now.
    let now = ProcessFile::read(data_dir).map_err(|source| process_error(data_dir, source))?;
    let pid = match now {
        Some(now) if now.pid == file.pid => now.pid,
        _ => {
            return Err(StopError::Stale {
                path: data_dir.join(PROCESS_FILE),
                pid: file.pid,
            });
        }
    };
    terminate(data_dir, pid)?;
    progress.set_message(format!("draining the node (pid {pid})\u{2026}"));
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let state = running(data_dir).map_err(|source| process_error(data_dir, source))?;
        if !matches!(state, Running::Yes(_)) {
            return Ok(Stopped { pid });
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(StopError::Timeout { pid, timeout });
        }
        tokio::time::sleep(POLL).await;
    }
}

/// The node running from `data_dir`, if `stop` may stop it.
fn target(data_dir: &Path) -> Result<ProcessFile, StopError> {
    let file = match running(data_dir).map_err(|source| process_error(data_dir, source))? {
        Running::No => {
            return Err(StopError::NotRunning {
                data_dir: data_dir.to_owned(),
            });
        }
        Running::Stale { pid } => {
            return Err(StopError::Stale {
                path: data_dir.join(PROCESS_FILE),
                pid,
            });
        }
        Running::Yes(file) => file,
    };
    match (file.started_by, file.supervisor) {
        (Some(StartedBy::Detach), _) | (None, None) => Ok(file),
        (None, Some(supervisor)) => Err(StopError::Supervised {
            supervisor,
            hint: supervisor.stop_hint(),
        }),
    }
}

fn process_error(data_dir: &Path, source: io::Error) -> StopError {
    StopError::Io {
        path: data_dir.join(PROCESS_FILE),
        source,
    }
}

/// Sends SIGTERM to `pid`, the node running from `data_dir`.
#[cfg(unix)]
fn terminate(data_dir: &Path, pid: u32) -> Result<(), StopError> {
    use rustix::io::Errno;
    use rustix::process::{Pid, Signal, kill_process};
    let stale = || StopError::Stale {
        path: data_dir.join(PROCESS_FILE),
        pid,
    };
    let target = i32::try_from(pid)
        .ok()
        .and_then(Pid::from_raw)
        .ok_or_else(stale)?;
    match kill_process(target, Signal::TERM) {
        Ok(()) => Ok(()),
        Err(Errno::SRCH) => Err(stale()),
        Err(error) => Err(StopError::Signal {
            pid,
            source: error.into(),
        }),
    }
}

/// Stopping a node needs `SIGTERM`, which only Unix has.
#[cfg(not(unix))]
fn terminate(_: &Path, _: u32) -> Result<(), StopError> {
    Err(StopError::Unsupported)
}

/// The `hint:` line for `stop` without `--data-dir` or `--config`, or `None` for any other
/// error: a default would stop the wrong node, so it names the dev node's command.
pub(crate) fn target_hint(error: &clap::Error) -> Option<&'static str> {
    if error.kind() != ErrorKind::MissingRequiredArgument {
        return None;
    }
    let Some(ContextValue::StyledStr(usage)) = error.get(ContextKind::Usage) else {
        return None;
    };
    let usage = usage.to_string();
    (usage.contains(" stop ") && !usage.contains(" dev stop"))
        .then_some("hint: to stop the dev node, run `fleetring dev stop`")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dev_dir(root: &Path) -> PathBuf {
        let dir = root.join("dev");
        fleetringd::dev::prepare(
            &dir,
            Some(17_400),
            fleetring_control::Timestamp::from_millis(1_790_000_000_000),
        )
        .expect("a dev directory");
        dir
    }

    #[test]
    fn the_data_directory_comes_from_the_config_or_the_directory() {
        let root = tempfile::tempdir().expect("a directory");
        let dir = dev_dir(root.path());
        let data = dir.join("data");
        let config = dir.join(CONFIG_FILE);
        // A directory with a configuration: the configuration's `data_dir`.
        assert_eq!(
            data_dir_of(&StopTarget::DataDir(dir.clone())).expect("found"),
            data
        );
        assert_eq!(
            data_dir_of(&StopTarget::Config(config)).expect("found"),
            data
        );
        // The data directory itself, and a directory with neither file.
        assert_eq!(
            data_dir_of(&StopTarget::DataDir(data.clone())).expect("found"),
            data
        );
        let empty = root.path().join("empty");
        assert_eq!(
            data_dir_of(&StopTarget::DataDir(empty.clone())).expect("found"),
            empty
        );
        // A process file wins over a configuration beside it.
        ProcessFile::this_process(PathBuf::from("fleetring"))
            .write(&dir)
            .expect("written");
        assert_eq!(
            data_dir_of(&StopTarget::DataDir(dir.clone())).expect("found"),
            dir
        );
        let missing = data_dir_of(&StopTarget::Config(root.path().join("none.toml")));
        assert!(
            matches!(missing, Err(StopError::Config { .. })),
            "{missing:?}"
        );
    }

    #[test]
    fn only_a_node_without_a_supervisor_or_started_by_dev_d_is_stopped() {
        let root = tempfile::tempdir().expect("a directory");
        let _locked = fleetringd::DataDir::lock(root.path()).expect("locked");
        let write = |supervisor, started_by| {
            ProcessFile {
                supervisor,
                started_by,
                ..ProcessFile::this_process(PathBuf::from("fleetring"))
            }
            .write(root.path())
            .expect("written");
        };
        write(None, None);
        assert!(target(root.path()).is_ok());
        write(Some(Supervisor::Systemd), Some(StartedBy::Detach));
        assert!(target(root.path()).is_ok());
        write(Some(Supervisor::Launchd), None);
        let refused = target(root.path()).expect_err("supervised");
        assert!(
            refused
                .to_string()
                .contains("launchd runs this node; stop it with `brew services stop fleetring`"),
            "{refused}"
        );
    }

    #[test]
    fn stop_without_a_target_hints_at_dev_stop() {
        let error = crate::parse(
            vec!["fleetring".into(), "stop".into()],
            crate::compat::ProgramName::Fleetring,
        )
        .expect_err("a target is required");
        assert_eq!(error.exit_code(), 2);
        assert_eq!(
            target_hint(&error),
            Some("hint: to stop the dev node, run `fleetring dev stop`")
        );
        let other = crate::parse(
            vec!["fleetring".into(), "db".into(), "create".into()],
            crate::compat::ProgramName::Fleetring,
        )
        .expect_err("a name is required");
        assert_eq!(target_hint(&other), None);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_refuses_to_stop() {
        let root = tempfile::tempdir().expect("a directory");
        let error = stop(root.path(), Duration::from_secs(1), &Progress::hidden())
            .await
            .expect_err("not on Windows");
        assert!(matches!(error, StopError::Unsupported), "{error}");
        assert!(error.to_string().contains("press Ctrl-C in its terminal"));
    }
}
