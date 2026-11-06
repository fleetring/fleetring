//! FR-075: `fleetring stop` stops a node started in a terminal, through its process file, and
//! refuses what it shouldn't stop (ADR-0043, point 5.4).
#![cfg(unix)]

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, ExitStatus, Output, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use fleetringd::DataDir;
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::upgrade::{ProcessFile, Running, Supervisor, running};

mod support;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// How long to wait between checks, and how many to make: about 30 seconds.
const POLL: Duration = Duration::from_millis(50);
const ATTEMPTS: u32 = 600;

/// A process ID no process has: above every system's limit, below `i32::MAX`.
const NO_PROCESS: u32 = 2_147_483_000;

/// The next port [`port_pair`] tries.
static NEXT_PORT: AtomicU16 = AtomicU16::new(0);

/// A port that is free with the one after it, for a node and its admin listener, in a range of
/// its own (`tests/dev_detach.rs` does the same, below it).
fn port_pair() -> TestResult<u16> {
    let start = 16_000 + u16::try_from(std::process::id() % 200)? * 20;
    let _ = NEXT_PORT.compare_exchange(0, start, Ordering::SeqCst, Ordering::SeqCst);
    for _ in 0..1_000 {
        let port = NEXT_PORT.fetch_add(2, Ordering::SeqCst);
        let free = |port: u16| TcpListener::bind(("127.0.0.1", port));
        if let (Ok(_first), Ok(_second)) = (free(port), free(port + 1)) {
            return Ok(port);
        }
    }
    Err("no free pair of ports".into())
}

fn printed(output: &Output) -> String {
    format!(
        "{}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// `fleetring stop` with `args`.
fn stop(args: &[&str]) -> TestResult<Output> {
    Ok(support::fleetring()
        .arg("stop")
        .args(args)
        .stdin(Stdio::null())
        .output()?)
}

/// A node `serve` runs as this test's child, as a terminal would run it; killed when dropped.
struct Node {
    child: Child,
    dir: PathBuf,
    log: PathBuf,
}

impl Node {
    /// Starts `serve` on a dev directory's configuration under `root`, and waits until it runs.
    /// A start that loses its port to another process begins again on another.
    fn start(root: &Path) -> TestResult<Self> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match Self::start_on(root, attempts) {
                Err(error)
                    if attempts < 5 && error.to_string().contains("Address already in use") => {}
                other => return other,
            }
        }
    }

    fn start_on(root: &Path, attempt: u32) -> TestResult<Self> {
        let dir = root.join(format!("node-{attempt}"));
        fleetringd::dev::prepare(&dir, Some(port_pair()?), SystemClock.now())?;
        let log = root.join(format!("node-{attempt}.log"));
        let child = support::fleetring()
            .arg("serve")
            .arg("--config")
            .arg(dir.join("fleetringd.toml"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log)?)
            .spawn()?;
        let mut node = Self { child, dir, log };
        for _ in 0..ATTEMPTS {
            if matches!(running(&node.data_dir())?, Running::Yes(_)) {
                return Ok(node);
            }
            if let Some(status) = node.child.try_wait()? {
                return Err(format!("the node exited ({status}): {}", node.log()).into());
            }
            std::thread::sleep(POLL);
        }
        Err(format!("the node never started: {}", node.log()).into())
    }

    fn data_dir(&self) -> PathBuf {
        self.dir.join("data")
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Waits for the node to exit, and returns how it did.
    fn exited(&mut self) -> TestResult<ExitStatus> {
        for _ in 0..ATTEMPTS {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            std::thread::sleep(POLL);
        }
        Err(format!("the node never exited: {}", self.log()).into())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn stop_drains_a_node_started_in_a_terminal() -> TestResult {
    let root = tempfile::tempdir()?;
    let mut node = Node::start(root.path())?;
    let pid = node.child.id();
    // The dev directory holds the configuration that names the data directory.
    let dir = node.dir.to_str().ok_or("a UTF-8 path")?.to_owned();
    let stopped = stop(&["--data-dir", &dir])?;
    assert!(stopped.status.success(), "{}", printed(&stopped));
    assert_eq!(
        String::from_utf8_lossy(&stopped.stdout),
        format!("stopped the node (pid {pid})\n")
    );
    let status = node.exited()?;
    assert!(status.success(), "{status}: {}", node.log());
    assert!(
        node.log().contains("shutting down"),
        "the node drained: {}",
        node.log()
    );
    // The lock is free.
    drop(DataDir::lock(node.data_dir())?);
    Ok(())
}

#[test]
fn stop_without_a_process_file_says_so() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().ok_or("a UTF-8 path")?;
    let stopped = stop(&["--data-dir", path])?;
    assert_eq!(stopped.status.code(), Some(1), "{}", printed(&stopped));
    let error = String::from_utf8_lossy(&stopped.stderr);
    assert!(
        error.contains(&format!(
            "no node is running from {path}: it has no fleetringd.process"
        )),
        "{error}"
    );
    Ok(())
}

#[test]
fn stop_with_a_stale_process_file_names_the_pid() -> TestResult {
    let dir = tempfile::tempdir()?;
    ProcessFile {
        pid: NO_PROCESS,
        ..ProcessFile::this_process(PathBuf::from("fleetring"))
    }
    .write(dir.path())?;
    let path = dir.path().to_str().ok_or("a UTF-8 path")?;
    let stopped = stop(&["--data-dir", path])?;
    assert_eq!(stopped.status.code(), Some(1), "{}", printed(&stopped));
    let error = String::from_utf8_lossy(&stopped.stderr);
    assert!(
        error.contains(&format!("(pid {NO_PROCESS}) isn't running")),
        "{error}"
    );
    assert!(error.contains("fleetringd.process"), "{error}");
    Ok(())
}

#[test]
fn stop_refuses_a_supervised_node() -> TestResult {
    let dir = tempfile::tempdir()?;
    // A live process holds the lock: this one.
    let _locked = DataDir::lock(dir.path())?;
    ProcessFile {
        pid: NO_PROCESS,
        supervisor: Some(Supervisor::Systemd),
        started_by: None,
        ..ProcessFile::this_process(PathBuf::from("fleetring"))
    }
    .write(dir.path())?;
    let path = dir.path().to_str().ok_or("a UTF-8 path")?;
    let stopped = stop(&["--data-dir", path])?;
    assert_eq!(stopped.status.code(), Some(1), "{}", printed(&stopped));
    let error = String::from_utf8_lossy(&stopped.stderr);
    assert!(
        error.contains("systemd runs this node; stop it with `systemctl stop <unit>`"),
        "{error}"
    );
    Ok(())
}

#[test]
fn stop_needs_a_target() -> TestResult {
    let stopped = stop(&[])?;
    assert_eq!(stopped.status.code(), Some(2), "{}", printed(&stopped));
    let error = String::from_utf8_lossy(&stopped.stderr);
    assert!(
        error.contains("<--data-dir <DIR>|--config <FILE>>"),
        "{error}"
    );
    assert!(
        error.contains("hint: to stop the dev node, run `fleetring dev stop`"),
        "{error}"
    );
    Ok(())
}
