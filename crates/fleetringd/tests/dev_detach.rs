//! FR-075: `fleetring dev -d`, a dev node in the background, and `dev stop` (ADR-0043, point 5).
//!
//! Each test runs the real binary with a home and a configuration directory of its own, on
//! ports of its own. A machine kills the node it started when it is dropped, whatever the test
//! did, so no test leaves a node running.
#![cfg(unix)]

use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use fleetringd::upgrade::{PROCESS_FILE, ProcessFile};
use rustix::process::{Pid, Signal, getsid, kill_process, test_kill_process};

mod support;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// What the dev node's banner starts with once it serves.
const BANNER: &str = "fleetringd is running in dev mode";

/// How long to wait between checks, and how many to make: about 30 seconds.
const POLL: Duration = Duration::from_millis(50);
const ATTEMPTS: u32 = 600;

/// The supervisor variables a shell may inherit, which a detached node must not.
const INHERITED: [(&str, &str); 3] = [
    ("INVOCATION_ID", "x"),
    ("JOURNAL_STREAM", "1:2"),
    ("XPC_SERVICE_NAME", "homebrew.mxcl.fleetring"),
];

/// The next port [`port_pair`] tries.
static NEXT_PORT: AtomicU16 = AtomicU16::new(0);

/// A port that is free with the one after it, for a dev node and its admin listener. Ports come
/// from below the systems' ephemeral ranges, from a start that differs between processes, in a
/// range of their own (`tests/dev/main.rs` and `tests/contexts.rs` do the same).
fn port_pair() -> TestResult<u16> {
    let start = 12_000 + u16::try_from(std::process::id() % 200)? * 20;
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

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The process ID in a line such as `dev node ready (pid 4242)`.
fn pid_in(text: &str) -> TestResult<u32> {
    let start = text.find("(pid ").ok_or("no pid")? + "(pid ".len();
    let rest = text.get(start..).ok_or("no pid")?;
    let end = rest.find(')').ok_or("no pid")?;
    Ok(rest.get(..end).ok_or("no pid")?.parse()?)
}

fn pid_of(pid: u32) -> TestResult<Pid> {
    Ok(Pid::from_raw(i32::try_from(pid)?).ok_or("pid 0")?)
}

/// Whether the process `pid` has exited: it can't be signalled, or it is a zombie its new
/// parent hasn't reaped yet.
fn gone(pid: u32) -> TestResult<bool> {
    if test_kill_process(pid_of(pid)?).is_err() {
        return Ok(true);
    }
    // `/proc/<pid>/stat` is `pid (comm) state …`; a zombie's state is `Z`.
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    Ok(stat
        .rsplit_once(") ")
        .is_some_and(|(_, rest)| rest.starts_with('Z')))
}

/// Waits until `done` holds, checking every [`POLL`].
fn wait_until(what: &str, mut done: impl FnMut() -> TestResult<bool>) -> TestResult {
    for _ in 0..ATTEMPTS {
        if done()? {
            return Ok(());
        }
        std::thread::sleep(POLL);
    }
    Err(format!("timed out waiting until {what}").into())
}

/// A scratch machine: a home whose `~/.fleetring` is the dev directory, a configuration
/// directory for the contexts, and the port its dev node takes.
struct Machine {
    root: tempfile::TempDir,
    port: u16,
}

impl Machine {
    fn new() -> TestResult<Self> {
        Ok(Self {
            root: tempfile::tempdir()?,
            port: port_pair()?,
        })
    }

    /// A machine whose dev node `dev -d` has started, with `env` in `dev -d`'s environment.
    /// A start that loses its port to another process begins again on another.
    fn started(env: &[(&str, &str)]) -> TestResult<(Self, Output)> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let machine = Self::new()?;
            let output = machine.detach(env)?;
            if output.status.success() {
                return Ok((machine, output));
            }
            if attempts >= 5 || !printed(&output).contains("Address already in use") {
                return Err(format!("dev -d failed: {}", printed(&output)).into());
            }
        }
    }

    fn dev_dir(&self) -> PathBuf {
        self.root.path().join(".fleetring")
    }

    fn data_dir(&self) -> PathBuf {
        self.dev_dir().join("data")
    }

    /// `fleetring` with `args`, whose home and contexts are this machine's.
    fn command(&self, args: &[&str]) -> Command {
        let mut command = support::fleetring();
        command
            .args(args)
            .env("HOME", self.root.path())
            .env("XDG_CONFIG_HOME", self.root.path().join("xdg"))
            .stdin(Stdio::null());
        command
    }

    fn run(&self, args: &[&str]) -> TestResult<Output> {
        Ok(self.command(args).output()?)
    }

    /// `dev -d` on this machine's port, with `env` set.
    fn detach(&self, env: &[(&str, &str)]) -> TestResult<Output> {
        let port = self.port.to_string();
        let mut command = self.command(&["dev", "-d", "--port", &port]);
        command.envs(env.iter().copied());
        Ok(command.output()?)
    }

    fn process_file(&self) -> TestResult<Option<ProcessFile>> {
        Ok(ProcessFile::read(&self.data_dir())?)
    }
}

impl Drop for Machine {
    /// Kills the node, if one still runs, and waits until it has gone.
    fn drop(&mut self) {
        let Ok(Some(file)) = self.process_file() else {
            return;
        };
        let Ok(pid) = pid_of(file.pid) else {
            return;
        };
        let _ = kill_process(pid, Signal::KILL);
        let _ = wait_until("the node is killed", || gone(file.pid));
    }
}

#[test]
fn a_clean_machine_creates_a_database_after_dev_detach() -> TestResult {
    let (machine, started) = Machine::started(&[])?;
    let text = stdout(&started);
    assert!(text.starts_with("dev node ready (pid "), "{text}");
    for line in [
        &format!("  address    127.0.0.1:{}\n", machine.port),
        &format!("  data dir   {}\n", machine.dev_dir().display()),
        &format!(
            "  log        {}\n",
            machine.dev_dir().join("fleetring.log").display()
        ),
        "  context    dev (current)\n",
        "Stop it with `fleetring dev stop`.\n",
    ] {
        assert!(text.contains(line), "{line:?} in {text}");
    }
    let created = machine.run(&["db", "create", "notes"])?;
    assert!(created.status.success(), "{}", printed(&created));
    assert!(
        stdout(&created).starts_with("created notes\n"),
        "{}",
        printed(&created)
    );
    let stopped = machine.run(&["dev", "stop"])?;
    assert!(stopped.status.success(), "{}", printed(&stopped));
    Ok(())
}

#[test]
fn dev_detach_is_idempotent() -> TestResult {
    let (machine, first) = Machine::started(&[])?;
    let pid = pid_in(&stdout(&first))?;
    let second = machine.detach(&[])?;
    assert!(second.status.success(), "{}", printed(&second));
    let text = stdout(&second);
    assert!(
        text.starts_with(&format!("dev node already running (pid {pid})\n")),
        "{text}"
    );
    assert!(text.contains("  context    dev (current)\n"), "{text}");
    Ok(())
}

#[test]
fn dev_stop_stops_its_own_node_despite_inherited_supervisor_variables() -> TestResult {
    let (machine, started) = Machine::started(&INHERITED)?;
    let pid = pid_in(&stdout(&started))?;
    let written = fs::read_to_string(machine.data_dir().join(PROCESS_FILE))?;
    let file: serde_json::Value = serde_json::from_str(&written)?;
    assert_eq!(file["started_by"], "detach", "{written}");
    assert!(file.get("supervisor").is_none(), "{written}");
    #[cfg(target_os = "linux")]
    {
        let environ = fs::read(format!("/proc/{pid}/environ"))?;
        for variable in environ.split(|&byte| byte == 0) {
            let variable = String::from_utf8_lossy(variable);
            assert!(
                INHERITED
                    .iter()
                    .all(|(name, _)| !variable.starts_with(&format!("{name}="))),
                "the node inherited {variable}"
            );
        }
    }
    let mut stop = machine.command(&["dev", "stop"]);
    stop.envs(INHERITED.iter().copied());
    let stopped = stop.output()?;
    assert!(stopped.status.success(), "{}", printed(&stopped));
    assert_eq!(
        stdout(&stopped),
        format!("stopped the dev node (pid {pid})\n")
    );
    Ok(())
}

#[test]
fn dev_stop_drains_and_the_process_exits() -> TestResult {
    let (machine, started) = Machine::started(&[])?;
    let pid = pid_in(&stdout(&started))?;
    let running = machine.run(&["dev", "status"])?;
    assert!(running.status.success(), "{}", printed(&running));
    assert!(
        stdout(&running).starts_with(&format!("running (pid {pid})\n")),
        "{}",
        printed(&running)
    );
    let stopped = machine.run(&["dev", "stop"])?;
    assert!(stopped.status.success(), "{}", printed(&stopped));
    wait_until("the node has exited", || gone(pid))?;
    wait_until("the process file is removed", || {
        Ok(!machine.data_dir().join(PROCESS_FILE).exists())
    })?;
    let status = machine.run(&["dev", "status"])?;
    assert!(status.status.success(), "{}", printed(&status));
    assert!(
        stdout(&status).starts_with("not running\n"),
        "{}",
        printed(&status)
    );
    Ok(())
}

#[test]
fn the_detached_node_is_in_its_own_session() -> TestResult {
    let (_machine, started) = Machine::started(&[])?;
    let pid = pid_of(pid_in(&stdout(&started))?)?;
    let session = getsid(Some(pid))?;
    assert_eq!(session, pid, "the node leads its own session");
    assert_ne!(session, getsid(None)?, "not the test's session");
    Ok(())
}

#[test]
fn the_log_has_no_colour_and_holds_the_banner() -> TestResult {
    let (machine, _started) = Machine::started(&[])?;
    let logs = machine.run(&["dev", "logs"])?;
    assert!(logs.status.success(), "{}", printed(&logs));
    let log = fs::read_to_string(machine.dev_dir().join("fleetring.log"))?;
    assert_eq!(stdout(&logs), log, "dev logs prints the file");
    assert!(log.contains(BANNER), "{log}");
    assert!(log.contains(" INFO "), "{log}");
    assert!(!log.contains('\u{1b}'), "an escape sequence in {log:?}");
    Ok(())
}

#[test]
fn a_child_that_fails_reports_the_log_tail() -> TestResult {
    let machine = Machine::new()?;
    // Taken, so the node can't serve on it.
    let _taken = TcpListener::bind(("127.0.0.1", machine.port))?;
    let failed = machine.detach(&[])?;
    assert_eq!(failed.status.code(), Some(1), "{}", printed(&failed));
    let error = String::from_utf8_lossy(&failed.stderr);
    let log = machine.dev_dir().join("fleetring.log");
    assert!(
        error.contains(&format!(
            "the dev node exited before it was ready; the last lines of {}:",
            log.display()
        )),
        "{error}"
    );
    assert!(
        error.contains("  fleetring: "),
        "the node's own error: {error}"
    );
    assert!(stdout(&failed).is_empty(), "{}", printed(&failed));
    Ok(())
}
