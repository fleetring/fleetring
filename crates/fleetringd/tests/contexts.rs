//! FR-056: contexts and `fleetring switch` (ADR-0043, point 4).
//!
//! Each test runs the real binary with `XDG_CONFIG_HOME` set on the child process to a scratch
//! directory, never on the test process, so the developer's own contexts are never read or
//! written. A dev node supplies a fleet with TLS to point contexts at.
#![cfg(unix)]

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// The variables a developer's shell may hold that would point a command elsewhere.
const CLIENT_VARS: [&str; 10] = [
    "FLEETRING_CONTEXT",
    "FLEETRING_CONTROL",
    "FLEETRING_TLS_DIR",
    "FLEETRING_AS",
    "FLEETRING_CA",
    "FLEETRING_CERT",
    "FLEETRING_KEY",
    "FLEETRING_TOKEN",
    "FLEETRING_TOKEN_FILE",
    "FLEETRING_LOG_FORMAT",
];

/// What the dev node's banner starts with once it serves.
const BANNER: &str = "fleetringd is running in dev mode";

/// How long to wait between attempts, and how many to make: about a minute.
const POLL: Duration = Duration::from_millis(50);
const ATTEMPTS: u32 = 1_200;

/// An address nothing listens on: connections to it are refused at once.
const DEAD: &str = "127.0.0.1:1";

/// The next port [`port_pair`] tries.
static NEXT_PORT: AtomicU16 = AtomicU16::new(0);

/// A port that is free with the one after it. Ports come from below the systems' ephemeral
/// ranges, from a start that differs between processes (`tests/dev/main.rs` does the same, with
/// a different range).
fn port_pair() -> TestResult<u16> {
    let start = 24_000 + u16::try_from(std::process::id() % 400)? * 20;
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

/// A scratch machine: a home, and a configuration directory the contexts live in.
struct Machine {
    root: tempfile::TempDir,
}

impl Machine {
    fn new() -> TestResult<Self> {
        Ok(Self {
            root: tempfile::tempdir()?,
        })
    }

    fn config_dir(&self) -> PathBuf {
        self.root.path().join("xdg")
    }

    fn contexts_file(&self) -> PathBuf {
        self.config_dir().join("fleetring").join("contexts.toml")
    }

    /// `fleetring` with `args`, in a clean environment whose contexts are this machine's.
    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fleetring"));
        command
            .args(args)
            .env("HOME", self.root.path())
            .env("XDG_CONFIG_HOME", self.config_dir())
            .stdin(Stdio::null());
        for variable in CLIENT_VARS {
            command.env_remove(variable);
        }
        command
    }

    fn run(&self, args: &[&str]) -> TestResult<Output> {
        Ok(self.command(args).output()?)
    }

    /// Runs `args`, which must succeed, and returns stdout.
    fn ok(&self, args: &[&str]) -> TestResult<String> {
        let output = self.run(args)?;
        assert!(output.status.success(), "{args:?}: {}", printed(&output));
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn current(&self) -> TestResult<String> {
        self.ok(&["context", "current"])
    }
}

fn printed(output: &Output) -> String {
    format!(
        "{}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// A dev node, killed when dropped, with the files a context points at.
struct Node {
    child: Child,
    control: String,
    tls: PathBuf,
}

impl Node {
    /// Starts a node on a free port. A port can be taken between the check and the node's
    /// bind, by a socket of another test process, so a start that fails for that reason
    /// begins again from scratch on another port (the first port is kept in the node's files).
    fn start(root: &Path) -> TestResult<Self> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match Self::start_on(root, port_pair()?) {
                Err(error)
                    if attempts < 5 && error.to_string().contains("Address already in use") =>
                {
                    fs::remove_dir_all(root.join("node"))?;
                }
                other => return other,
            }
        }
    }

    fn start_on(root: &Path, port: u16) -> TestResult<Self> {
        let dir = root.join("node");
        let log = root.join("node.log");
        let mut command = Command::new(env!("CARGO_BIN_EXE_fleetring"));
        command
            .args(["dev", "--port", &port.to_string(), "--data-dir"])
            .arg(&dir)
            // The node's own `dev` context goes to a place of its own.
            .env("HOME", root.join("node-home"))
            .env("XDG_CONFIG_HOME", root.join("node-xdg"))
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log)?);
        for variable in CLIENT_VARS {
            command.env_remove(variable);
        }
        let mut child = command.spawn()?;
        for _ in 0..ATTEMPTS {
            if fs::read_to_string(&log)
                .unwrap_or_default()
                .contains(BANNER)
            {
                return Ok(Self {
                    child,
                    control: format!("127.0.0.1:{port}"),
                    tls: dir.join("tls"),
                });
            }
            if let Some(status) = child.try_wait()? {
                return Err(format!(
                    "the dev node exited ({status})\n{}",
                    fs::read_to_string(&log).unwrap_or_default()
                )
                .into());
            }
            std::thread::sleep(POLL);
        }
        Err("the dev node never started".into())
    }

    /// Adds a context named `name` for this node, with the given extra arguments, and waits
    /// until the node is a live member of its own fleet.
    fn add_context(&self, machine: &Machine, name: &str, extra: &[&str]) -> TestResult {
        let tls = self.tls.to_str().ok_or("a UTF-8 path")?;
        let mut args = vec![
            "context",
            "add",
            name,
            "--control",
            &self.control,
            "--tls-dir",
            tls,
            "--as",
            "admin",
        ];
        args.extend_from_slice(extra);
        machine.ok(&args)?;
        Ok(())
    }

    /// Waits until `db list --context <name>` works.
    fn wait_until_live(&self, machine: &Machine, name: &str) -> TestResult {
        let mut last = String::new();
        for _ in 0..ATTEMPTS {
            let output = machine.run(&["status", "--context", name])?;
            if output.status.success() && String::from_utf8_lossy(&output.stdout).contains(" live ")
            {
                return Ok(());
            }
            last = printed(&output);
            std::thread::sleep(POLL);
        }
        Err(format!("the node never became a live member: {last}").into())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A machine with a live node, the context `good` for it (current), and `bad`, which names a
/// dead address with the same identity.
fn fleet() -> TestResult<(Machine, Node)> {
    let machine = Machine::new()?;
    let node = Node::start(machine.root.path())?;
    node.add_context(&machine, "good", &[])?;
    let tls = node.tls.to_str().ok_or("a UTF-8 path")?;
    machine.ok(&[
        "context",
        "add",
        "bad",
        "--control",
        DEAD,
        "--tls-dir",
        tls,
        "--as",
        "admin",
    ])?;
    node.wait_until_live(&machine, "good")?;
    Ok((machine, node))
}

#[test]
fn add_use_and_db_list_need_no_other_flag() -> TestResult {
    let machine = Machine::new()?;
    let node = Node::start(machine.root.path())?;
    assert_eq!(machine.run(&["context", "current"])?.status.code(), Some(1));
    // The first context added becomes current, and the fleet comes from the certificate.
    let tls = node.tls.to_str().ok_or("a UTF-8 path")?;
    let added = machine.run(&[
        "context",
        "add",
        "home",
        "--control",
        &node.control,
        "--tls-dir",
        tls,
        "--as",
        "admin",
    ])?;
    assert!(added.status.success(), "{}", printed(&added));
    assert!(
        stderr(&added).contains("added context 'home', now current"),
        "{}",
        printed(&added)
    );
    machine.ok(&["context", "use", "home"])?;
    assert_eq!(machine.current()?, "home\n");
    let shown = machine.ok(&["context", "show", "-o", "json"])?;
    let shown: serde_json::Value = serde_json::from_str(&shown)?;
    assert_eq!(shown["fleet"], "dev");
    assert_eq!(shown["as"], "admin");
    assert_eq!(shown["current"], true);

    node.wait_until_live(&machine, "home")?;
    // With no connection flag at all.
    let created = machine.run(&["db", "create", "notes"])?;
    assert!(created.status.success(), "{}", printed(&created));
    let listed = machine.ok(&["db", "list"])?;
    assert!(listed.contains("notes"), "{listed}");
    Ok(())
}

#[test]
fn the_file_holds_paths_and_is_private() -> TestResult {
    use std::os::unix::fs::PermissionsExt as _;
    let machine = Machine::new()?;
    machine.ok(&[
        "context",
        "add",
        "home",
        "--control",
        "10.0.0.2:7400",
        "--fleet",
        "home",
        "--token-file",
        "tokens/home",
    ])?;
    let text = fs::read_to_string(machine.contexts_file())?;
    assert!(text.contains("version = 1"), "{text}");
    let token_file = std::path::absolute("tokens/home")?;
    assert!(
        text.contains(&format!(
            "token_file = {:?}",
            token_file.display().to_string()
        )),
        "paths are stored absolute: {text}"
    );
    let mode = |path: &Path| fs::metadata(path).map(|meta| meta.permissions().mode() & 0o777);
    assert_eq!(mode(&machine.contexts_file())?, 0o600);
    assert_eq!(mode(&machine.config_dir().join("fleetring"))?, 0o700);
    Ok(())
}

#[test]
fn a_context_file_of_another_version_is_refused() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&[
        "context",
        "add",
        "home",
        "--control",
        "a:1",
        "--fleet",
        "home",
    ])?;
    let file = machine.contexts_file();
    let text = fs::read_to_string(&file)?.replace("version = 1", "version = 2");
    fs::write(&file, &text)?;
    for args in [
        &["context", "list"][..],
        &["context", "use", "home"],
        &["db", "list"],
    ] {
        let output = machine.run(args)?;
        assert_eq!(
            output.status.code(),
            Some(1),
            "{args:?}: {}",
            printed(&output)
        );
        assert!(
            stderr(&output).contains(&format!(
                "{} has format version 2; this release reads version 1",
                file.display()
            )),
            "{args:?}: {}",
            printed(&output)
        );
    }
    assert_eq!(
        fs::read_to_string(&file)?,
        text,
        "a refused file is left alone"
    );
    Ok(())
}

#[test]
fn concurrent_adds_lose_no_context() -> TestResult {
    let machine = Machine::new()?;
    let children: Vec<Child> = (0..8)
        .map(|n| {
            machine
                .command(&[
                    "context",
                    "add",
                    &format!("c{n}"),
                    "--control",
                    "a:1",
                    "--fleet",
                    "f",
                ])
                .stderr(Stdio::null())
                .spawn()
        })
        .collect::<Result<_, _>>()?;
    for mut child in children {
        assert!(child.wait()?.success());
    }
    let list = machine.ok(&["context", "list", "--json"])?;
    let list: serde_json::Value = serde_json::from_str(&list)?;
    assert_eq!(list["contexts"].as_array().map(Vec::len), Some(8), "{list}");
    Ok(())
}

#[test]
fn context_flag_beats_a_field_variable() -> TestResult {
    let (machine, node) = fleet()?;
    machine.ok(&["switch", "bad"])?;
    // The variable names a dead address, and `--context good` outranks it.
    let output = machine
        .command(&["db", "list", "--context", "good"])
        .env("FLEETRING_CONTROL", DEAD)
        .output()?;
    assert!(output.status.success(), "{}", printed(&output));
    assert!(
        stderr(&output)
            .contains("warning: FLEETRING_CONTROL is ignored: --context good sets control"),
        "{}",
        printed(&output)
    );
    drop(node);
    Ok(())
}

#[test]
fn a_field_variable_beats_the_current_context() -> TestResult {
    let (machine, node) = fleet()?;
    machine.ok(&["switch", "bad"])?;
    // Without the variable the current context is dead.
    let dead = machine.run(&["db", "list"])?;
    assert!(!dead.status.success(), "{}", printed(&dead));
    let output = machine
        .command(&["db", "list"])
        .env("FLEETRING_CONTROL", &node.control)
        .output()?;
    assert!(output.status.success(), "{}", printed(&output));
    assert!(
        stderr(&output).contains(&format!(
            "warning: FLEETRING_CONTROL overrides context 'bad' (control {DEAD})"
        )),
        "{}",
        printed(&output)
    );
    // The variable that names a context is outranked by the field variable the same way.
    let output = machine
        .command(&["db", "list"])
        .env("FLEETRING_CONTEXT", "bad")
        .env("FLEETRING_CONTROL", &node.control)
        .output()?;
    assert!(output.status.success(), "{}", printed(&output));
    Ok(())
}

#[test]
fn a_field_flag_beats_the_context_flag() -> TestResult {
    let (machine, node) = fleet()?;
    let output = machine.run(&["db", "list", "--context", "bad", "--control", &node.control])?;
    assert!(output.status.success(), "{}", printed(&output));
    assert_eq!(stderr(&output), "", "a flag over a context is no surprise");
    Ok(())
}

#[test]
fn fleetring_context_names_a_context_and_the_flag_beats_it() -> TestResult {
    let (machine, node) = fleet()?;
    machine.ok(&["switch", "bad"])?;
    let by_variable = machine
        .command(&["db", "list"])
        .env("FLEETRING_CONTEXT", "good")
        .output()?;
    assert!(by_variable.status.success(), "{}", printed(&by_variable));
    let by_flag = machine
        .command(&["db", "list", "--context", "good"])
        .env("FLEETRING_CONTEXT", "bad")
        .output()?;
    assert!(by_flag.status.success(), "{}", printed(&by_flag));
    let unknown = machine
        .command(&["db", "list"])
        .env("FLEETRING_CONTEXT", "nope")
        .output()?;
    assert_eq!(unknown.status.code(), Some(1));
    assert!(
        stderr(&unknown).contains(&format!(
            "context 'nope' doesn't exist in {}",
            machine.contexts_file().display()
        )),
        "{}",
        printed(&unknown)
    );
    drop(node);
    Ok(())
}

#[test]
fn the_warning_shows_without_a_terminal_and_not_with_q() -> TestResult {
    let (machine, node) = fleet()?;
    machine.ok(&["switch", "bad"])?;
    for (args, warns) in [
        (&["db", "list"][..], true),
        (&["-q", "db", "list"], false),
        (&["db", "list", "-q"], false),
    ] {
        let output = machine
            .command(args)
            .env("FLEETRING_CONTROL", &node.control)
            .output()?;
        assert!(output.status.success(), "{args:?}: {}", printed(&output));
        // stdin and stderr are pipes here, not terminals.
        assert_eq!(
            stderr(&output).contains("FLEETRING_CONTROL overrides context 'bad'"),
            warns,
            "{args:?}: {}",
            printed(&output)
        );
    }
    Ok(())
}

#[test]
fn a_fleet_mismatch_names_both_fleets() -> TestResult {
    let (machine, node) = fleet()?;
    node.add_context(&machine, "wrong", &["--fleet", "other"])?;
    let output = machine.run(&["db", "list", "--context", "wrong"])?;
    assert_eq!(output.status.code(), Some(1), "{}", printed(&output));
    assert!(
        stderr(&output).contains(
            "context 'wrong' is for fleet 'other', but its certificate is for fleet 'dev'"
        ),
        "{}",
        printed(&output)
    );
    Ok(())
}

#[test]
fn with_nothing_set_up_a_command_says_how_to_start() -> TestResult {
    let machine = Machine::new()?;
    let output = machine.run(&["status", "--insecure-plaintext"])?;
    assert_eq!(output.status.code(), Some(1), "{}", printed(&output));
    let text = stderr(&output);
    assert!(text.contains("no control address"), "{text}");
    assert!(text.contains("hint: run `fleetring dev -d` for a local node, or `fleetring context add <name> --control <host:port>"), "{text}");
    Ok(())
}

#[test]
fn switch_dash_goes_back() -> TestResult {
    let machine = Machine::new()?;
    for (name, fleet) in [("a", "fa"), ("b", "fb")] {
        machine.ok(&["context", "add", name, "--control", "x:1", "--fleet", fleet])?;
    }
    // Nothing to go back to yet.
    let output = machine.run(&["switch", "-"])?;
    assert_eq!(output.status.code(), Some(1), "{}", printed(&output));
    assert!(stderr(&output).contains("no previous context to go back to"));

    let output = machine.run(&["switch", "b"])?;
    assert!(output.status.success(), "{}", printed(&output));
    assert!(
        stderr(&output).contains("switched to context 'b' (fleet 'fb')"),
        "{}",
        printed(&output)
    );
    assert_eq!(machine.current()?, "b\n");
    let output = machine.run(&["switch", "-"])?;
    assert!(
        stderr(&output).contains("switched to context 'a' (fleet 'fa')"),
        "{}",
        printed(&output)
    );
    assert_eq!(machine.current()?, "a\n");
    machine.ok(&["switch", "-"])?;
    assert_eq!(machine.current()?, "b\n");
    // `context use` records the previous one too.
    machine.ok(&["context", "use", "a"])?;
    machine.ok(&["switch", "-"])?;
    assert_eq!(machine.current()?, "b\n");
    // An unknown name changes nothing.
    let output = machine.run(&["switch", "nope"])?;
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(machine.current()?, "b\n");
    // Removing the current context leaves none.
    let output = machine.run(&["context", "remove", "b"])?;
    assert!(
        stderr(&output).contains("no context is current now"),
        "{}",
        printed(&output)
    );
    assert_eq!(machine.run(&["context", "current"])?.status.code(), Some(1));
    Ok(())
}

#[test]
fn switch_without_a_name_or_a_terminal_exits_2() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&[
        "context",
        "add",
        "home",
        "--control",
        "x:1",
        "--fleet",
        "home",
    ])?;
    let output = machine.run(&["switch"])?;
    assert_eq!(output.status.code(), Some(2), "{}", printed(&output));
    assert!(output.stdout.is_empty(), "{}", printed(&output));
    let text = stderr(&output);
    assert!(text.contains("CURRENT  NAME  FLEET  CONTROL  AS"), "{text}");
    assert!(text.contains("*        home  home   x:1"), "{text}");
    assert!(
        text.contains("hint: pass a name: `fleetring switch <name>`"),
        "{text}"
    );
    // A bad name is a usage error too.
    let output = machine.run(&["switch", "Not A Name"])?;
    assert_eq!(output.status.code(), Some(2), "{}", printed(&output));
    Ok(())
}

#[test]
fn context_current_prints_only_the_name_and_exits_1_when_none() -> TestResult {
    let machine = Machine::new()?;
    let none = machine.run(&["context", "current"])?;
    assert_eq!(none.status.code(), Some(1));
    assert!(
        none.stdout.is_empty() && none.stderr.is_empty(),
        "{}",
        printed(&none)
    );
    machine.ok(&[
        "context",
        "add",
        "home",
        "--control",
        "x:1",
        "--fleet",
        "home",
    ])?;
    let one = machine.run(&["context", "current"])?;
    assert_eq!(one.status.code(), Some(0));
    assert_eq!(String::from_utf8_lossy(&one.stdout), "home\n");
    assert!(one.stderr.is_empty(), "{}", printed(&one));
    Ok(())
}

#[test]
fn adding_a_name_that_exists_is_refused_with_a_hint() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&[
        "context",
        "add",
        "home",
        "--control",
        "x:1",
        "--fleet",
        "home",
    ])?;
    let output = machine.run(&[
        "context",
        "add",
        "home",
        "--control",
        "y:1",
        "--fleet",
        "home",
    ])?;
    assert_eq!(output.status.code(), Some(1), "{}", printed(&output));
    assert!(
        stderr(&output).contains("context 'home' already exists\n  hint: remove it first with `fleetring context remove home`"),
        "{}",
        printed(&output)
    );
    // Without a certificate to read the fleet from, `--fleet` is asked for.
    let output = machine.run(&["context", "add", "other", "--control", "y:1"])?;
    assert_eq!(output.status.code(), Some(1), "{}", printed(&output));
    assert!(
        stderr(&output).contains("hint: pass --fleet <name>"),
        "{}",
        printed(&output)
    );
    Ok(())
}
