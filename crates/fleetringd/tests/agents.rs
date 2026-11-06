//! FR-079 (#114): an agent boots, and acquires a node and an identity for itself (ADR-0043,
//! point 8): `fleetring mcp --agent <id>`, `fleetring agent …`, `--agent` on other commands, and
//! the refusals that keep minting to dev mode and dotted agent names to delegation.
//!
//! Each test runs the real binary with a home, a configuration directory and a data directory of
//! its own. Most seed the dev directory's configuration with ports of their own, which a first
//! start then completes; the clean-machine test uses the default port when it is free. A machine
//! kills the node it started when it is dropped, whatever the test did, so no test leaves a node
//! running.
#![cfg(unix)]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use fleetringd::DataDir;
use fleetringd::upgrade::ProcessFile;
use rustix::process::{Pid, Signal, kill_process, test_kill_process};
use serde_json::{Value, json};

use crate::support::mcp::Session;

mod support;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

/// How long to wait between checks, and how many to make: about 30 seconds.
const POLL: Duration = Duration::from_millis(50);
const ATTEMPTS: u32 = 600;

/// The files an identity holds, besides its lock (ADR-0043, point 8.3).
const IDENTITY_FILES: [&str; 5] = [
    "ca.crt",
    "client.agent-42.crt",
    "client.agent-42.key",
    "token",
    "identity.toml",
];

/// What every start of `mcp --agent` and `agent add` says about isolation.
const TRUSTED_LOCAL: &str =
    "trusted local development: agents on this machine can read each other's credentials";

/// The next port [`port_pair`] tries.
static NEXT_PORT: AtomicU16 = AtomicU16::new(0);

/// A port that is free with the one after it, for a dev node and its admin listener, from a
/// range of this binary's own (`tests/dev_detach.rs` does the same from 12000).
fn port_pair() -> TestResult<u16> {
    let start = 16_000 + u16::try_from(std::process::id() % 200)? * 20;
    let _ = NEXT_PORT.compare_exchange(0, start, Ordering::SeqCst, Ordering::SeqCst);
    for _ in 0..1_000 {
        let port = NEXT_PORT.fetch_add(2, Ordering::SeqCst);
        if free(port) && free(port + 1) {
            return Ok(port);
        }
    }
    Err("no free pair of ports".into())
}

fn free(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
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

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn pid_of(pid: u32) -> TestResult<Pid> {
    Ok(Pid::from_raw(i32::try_from(pid)?).ok_or("pid 0")?)
}

/// Whether the process `pid` has exited: it can't be signalled, or it is a zombie.
fn gone(pid: u32) -> TestResult<bool> {
    if test_kill_process(pid_of(pid)?).is_err() {
        return Ok(true);
    }
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

/// Every file under `dir`, with its bytes, so a test can tell that nothing was written.
fn snapshot(dir: &Path) -> TestResult<BTreeMap<PathBuf, Vec<u8>>> {
    let mut found = BTreeMap::new();
    if !dir.exists() {
        return Ok(found);
    }
    let mut pending = vec![dir.to_owned()];
    while let Some(next) = pending.pop() {
        for entry in fs::read_dir(&next)? {
            let path = entry?.path();
            if path.is_dir() {
                found.insert(path.clone(), Vec::new());
                pending.push(path);
            } else {
                let bytes = fs::read(&path)?;
                found.insert(path, bytes);
            }
        }
    }
    Ok(found)
}

fn mode(path: &Path) -> TestResult<u32> {
    Ok(fs::metadata(path)?.permissions().mode() & 0o777)
}

/// A scratch machine: a home whose `~/.fleetring` is the dev directory, with its own
/// configuration and data directories, and the port its dev node takes.
struct Machine {
    root: tempfile::TempDir,
    /// The port, which a first start that loses it to another process replaces.
    port: Cell<u16>,
}

impl Machine {
    /// A machine whose dev directory holds only a configuration with ports of its own: the
    /// first start creates the authority and the certificates, as on a clean machine.
    fn new() -> TestResult<Self> {
        let machine = Self {
            root: tempfile::tempdir()?,
            port: Cell::new(port_pair()?),
        };
        machine.seed()?;
        Ok(machine)
    }

    /// A clean machine: an empty home, on dev mode's default port, if it and the next one are
    /// free; otherwise a machine as [`new`](Self::new) makes, so the test still runs.
    fn clean() -> TestResult<Self> {
        if free(7400) && free(7401) {
            return Ok(Self {
                root: tempfile::tempdir()?,
                port: Cell::new(7400),
            });
        }
        Self::new()
    }

    /// Writes the dev directory's configuration, as `fleetring dev` would write it, on this
    /// machine's ports.
    fn seed(&self) -> TestResult {
        let dir = self.dev_dir();
        fs::create_dir_all(&dir)?;
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
        let tls = dir.join("tls");
        let config = format!(
            "dev = true\ndata_dir = {:?}\nlisten = \"127.0.0.1:{port}\"\nrole = \"both\"\n\n\
             [node]\nid = \"dev\"\n\n[membership]\non_shutdown = \"stay\"\n\n\
             [replication]\nreplication_factor = 1\n\n\
             [tls]\nca = {:?}\ncert = {:?}\nkey = {:?}\n\n\
             [[principal]]\nname = \"admin\"\nscopes = [\"admin:*\", \"stream:*\", \"fleet\"]\n\n\
             [admin]\nlisten = \"127.0.0.1:{admin}\"\n",
            dir.join("data"),
            tls.join("ca.crt"),
            tls.join("node.dev.crt"),
            tls.join("node.dev.key"),
            port = self.port(),
            admin = self.port() + 1,
        );
        fs::write(dir.join("fleetringd.toml"), config)?;
        Ok(())
    }

    fn port(&self) -> u16 {
        self.port.get()
    }

    /// Whether a failed first start lost its port to another process, so that it may start
    /// again on another: no node runs, and the failure says the address was in use.
    fn lost_port(&self, failure: &str) -> TestResult<bool> {
        Ok(failure.contains("Address already in use") && self.process_file()?.is_none())
    }

    /// Moves the dev directory's configuration to a new pair of ports.
    fn reseed(&self) -> TestResult {
        self.port.set(port_pair()?);
        self.seed()
    }

    fn home(&self) -> &Path {
        self.root.path()
    }

    fn dev_dir(&self) -> PathBuf {
        self.home().join(".fleetring")
    }

    fn data_dir(&self) -> PathBuf {
        self.dev_dir().join("data")
    }

    fn agent_dir(&self, id: &str) -> PathBuf {
        self.dev_dir().join("agents").join(id)
    }

    fn config_dir(&self) -> PathBuf {
        self.home().join("xdg").join("fleetring")
    }

    fn data_home(&self) -> PathBuf {
        self.home().join("xdg-data")
    }

    /// `fleetring` with `args`, whose home, contexts and agents are this machine's.
    fn command(&self, args: &[&str]) -> Command {
        let mut command = support::fleetring();
        command
            .args(args)
            .env("HOME", self.home())
            .env("USERPROFILE", self.home())
            .env("XDG_CONFIG_HOME", self.home().join("xdg"))
            .env("XDG_DATA_HOME", self.data_home())
            .stdin(Stdio::null());
        command
    }

    /// Runs `args`; a command that starts the dev node and loses its port runs again on
    /// another, up to five times.
    fn run(&self, args: &[&str]) -> TestResult<Output> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let output = self.command(args).output()?;
            if output.status.success() || attempts >= 5 || !self.lost_port(&stderr(&output))? {
                return Ok(output);
            }
            self.reseed()?;
        }
    }

    /// Runs `args`, which must succeed, and returns stdout.
    fn ok(&self, args: &[&str]) -> TestResult<String> {
        let output = self.run(args)?;
        assert!(output.status.success(), "{args:?}: {}", printed(&output));
        Ok(stdout(&output))
    }

    /// An MCP session of `fleetring mcp` with `args`.
    async fn mcp(&self, args: &[&str]) -> TestResult<Session> {
        let mut command = vec!["mcp"];
        command.extend_from_slice(args);
        let mut attempts = 0;
        loop {
            attempts += 1;
            match Session::spawn(tokio::process::Command::from(self.command(&command))).await {
                Err(error) if attempts < 5 && self.lost_port(&error.to_string())? => {
                    self.reseed()?;
                }
                started => return started,
            }
        }
    }

    fn process_file(&self) -> TestResult<Option<ProcessFile>> {
        Ok(ProcessFile::read(&self.data_dir())?)
    }

    fn record(&self, id: &str) -> TestResult<toml::Table> {
        Ok(fs::read_to_string(self.agent_dir(id).join("identity.toml"))?.parse()?)
    }

    /// The agent's token, as its file holds it.
    fn token(&self, id: &str) -> TestResult<String> {
        Ok(fs::read_to_string(self.agent_dir(id).join("token"))?
            .trim()
            .to_owned())
    }

    /// Writes the contexts file.
    fn contexts(&self, text: &str) -> TestResult {
        fs::create_dir_all(self.config_dir())?;
        fs::write(self.config_dir().join("contexts.toml"), text)?;
        Ok(())
    }
}

impl Drop for Machine {
    /// Kills the dev node, if one still runs, and waits until it has gone.
    fn drop(&mut self) {
        let Ok(Some(file)) = self.process_file() else {
            return;
        };
        if file.pid == std::process::id() {
            return;
        }
        let Ok(pid) = pid_of(file.pid) else {
            return;
        };
        let _ = kill_process(pid, Signal::KILL);
        let _ = wait_until("the node is killed", || gone(file.pid));
    }
}

/// `db list` as the agent's token alone sees: `token` presented with `agent_dir`'s certificate.
fn list_with_token(machine: &Machine, cert_dir: &Path, token: &str) -> TestResult<Output> {
    let dir = cert_dir.to_str().ok_or("not UTF-8")?;
    machine.run(&[
        "db",
        "list",
        "--tls-dir",
        dir,
        "--as",
        "agent-42",
        "--token",
        token,
    ])
}

/// Waits until the node refuses `token` as revoked.
fn wait_until_revoked(machine: &Machine, cert_dir: &Path, token: &str) -> TestResult {
    wait_until("the old token is refused as revoked", || {
        let output = list_with_token(machine, cert_dir, token)?;
        Ok(!output.status.success() && stderr(&output).contains("revoked"))
    })
}

/// A copy of the agent's certificate, key and `ca.crt`, to present a token with after the
/// directory has changed or gone.
fn copy_certificate(machine: &Machine, id: &str) -> TestResult<tempfile::TempDir> {
    let copy = tempfile::tempdir()?;
    for name in [
        "ca.crt",
        &format!("client.agent-{id}.crt"),
        &format!("client.agent-{id}.key"),
    ] {
        fs::copy(machine.agent_dir(id).join(name), copy.path().join(name))?;
    }
    Ok(copy)
}

/// Splits a line as a POSIX shell would split the words `--print-config claude` prints: plain
/// words and single-quoted ones.
fn shell_words(line: &str) -> TestResult<Vec<String>> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quoted = false;
    let mut started = false;
    for character in line.chars() {
        match (character, quoted) {
            ('\'', _) => {
                quoted = !quoted;
                started = true;
            }
            (' ', false) => {
                if started {
                    words.push(std::mem::take(&mut word));
                    started = false;
                }
            }
            (other, _) => {
                word.push(other);
                started = true;
            }
        }
    }
    if quoted {
        return Err(format!("an unclosed quote in {line}").into());
    }
    if started {
        words.push(word);
    }
    Ok(words)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_agent_boots_a_node_and_an_identity() -> TestResult {
    let machine = Machine::clean()?;
    assert!(machine.process_file()?.is_none(), "no node runs yet");
    let mut session = machine.mcp(&["--agent", "42"]).await?;
    let created = session
        .ok("db_create", json!({"name": "agent-42-memory"}))
        .await?;
    assert_eq!(created["database"], "agent-42-memory", "{created}");
    let refused = session
        .failed("db_create", json!({"name": "agent-43-x"}))
        .await?;
    assert_eq!(refused["code"], "permission_denied", "{refused}");
    let log = session.close().await?;
    assert!(
        log.contains("serving MCP as agent-42 on context 'dev'; certificate expires "),
        "{log}"
    );
    assert!(
        log.contains("(renewed at start when under 7 days)"),
        "{log}"
    );
    assert!(log.contains(TRUSTED_LOCAL), "{log}");
    assert!(log.contains("minted agent 42's identity"), "{log}");
    let token = machine.token("42")?;
    assert!(!log.contains(&token), "the log holds no token");

    // The identity: five private files in a private directory, and a lock.
    let dir = machine.agent_dir("42");
    assert_eq!(mode(&dir)?, 0o700);
    assert_eq!(mode(&machine.dev_dir().join("agents"))?, 0o700);
    for name in IDENTITY_FILES {
        assert_eq!(mode(&dir.join(name))?, 0o600, "{name}");
    }
    let mut entries: Vec<String> = fs::read_dir(&dir)?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_, _>>()?;
    entries.sort();
    let mut expected: Vec<String> = IDENTITY_FILES.iter().map(|&name| name.to_owned()).collect();
    expected.push(".lock".to_owned());
    expected.sort();
    assert_eq!(entries, expected);

    // The node outlives the server, and the `dev` context is current.
    assert!(machine.process_file()?.is_some(), "the dev node runs");
    assert_eq!(machine.ok(&["context", "current"])?, "dev\n");
    // A name under `_fleetring` is FleetRing's own, even for the admin.
    let reserved = machine.run(&["db", "create", "_fleetring-x"])?;
    assert_eq!(reserved.status.code(), Some(1), "{}", printed(&reserved));
    assert!(
        stderr(&reserved).contains("reserved"),
        "{}",
        printed(&reserved)
    );
    machine.ok(&["dev", "stop"])?;
    Ok(())
}

/// The configuration `--print-config` prints, and the quickstart's `claude mcp add` line, pin
/// `--context dev`, which the first start writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pinned_dev_context_works_before_it_exists() -> TestResult {
    let machine = Machine::new()?;
    assert!(!machine.config_dir().join("contexts.toml").exists());
    let mut session = machine.mcp(&["--agent", "42", "--context", "dev"]).await?;
    session
        .ok("db_create", json!({"name": "agent-42-notes"}))
        .await?;
    let log = session.close().await?;
    assert!(
        log.contains("serving MCP as agent-42 on context 'dev'"),
        "{log}"
    );
    assert!(machine.agent_dir("42").join("identity.toml").is_file());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_start_reuses_everything() -> TestResult {
    let machine = Machine::new()?;
    machine.mcp(&["--agent", "42"]).await?.close().await?;
    let pid = machine.process_file()?.ok_or("no node")?.pid;
    let before = snapshot(&machine.agent_dir("42"))?;
    let mut session = machine.mcp(&["--agent", "42"]).await?;
    session.ok("db_list", json!({})).await?;
    let log = session.close().await?;
    assert!(!log.contains("minted agent 42's identity"), "{log}");
    assert!(!log.contains("renewed agent 42's"), "{log}");
    assert!(!log.contains("started the dev node"), "{log}");
    assert_eq!(snapshot(&machine.agent_dir("42"))?, before);
    assert_eq!(machine.process_file()?.ok_or("no node")?.pid, pid);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expiring_token_is_renewed_in_dev_mode_only() -> TestResult {
    let machine = Machine::new()?;
    machine.mcp(&["--agent", "42"]).await?.close().await?;
    let old_token = machine.token("42")?;
    let old_record = machine.record("42")?;
    let old_id = old_record["token_id"]
        .as_str()
        .ok_or("no token_id")?
        .to_owned();
    let certificate = fs::read(machine.agent_dir("42").join("client.agent-42.crt"))?;
    let cert_dir = copy_certificate(&machine, "42")?;

    // Tomorrow is under 7 days away, so the next start renews the token, and only it.
    let tomorrow = time::OffsetDateTime::now_utc() + time::Duration::days(1);
    let tomorrow = tomorrow.format(&time::format_description::well_known::Rfc3339)?;
    let mut expiring = old_record.clone();
    expiring.insert("token_expires_at".to_owned(), tomorrow.clone().into());
    let record_file = machine.agent_dir("42").join("identity.toml");
    fs::write(&record_file, toml::to_string(&expiring)?)?;

    // Another context's identity is never renewed: the same identity under a context that
    // isn't dev mode's is used as it is.
    let prod_agents = machine
        .data_home()
        .join("fleetring")
        .join("agents")
        .join("prod");
    fs::create_dir_all(&prod_agents)?;
    let prod_copy = prod_agents.join("42");
    fs::create_dir_all(&prod_copy)?;
    for name in IDENTITY_FILES {
        fs::copy(machine.agent_dir("42").join(name), prod_copy.join(name))?;
    }
    let contexts = fs::read_to_string(machine.config_dir().join("contexts.toml"))?;
    machine.contexts(&format!(
        "{contexts}\n[contexts.prod]\nfleet = \"dev\"\ncontrol = [\"127.0.0.1:{}\"]\n",
        machine.port()
    ))?;
    let mut prod = machine.mcp(&["--agent", "42", "--context", "prod"]).await?;
    prod.ok("db_list", json!({})).await?;
    let log = prod.close().await?;
    assert!(!log.contains("renewed"), "{log}");
    assert!(
        log.contains("serving MCP as agent-42 on context 'prod'"),
        "{log}"
    );
    assert!(log.contains("agent renew 42 --authority"), "{log}");
    assert_eq!(
        fs::read_to_string(prod_copy.join("token"))?.trim(),
        old_token
    );

    // In dev mode, the start renews the token and revokes the old one.
    let mut session = machine.mcp(&["--agent", "42"]).await?;
    session.ok("db_list", json!({})).await?;
    let log = session.close().await?;
    assert!(log.contains("renewed agent 42's token"), "{log}");
    assert!(
        log.contains(&format!("revoked the old token {old_id}")),
        "{log}"
    );
    let record = machine.record("42")?;
    assert_ne!(record["token_id"].as_str(), Some(old_id.as_str()));
    assert_ne!(record["token_expires_at"].as_str(), Some(tomorrow.as_str()));
    assert_ne!(machine.token("42")?, old_token);
    assert_eq!(
        fs::read(machine.agent_dir("42").join("client.agent-42.crt"))?,
        certificate,
        "the certificate isn't due"
    );
    wait_until_revoked(&machine, cert_dir.path(), &old_token)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_context_pointing_dev_dir_at_a_non_dev_directory_mints_nothing() -> TestResult {
    let machine = Machine::new()?;
    // A real node's directory: another fleet's authority and a configuration without
    // `dev = true`, with a process that holds its data directory, so no start runs.
    let prod = machine.home().join("prod");
    let tls = prod.join("tls");
    let tls_dir = tls.to_str().ok_or("not UTF-8")?;
    machine.ok(&["cert", "init", "--fleet", "prod", "--dir", tls_dir])?;
    fs::write(
        prod.join("fleetringd.toml"),
        format!(
            "data_dir = {:?}\nlisten = \"127.0.0.1:{}\"\n\n[node]\nid = \"prod-1\"\n",
            prod.join("data"),
            machine.port()
        ),
    )?;
    let data = prod.join("data");
    let held = DataDir::lock(&data)?;
    ProcessFile::this_process(std::env::current_exe()?).write(&data)?;
    // The `dev` context, hand-edited to point there.
    machine.contexts(&format!(
        "version = 1\ncurrent = \"dev\"\n\n[contexts.dev]\nfleet = \"dev\"\n\
         control = [\"127.0.0.1:{}\"]\ntls_dir = {:?}\nas = \"admin\"\ndev_dir = {:?}\n",
        machine.port(),
        tls,
        prod
    ))?;
    let before = snapshot(&prod)?;
    let output = machine.run(&["mcp", "--agent", "42"])?;
    assert_eq!(output.status.code(), Some(1), "{}", printed(&output));
    assert!(
        stderr(&output).contains("isn't a dev mode configuration"),
        "the error names the check: {}",
        printed(&output)
    );
    assert!(output.stdout.is_empty(), "{}", printed(&output));
    assert_eq!(
        snapshot(&prod)?,
        before,
        "nothing was written under {prod:?}"
    );
    assert!(!machine.dev_dir().join("agents").exists());
    drop(held);
    Ok(())
}

#[test]
fn outside_dev_mode_nothing_is_minted() -> TestResult {
    let machine = Machine::new()?;
    machine.contexts(
        "version = 1\ncurrent = \"prod\"\n\n[contexts.prod]\nfleet = \"prod\"\n\
         control = [\"127.0.0.1:1\"]\ntls_dir = \"/nonexistent/tls\"\nas = \"ops\"\n",
    )?;
    let before = snapshot(machine.home())?;
    let output = machine.run(&["mcp", "--agent", "42"])?;
    assert_eq!(output.status.code(), Some(1), "{}", printed(&output));
    let text = stderr(&output);
    assert!(
        text.contains("agent '42' has no identity in "),
        "{}",
        printed(&output)
    );
    assert!(
        text.contains("fleetring agent add 42 --context prod --authority"),
        "{}",
        printed(&output)
    );
    assert_eq!(snapshot(machine.home())?, before, "nothing was written");
    // `agent add` outside dev mode needs the authority, and mints nothing without it.
    let refused = machine.run(&["agent", "add", "42"])?;
    assert_eq!(refused.status.code(), Some(1), "{}", printed(&refused));
    assert!(
        stderr(&refused).contains("--authority"),
        "{}",
        printed(&refused)
    );
    assert_eq!(snapshot(machine.home())?, before, "nothing was written");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_starts_at_once_share_one_node_and_one_identity() -> TestResult {
    let machine = Machine::new()?;
    let (first, second) = tokio::join!(
        machine.mcp(&["--agent", "42"]),
        machine.mcp(&["--agent", "42"])
    );
    let (mut first, mut second) = (first?, second?);
    first.ok("db_list", json!({})).await?;
    second.ok("db_list", json!({})).await?;
    let logs = [first.close().await?, second.close().await?];
    let minted = logs
        .iter()
        .filter(|log| log.contains("minted agent 42's identity"))
        .count();
    assert_eq!(minted, 1, "{logs:?}");
    let started = logs
        .iter()
        .filter(|log| log.contains("started the dev node"))
        .count();
    assert_eq!(started, 1, "{logs:?}");
    assert!(machine.process_file()?.is_some());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn print_config_runs_the_agent() -> TestResult {
    let machine = Machine::new()?;
    let output = machine.run(&["agent", "add", "42", "--print-config", "claude"])?;
    assert!(output.status.success(), "{}", printed(&output));
    let line = stdout(&output);
    let token = machine.token("42")?;
    assert!(!line.contains(&token), "no token in the configuration");
    assert!(
        stderr(&output).contains(TRUSTED_LOCAL),
        "{}",
        printed(&output)
    );
    assert!(!stderr(&output).contains(&token), "no token on stderr");
    let words = shell_words(line.trim_end())?;
    assert_eq!(words[..4], ["claude", "mcp", "add", "fleetring"], "{line}");
    assert_eq!(words[4], "--", "{line}");
    let (exe, args) = (&words[5], &words[6..]);
    assert!(Path::new(exe).is_absolute(), "{line}");
    assert_eq!(args, ["mcp", "--agent", "42", "--context", "dev"], "{line}");

    // The printed command serves, run as an MCP client would run it.
    let mut command = Command::new(exe);
    support::isolate(&mut command);
    command
        .args(args)
        .env("HOME", machine.home())
        .env("XDG_CONFIG_HOME", machine.home().join("xdg"))
        .env("XDG_DATA_HOME", machine.data_home());
    let mut session = Session::spawn(tokio::process::Command::from(command)).await?;
    session.ok("db_list", json!({})).await?;
    session.close().await?;

    // `json` holds the same command, and nothing secret either.
    let output = machine.run(&["agent", "add", "43", "--print-config", "json"])?;
    assert!(output.status.success(), "{}", printed(&output));
    let config: Value = serde_json::from_str(&stdout(&output))?;
    let server = &config["mcpServers"]["fleetring"];
    assert_eq!(server["command"], json!(exe), "{config}");
    assert_eq!(
        server["args"],
        json!(["mcp", "--agent", "43", "--context", "dev"])
    );
    assert!(server.get("env").is_none(), "{config}");
    assert!(!stdout(&output).contains(&machine.token("43")?));
    // A second add is refused, and names `renew`.
    let again = machine.run(&["agent", "add", "42"])?;
    assert_eq!(again.status.code(), Some(1), "{}", printed(&again));
    assert!(
        stderr(&again).contains("fleetring agent renew 42"),
        "{}",
        printed(&again)
    );
    Ok(())
}

#[test]
fn agent_ids_with_a_dash_are_refused() -> TestResult {
    let machine = Machine::new()?;
    for args in [
        &["agent", "add", "4-2"][..],
        &["agent", "add", "42.c7"],
        &["mcp", "--agent", "4-2"],
        &["db", "list", "--agent", "Agent"],
    ] {
        let output = machine.run(args)?;
        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}: {}",
            printed(&output)
        );
        assert!(
            stderr(&output).contains("invalid agent ID"),
            "{args:?}: {}",
            printed(&output)
        );
    }
    let variable = machine
        .command(&["db", "list"])
        .env("FLEETRING_AGENT", "4-2")
        .output()?;
    assert_eq!(variable.status.code(), Some(1), "{}", printed(&variable));
    assert!(stderr(&variable).contains("invalid agent ID"));
    assert!(!machine.dev_dir().join("agents").exists());
    assert!(
        !machine.dev_dir().join("tls").exists(),
        "nothing was minted"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identity_toml_records_owner_and_no_parent() -> TestResult {
    let machine = Machine::new()?;
    machine.mcp(&["--agent", "42"]).await?.close().await?;
    let text = fs::read_to_string(machine.agent_dir("42").join("identity.toml"))?;
    let record = machine.record("42")?;
    assert_eq!(record["version"].as_integer(), Some(1), "{text}");
    assert_eq!(record["principal"].as_str(), Some("agent-42"), "{text}");
    assert_eq!(record["owner"].as_str(), Some("admin"), "{text}");
    assert!(record.get("parent").is_none(), "{text}");
    assert_eq!(record["fleet"].as_str(), Some("dev"), "{text}");
    assert_eq!(record["issued_by"].as_str(), Some("dev"), "{text}");
    assert_eq!(record["read_only"].as_bool(), Some(false), "{text}");
    let scopes: Vec<&str> = record["scopes"]
        .as_array()
        .ok_or("no scopes")?
        .iter()
        .filter_map(toml::Value::as_str)
        .collect();
    assert_eq!(scopes, ["admin:agent-42-*"], "{text}");
    assert!(!text.contains(&machine.token("42")?), "no token in {text}");

    // A record a later release wrote is refused with a message that says to upgrade.
    fs::write(
        machine.agent_dir("42").join("identity.toml"),
        text.replace(
            "version = 1\n",
            "version = 2\nholder_key = \"holder.key\"\n",
        ),
    )?;
    let newer = machine.run(&["db", "list", "--agent", "42"])?;
    assert_eq!(newer.status.code(), Some(1), "{}", printed(&newer));
    assert!(
        stderr(&newer).contains("this identity was written by a newer fleetring; upgrade"),
        "{}",
        printed(&newer)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_remove_revokes_and_deletes() -> TestResult {
    let machine = Machine::new()?;
    machine.mcp(&["--agent", "42"]).await?.close().await?;
    let token = machine.token("42")?;
    let cert_dir = copy_certificate(&machine, "42")?;
    // Without a terminal, it asks for --yes and keeps everything.
    let refused = machine.run(&["agent", "remove", "42"])?;
    assert_eq!(refused.status.code(), Some(1), "{}", printed(&refused));
    assert!(stderr(&refused).contains("--yes"), "{}", printed(&refused));
    assert!(machine.agent_dir("42").exists());

    let removed = machine.ok(&["agent", "remove", "42", "--yes"])?;
    assert!(
        removed.starts_with("removed agent 42: revoked token "),
        "{removed}"
    );
    assert!(!machine.agent_dir("42").exists());
    wait_until_revoked(&machine, cert_dir.path(), &token)?;

    // With the fleet out of reach, it refuses unless told not to revoke.
    machine.ok(&["agent", "add", "43"])?;
    machine.ok(&["dev", "stop"])?;
    let unreachable = machine.run(&["agent", "remove", "43", "--yes"])?;
    assert_eq!(
        unreachable.status.code(),
        Some(1),
        "{}",
        printed(&unreachable)
    );
    assert!(
        stderr(&unreachable).contains("nothing was removed"),
        "{}",
        printed(&unreachable)
    );
    assert!(machine.agent_dir("43").exists());
    let forced = machine.run(&["agent", "remove", "43", "--yes", "--no-revoke"])?;
    assert!(forced.status.success(), "{}", printed(&forced));
    assert!(
        stderr(&forced).contains("stays valid until"),
        "{}",
        printed(&forced)
    );
    assert!(!machine.agent_dir("43").exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn db_list_with_agent_uses_its_scopes() -> TestResult {
    let machine = Machine::new()?;
    let mut session = machine.mcp(&["--agent", "42"]).await?;
    session
        .ok("db_create", json!({"name": "agent-42-notes"}))
        .await?;
    session.close().await?;
    machine.ok(&["db", "create", "operators-notes"])?;

    let listed = machine.ok(&["db", "list", "--agent", "42"])?;
    assert!(listed.contains("agent-42-notes"), "{listed}");
    assert!(!listed.contains("operators-notes"), "{listed}");
    let from_variable = machine
        .command(&["db", "list"])
        .env("FLEETRING_AGENT", "42")
        .output()?;
    assert!(
        from_variable.status.success(),
        "{}",
        printed(&from_variable)
    );
    assert_eq!(stdout(&from_variable), listed);
    let everything = machine.ok(&["db", "list"])?;
    assert!(everything.contains("operators-notes"), "{everything}");

    let outside = machine.run(&["db", "create", "agent-43-x", "--agent", "42"])?;
    assert_eq!(outside.status.code(), Some(1), "{}", printed(&outside));
    assert!(
        stderr(&outside).to_lowercase().contains("permission"),
        "{}",
        printed(&outside)
    );
    // `--agent` only uses an identity that exists; it mints none.
    let missing = machine.run(&["db", "list", "--agent", "7"])?;
    assert_eq!(missing.status.code(), Some(1), "{}", printed(&missing));
    assert!(
        stderr(&missing).contains("agent '7' has no identity in"),
        "{}",
        printed(&missing)
    );
    assert!(!machine.agent_dir("7").exists());

    // `agent list` and `show` read the records.
    let list = machine.ok(&["agent", "list"])?;
    assert!(list.starts_with("ID"), "{list}");
    assert!(list.contains("agent-42"), "{list}");
    let json: Value = serde_json::from_str(&machine.ok(&["agent", "list", "-o", "json"])?)?;
    assert_eq!(json["agents"][0]["id"], "42", "{json}");
    assert_eq!(json["agents"][0]["scopes"], json!(["admin:agent-42-*"]));
    let shown = machine.ok(&["agent", "show", "42"])?;
    assert!(shown.contains("renew by"), "{shown}");
    assert!(shown.contains("mcp --agent 42 --context dev"), "{shown}");
    assert!(shown.contains(TRUSTED_LOCAL), "{shown}");
    assert!(!shown.contains(&machine.token("42")?), "{shown}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_only_agent_serves_only_the_reading_tools() -> TestResult {
    let machine = Machine::new()?;
    let added = machine.ok(&["agent", "add", "42", "--read-only"])?;
    assert!(added.contains("read:agent-42-*"), "{added}");
    let mut session = machine.mcp(&["--agent", "42"]).await?;
    let mut tools: Vec<String> = session
        .tools()
        .await?
        .iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
        .collect();
    tools.sort();
    assert_eq!(tools, ["db_list", "db_show", "query"]);
    session.close().await?;
    Ok(())
}

#[test]
fn cert_client_and_token_issue_refuse_reserved_dotted_agents() -> TestResult {
    let machine = Machine::new()?;
    let tls = machine.home().join("prod-tls");
    let dir = tls.to_str().ok_or("not UTF-8")?;
    machine.ok(&["cert", "init", "--fleet", "prod", "--dir", dir])?;
    let before = snapshot(&tls)?;
    for args in [
        &["cert", "client", "agent-42.c7", "--dir", dir][..],
        &[
            "token",
            "issue",
            "--principal",
            "agent-42.c7",
            "--scope",
            "read:x",
            "--dir",
            dir,
        ],
    ] {
        let output = machine.run(args)?;
        assert_eq!(
            output.status.code(),
            Some(1),
            "{args:?}: {}",
            printed(&output)
        );
        assert!(
            stderr(&output).contains("'agent-42.c7' is reserved"),
            "{args:?}: {}",
            printed(&output)
        );
        assert!(output.stdout.is_empty(), "{}", printed(&output));
    }
    assert_eq!(snapshot(&tls)?, before);
    // A dotted service name is still issued.
    machine.ok(&["cert", "client", "workload.prod", "--dir", dir])?;
    Ok(())
}

#[test]
fn token_issue_refuses_fork_and_grant() -> TestResult {
    let machine = Machine::new()?;
    let tls = machine.home().join("prod-tls");
    let dir = tls.to_str().ok_or("not UTF-8")?;
    machine.ok(&["cert", "init", "--fleet", "prod", "--dir", dir])?;
    for (scope, word) in [
        ("fork", "fork"),
        ("grant:agent-42-*", "grant"),
        ("fork:x", "fork"),
    ] {
        let output = machine.run(&[
            "token",
            "issue",
            "--principal",
            "agent-42",
            "--scope",
            scope,
            "--dir",
            dir,
        ])?;
        assert_eq!(
            output.status.code(),
            Some(2),
            "{scope}: {}",
            printed(&output)
        );
        assert!(
            stderr(&output).contains(&format!(
                "'{word}' is reserved for delegation, which this release doesn't support"
            )),
            "{scope}: {}",
            printed(&output)
        );
    }
    let refused = machine.run(&["agent", "add", "42", "--scope", "grant:agent-42.*"])?;
    assert_eq!(refused.status.code(), Some(2), "{}", printed(&refused));
    Ok(())
}

#[test]
fn cert_client_names_the_recovery_for_a_missing_ca_crt() -> TestResult {
    let machine = Machine::new()?;
    let tls = machine.home().join("prod-tls");
    let dir = tls.to_str().ok_or("not UTF-8")?;
    machine.ok(&["cert", "init", "--fleet", "prod", "--dir", dir])?;
    fs::remove_file(tls.join("ca.crt"))?;
    for args in [
        &["cert", "client", "ops", "--dir", dir][..],
        &[
            "cert",
            "node",
            "n1",
            "--addresses",
            "127.0.0.1",
            "--dir",
            dir,
        ],
        &[
            "token",
            "issue",
            "--principal",
            "ops",
            "--scope",
            "read:x",
            "--dir",
            dir,
        ],
        &["agent", "add", "42", "--authority", dir],
    ] {
        let output = machine.run(args)?;
        assert_eq!(
            output.status.code(),
            Some(1),
            "{args:?}: {}",
            printed(&output)
        );
        let text = stderr(&output);
        assert!(
            text.contains(&format!("{dir} holds ca.key but not ca.crt")),
            "{args:?}: {text}"
        );
        assert!(
            text.contains("hint: copy ca.crt back from a node's [tls] ca file or a backup"),
            "{args:?}: {text}"
        );
    }
    Ok(())
}

#[test]
fn cert_client_out_writes_a_complete_private_directory() -> TestResult {
    let machine = Machine::new()?;
    let tls = machine.home().join("prod-tls");
    let dir = tls.to_str().ok_or("not UTF-8")?;
    machine.ok(&["cert", "init", "--fleet", "prod", "--dir", dir])?;
    let before = snapshot(&tls)?;
    let out = machine.home().join("ops");
    let out_dir = out.to_str().ok_or("not UTF-8")?;
    let printed_out = machine.ok(&["cert", "client", "ops", "--dir", dir, "--out", out_dir])?;
    assert!(printed_out.contains("a copy of ca.crt"), "{printed_out}");
    assert_eq!(
        snapshot(&tls)?,
        before,
        "nothing lands in the authority's directory"
    );
    assert_eq!(mode(&out)?, 0o700);
    assert_eq!(mode(&out.join("client.ops.key"))?, 0o600);
    assert_eq!(fs::read(out.join("ca.crt"))?, fs::read(tls.join("ca.crt"))?);
    assert!(out.join("client.ops.crt").is_file());
    Ok(())
}
