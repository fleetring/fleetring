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
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use fleetring_control::{Action, ControlConfig, Feature, Member, Release, Software};
use fleetringd::DataDir;
use fleetringd::auth::{AuthLayer, Authorizer};
use fleetringd::clock::SystemClock;
use fleetringd::control::{ControlNode, ControlOptions, ControlService, StorageIo};
use fleetringd::transport::control_server;
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

/// FR-081's defaults for agent `id`, whose access scope is `access`'s: what a fleet whose
/// members don't all report `delegation` mints.
fn fr081_defaults(id: &str, access: &str) -> Vec<String> {
    vec![
        format!("{access}:agent-{id}-*"),
        format!("stream:agent-{id}-*"),
    ]
}

/// This release's defaults for agent `id`, once every member reports `delegation`: `access`,
/// `stream`, and `grant`, each on `agent-<id>-*` and `agent-<id>.*`.
fn delegating_defaults(id: &str, access: &str) -> Vec<String> {
    let mut scopes = Vec::new();
    for action in [access, "stream", "grant"] {
        for separator in ['-', '.'] {
            scopes.push(format!("{action}:agent-{id}{separator}*"));
        }
    }
    scopes
}

/// The defaults dev mode mints agent `id` with: a dev node reports `delegation`, so its root
/// agents can delegate (ADR-0047, point 2.6).
fn dev_defaults(id: &str, access: &str) -> Vec<String> {
    delegating_defaults(id, access)
}

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
    /// machine's ports, with its change sink that records every database.
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
             [admin]\nlisten = \"127.0.0.1:{admin}\"\n\n\
             [[changes.sink]]\nname = \"dev\"\nkind = \"file\"\ndatabases = [\"*\"]\npath = {:?}\n",
            dir.join("data"),
            tls.join("ca.crt"),
            tls.join("node.dev.crt"),
            tls.join("node.dev.key"),
            dir.join("changes"),
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

    /// Where agent `id`'s identity lives for the context `context`, which isn't dev mode's.
    fn context_agent_dir(&self, context: &str, id: &str) -> PathBuf {
        self.data_home()
            .join("fleetring")
            .join("agents")
            .join(context)
            .join(id)
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

/// A token's claims, read without verifying it.
fn claims(token: &str) -> TestResult<Value> {
    use base64::Engine as _;
    let payload = token.split('.').nth(1).ok_or("not a token")?;
    let json = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload)?;
    Ok(serde_json::from_slice(&json)?)
}

/// A token's ID, its `jti` claim.
fn token_id(token: &str) -> TestResult<String> {
    Ok(claims(token)?["jti"].as_str().ok_or("no jti")?.to_owned())
}

/// When a token expires, its `exp` claim, in RFC 3339 as the commands print times.
fn token_expiry(token: &str) -> TestResult<String> {
    let exp = claims(token)?["exp"].as_i64().ok_or("no exp")?;
    Ok(time::OffsetDateTime::from_unix_timestamp(exp)?
        .format(&time::format_description::well_known::Rfc3339)?)
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

    // In dev mode, the start renews the token and revokes the old one until the old token
    // itself expires, not until the record's tomorrow.
    let mut session = machine.mcp(&["--agent", "42"]).await?;
    session.ok("db_list", json!({})).await?;
    let log = session.close().await?;
    assert!(log.contains("renewed agent 42's token"), "{log}");
    assert!(
        log.contains(&format!(
            "revoked token {old_id} until {}",
            token_expiry(&old_token)?
        )),
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
    assert_eq!(scopes, dev_defaults("42", "admin"), "{text}");
    // The defaults need no field a 0.7 reader lacks.
    assert!(!text.contains("scopes_from"), "{text}");
    assert!(!text.contains(&machine.token("42")?), "no token in {text}");

    // A record a later release wrote is refused with a message that says to upgrade.
    fs::write(
        machine.agent_dir("42").join("identity.toml"),
        text.replace(
            "version = 1\n",
            "version = 3\nholder_key = \"holder.key\"\n",
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
    // Without a terminal, it asks for --yes, a usage error, and keeps everything.
    let refused = machine.run(&["agent", "remove", "42"])?;
    assert_eq!(refused.status.code(), Some(2), "{}", printed(&refused));
    assert!(stderr(&refused).contains("--yes"), "{}", printed(&refused));
    assert!(machine.agent_dir("42").exists());

    // The record is no authority on when the token ends: one edited to say it ends in a minute
    // still gets a revocation that lasts as long as the token itself.
    let mut record = machine.record("42")?;
    let soon = time::OffsetDateTime::now_utc() + time::Duration::minutes(1);
    record.insert(
        "token_expires_at".to_owned(),
        soon.format(&time::format_description::well_known::Rfc3339)?
            .into(),
    );
    fs::write(
        machine.agent_dir("42").join("identity.toml"),
        toml::to_string(&record)?,
    )?;
    let removed = machine.ok(&["agent", "remove", "42", "--yes"])?;
    let id = token_id(&token)?;
    // The agent revokes itself, its principal and its token, for at least the token's life.
    let prefix = format!(
        "removed agent 42: revoked agent-42 and every agent below it, and token {id} until "
    );
    assert!(removed.starts_with(&prefix), "{removed}");
    let until = removed[prefix.len()..].get(..19).ok_or("no time")?;
    assert!(until >= &token_expiry(&token)?[..19], "{removed}");
    assert!(
        removed.contains("  a new token for agent-42 may be minted from "),
        "{removed}"
    );
    assert!(!machine.agent_dir("42").exists());
    wait_until_revoked(&machine, cert_dir.path(), &token)?;

    // Renewing or removing an agent that has no identity creates nothing.
    for args in [
        &["agent", "renew", "9"][..],
        &["agent", "remove", "9", "--yes"],
    ] {
        let missing = machine.run(args)?;
        assert_eq!(missing.status.code(), Some(1), "{}", printed(&missing));
        assert!(
            stderr(&missing).contains("agent '9' has no identity"),
            "{}",
            printed(&missing)
        );
        assert!(!machine.agent_dir("9").exists(), "{args:?}");
    }

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
    // A refusal exits 3.
    assert_eq!(outside.status.code(), Some(3), "{}", printed(&outside));
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
    let defaults = dev_defaults("42", "admin");
    assert_eq!(json["agents"][0]["scopes"], json!(defaults));
    let shown = machine.ok(&["agent", "show", "42"])?;
    assert!(
        shown.contains(&format!("scopes       {}\n", defaults.join(", "))),
        "{shown}"
    );
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
    assert!(
        added.contains(&dev_defaults("42", "read").join(", ")),
        "{added}"
    );
    let mut session = machine.mcp(&["--agent", "42"]).await?;
    assert_eq!(tool_names(&mut session).await?, READING_TOOLS);
    // It follows the changes of its databases, which the operator writes: following is
    // reading.
    machine.ok(&["db", "create", "agent-42-feed"])?;
    machine.ok(&[
        "db",
        "exec",
        "agent-42-feed",
        "CREATE TABLE item (id INTEGER PRIMARY KEY)",
    ])?;
    let from = session
        .ok("changes", json!({"database": "agent-42-feed"}))
        .await?;
    machine.ok(&[
        "db",
        "exec",
        "agent-42-feed",
        "INSERT INTO item (id) VALUES (7)",
    ])?;
    let read = session
        .ok(
            "changes",
            json!({"database": "agent-42-feed", "after": from["next_after"]}),
        )
        .await?;
    assert_eq!(
        read["transactions"][0]["changes"][0]["key"],
        json!([{"name": "id", "value": 7}]),
        "{read}"
    );
    session.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fresh_agent_follows_its_own_writes_and_no_other_agents() -> TestResult {
    let machine = Machine::new()?;
    let mut session = machine.mcp(&["--agent", "42"]).await?;
    session
        .ok("db_create", json!({"name": "agent-42-memory"}))
        .await?;
    session
        .ok(
            "exec",
            json!({"database": "agent-42-memory", "sql": "CREATE TABLE fact (id INTEGER PRIMARY KEY, body TEXT)"}),
        )
        .await?;
    let from = session
        .ok("changes", json!({"database": "agent-42-memory"}))
        .await?;
    for id in [1, 2] {
        session
            .ok(
                "exec",
                json!({
                    "database": "agent-42-memory",
                    "sql": "INSERT INTO fact (id, body) VALUES (?, 'x')",
                    "params": [id],
                }),
            )
            .await?;
    }
    let read = session
        .ok(
            "changes",
            json!({"database": "agent-42-memory", "after": from["next_after"]}),
        )
        .await?;
    let keys: Vec<&Value> = read["transactions"]
        .as_array()
        .ok_or("no transactions")?
        .iter()
        .map(|transaction| &transaction["changes"][0]["key"][0]["value"])
        .collect();
    assert_eq!(keys, [&json!(1), &json!(2)], "{read}");

    // Another agent's database, recorded like every one in dev mode, is refused.
    machine.ok(&["db", "create", "agent-43-memory"])?;
    let refused = session
        .failed("changes", json!({"database": "agent-43-memory"}))
        .await?;
    assert_eq!(refused["code"], "permission_denied", "{refused}");
    session.close().await?;
    Ok(())
}

/// The tools a read-only server offers.
const READING_TOOLS: [&str; 4] = ["changes", "db_list", "db_show", "query"];

/// The names of the tools `session` offers, sorted.
async fn tool_names(session: &mut Session) -> TestResult<Vec<String>> {
    let mut tools: Vec<String> = session
        .tools()
        .await?
        .iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
        .collect();
    tools.sort();
    Ok(tools)
}

/// The scopes and `read_only` an identity records.
fn recorded_access(machine: &Machine, id: &str) -> TestResult<(Vec<String>, Option<bool>)> {
    let record = machine.record(id)?;
    let scopes = record["scopes"]
        .as_array()
        .ok_or("no scopes")?
        .iter()
        .filter_map(|scope| scope.as_str().map(str::to_owned))
        .collect();
    Ok((scopes, record["read_only"].as_bool()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_read_only_mints_a_reading_identity_and_changes_no_existing_one() -> TestResult {
    let machine = Machine::new()?;
    // A first start with --read-only mints a reading identity.
    let mut session = machine.mcp(&["--agent", "42", "--read-only"]).await?;
    assert_eq!(tool_names(&mut session).await?, READING_TOOLS);
    session.close().await?;
    let reading = (dev_defaults("42", "read"), Some(true));
    assert_eq!(recorded_access(&machine, "42")?, reading);
    // Later starts without it don't widen it.
    let mut session = machine.mcp(&["--agent", "42"]).await?;
    assert_eq!(tool_names(&mut session).await?, READING_TOOLS);
    session.close().await?;
    assert_eq!(recorded_access(&machine, "42")?, reading);

    // An identity that may write serves read-only for one start, and stays as it was.
    machine.mcp(&["--agent", "43"]).await?.close().await?;
    let writing = (dev_defaults("43", "admin"), Some(false));
    assert_eq!(recorded_access(&machine, "43")?, writing);
    let mut session = machine.mcp(&["--agent", "43", "--read-only"]).await?;
    assert_eq!(tool_names(&mut session).await?, READING_TOOLS);
    session.close().await?;
    assert_eq!(recorded_access(&machine, "43")?, writing);
    Ok(())
}

/// Makes agent `id`'s dev mode identity what 0.7 minted: `agent add --scope <scope>`, then
/// [`back_to_0_7`].
fn as_minted_by_0_7(machine: &Machine, id: &str, scope: &str) -> TestResult {
    machine.ok(&["agent", "add", id, "--scope", scope])?;
    back_to_0_7(
        machine,
        &machine.agent_dir(id),
        &machine.dev_dir().join("tls"),
        id,
        scope,
    )
}

/// Makes the identity in `dir`, which `agent add <id> --scope <scope>` minted, what 0.7
/// minted: its record loses `scopes_from` and goes back to version 1, and its token is one the
/// authority in `authority` signs with `scope` alone and neither `fleetring.kind` nor
/// `fleetring.owner`, as `token issue` signs it.
fn back_to_0_7(
    machine: &Machine,
    dir: &Path,
    authority: &Path,
    id: &str,
    scope: &str,
) -> TestResult {
    let file = dir.join("identity.toml");
    let text = fs::read_to_string(&file)?;
    assert!(text.contains("version = 2\n"), "{text}");
    assert!(text.contains("scopes_from = \"flags\"\n"), "{text}");
    let principal = format!("agent-{id}");
    let authority = authority.to_str().ok_or("not UTF-8")?;
    let issued = machine.ok(&[
        "token",
        "issue",
        "--principal",
        &principal,
        "--scope",
        scope,
        "--ttl",
        "30d",
        "--dir",
        authority,
    ])?;
    let token = issued.trim();
    let claimed = claims(token)?;
    assert_eq!(claimed.get("fleetring.kind"), None, "{claimed}");
    fs::write(dir.join("token"), format!("{token}\n"))?;
    let mut record: toml::Table = text.parse()?;
    record.insert("version".to_owned(), 1.into());
    record.remove("scopes_from");
    record.insert("token_id".to_owned(), token_id(token)?.into());
    record.insert("token_expires_at".to_owned(), token_expiry(token)?.into());
    fs::write(&file, toml::to_string(&record)?)?;
    Ok(())
}

/// The scopes a token carries, read without verifying it.
fn token_scopes(token: &str) -> TestResult<Vec<String>> {
    Ok(claims(token)?["fleetring.scopes"]
        .as_array()
        .ok_or("no scopes claim")?
        .iter()
        .filter_map(|scope| scope.as_str().map(str::to_owned))
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renew_adds_stream_only_to_the_old_default_and_a_start_never_does() -> TestResult {
    let machine = Machine::new()?;
    // Agent 42 as 0.7 minted it by default, 43 as 0.7 minted it with --scope, and 44 with
    // the old default chosen with --scope in this release, which records that.
    as_minted_by_0_7(&machine, "42", "admin:agent-42-*")?;
    as_minted_by_0_7(&machine, "43", "write:agent-43-*")?;
    machine.ok(&["agent", "add", "44", "--scope", "admin:agent-44-*"])?;
    let old = (vec!["admin:agent-42-*".to_owned()], Some(false));
    assert_eq!(recorded_access(&machine, "42")?, old);

    // A start serves with the identity as it is, and says how to widen it: to the defaults
    // the dev fleet lets this build mint.
    let defaults = dev_defaults("42", "admin");
    let (last, rest) = defaults.split_last().ok_or("no defaults")?;
    let before = snapshot(&machine.agent_dir("42"))?;
    let session = machine.mcp(&["--agent", "42"]).await?;
    let log = session.close().await?;
    assert!(
        log.contains(&format!(
            "note: agent 42's identity predates the stream scope, so it can't follow its \
             databases' changes; `fleetring agent renew 42` gives it {} and {last}",
            rest.join(", ")
        )),
        "{log}"
    );
    assert_eq!(
        snapshot(&machine.agent_dir("42"))?,
        before,
        "nothing changed"
    );
    for id in ["43", "44"] {
        let session = machine.mcp(&["--agent", id]).await?;
        let log = session.close().await?;
        assert!(!log.contains("predates the stream scope"), "{id}: {log}");
    }

    // Renew widens exactly the old default, and says so.
    let renewed = machine.ok(&["agent", "renew", "42"])?;
    let what = "defaults since 0.8 that let it delegate";
    assert!(
        renewed.contains(&format!(
            "  scopes: {} (added {}, {what}; the identity held only 0.7's default)\n",
            defaults.join(", "),
            defaults[1..].join(", ")
        )),
        "{renewed}"
    );
    let widened = (defaults.clone(), Some(false));
    assert_eq!(recorded_access(&machine, "42")?, widened);
    assert_eq!(machine.record("42")?["version"].as_integer(), Some(1));
    assert_eq!(token_scopes(&machine.token("42")?)?, widened.0);
    // Once widened, a renewal has nothing to add.
    let again = machine.ok(&["agent", "renew", "42"])?;
    assert!(
        again.contains(&format!("  scopes: {} (unchanged)\n", defaults.join(", "))),
        "{again}"
    );

    // Scopes of an identity's own are kept, whichever release minted it.
    let kept = machine.ok(&["agent", "renew", "43"])?;
    assert!(
        kept.contains("  scopes: write:agent-43-* (unchanged)\n"),
        "{kept}"
    );
    assert_eq!(token_scopes(&machine.token("43")?)?, ["write:agent-43-*"]);
    let chosen = machine.ok(&["agent", "renew", "44"])?;
    assert!(
        chosen.contains(
            "  scopes: admin:agent-44-* (unchanged: chosen with --scope, which renew never \
             widens; to change them, remove the agent and add it again)\n"
        ),
        "{chosen}"
    );
    assert_eq!(token_scopes(&machine.token("44")?)?, ["admin:agent-44-*"]);
    assert_eq!(machine.record("44")?["version"].as_integer(), Some(2));
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

/// `fork` and `grant` are actions since 0.8 (ADR-0044, point 2), so `token issue` and
/// `agent add` mint them, and `agent renew` re-mints them, with a note that nodes before 0.8
/// refuse such a token.
#[test]
fn token_issue_agent_add_and_renew_mint_fork_and_grant_with_a_note() -> TestResult {
    const NOTE: &str = "note: nodes before 0.8 refuse this token, since it carries fork or grant";
    let machine = Machine::new()?;
    let tls = machine.home().join("prod-tls");
    let dir = tls.to_str().ok_or("not UTF-8")?;
    machine.ok(&["cert", "init", "--fleet", "prod", "--dir", dir])?;
    let issue = |scopes: &[&str]| -> TestResult<Output> {
        let mut args = vec!["token", "issue", "--principal", "agent-42", "--dir", dir];
        for scope in scopes {
            args.extend(["--scope", scope]);
        }
        machine.run(&args)
    };
    for scopes in [
        &["fork:x"][..],
        &["grant:agent-42.*"],
        &["admin:agent-42-*", "fork:src-*", "grant:agent-42.*"],
    ] {
        let output = issue(scopes)?;
        assert!(output.status.success(), "{scopes:?}: {}", printed(&output));
        assert_eq!(token_scopes(stdout(&output).trim())?, scopes);
        assert_eq!(
            stderr(&output).matches(NOTE).count(),
            1,
            "{scopes:?}: {}",
            printed(&output)
        );
    }
    let plain = issue(&["admin:agent-42-*", "stream:agent-42-*"])?;
    assert!(plain.status.success(), "{}", printed(&plain));
    assert!(!stderr(&plain).contains(NOTE), "{}", printed(&plain));
    // The bare words are no scope, as `read` alone isn't.
    for bare in ["fork", "grant"] {
        let output = issue(&[bare])?;
        assert_eq!(
            output.status.code(),
            Some(2),
            "{bare}: {}",
            printed(&output)
        );
        assert!(
            stderr(&output).contains(&format!(
                "a scope is `<action>:<pattern>` or `fleet`, not '{bare}'"
            )),
            "{bare}: {}",
            printed(&output)
        );
    }

    let added = machine.run(&[
        "agent",
        "add",
        "42",
        "--scope",
        "admin:agent-42-*",
        "--scope",
        "grant:agent-42.*",
    ])?;
    assert!(added.status.success(), "{}", printed(&added));
    assert!(stderr(&added).contains(NOTE), "{}", printed(&added));
    assert_eq!(
        token_scopes(&machine.token("42")?)?,
        ["admin:agent-42-*", "grant:agent-42.*"]
    );
    let defaults = machine.run(&["agent", "add", "43"])?;
    assert!(defaults.status.success(), "{}", printed(&defaults));
    assert!(!stderr(&defaults).contains(NOTE), "{}", printed(&defaults));

    // A renewal re-mints the recorded scopes, and notes them the same way.
    let renewed = machine.run(&["agent", "renew", "42"])?;
    assert!(renewed.status.success(), "{}", printed(&renewed));
    assert_eq!(
        stderr(&renewed).matches(NOTE).count(),
        1,
        "{}",
        printed(&renewed)
    );
    assert_eq!(
        token_scopes(&machine.token("42")?)?,
        ["admin:agent-42-*", "grant:agent-42.*"]
    );
    let plain = machine.run(&["agent", "renew", "43"])?;
    assert!(plain.status.success(), "{}", printed(&plain));
    assert!(!stderr(&plain).contains(NOTE), "{}", printed(&plain));
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

    // A directory that holds anything, such as the working or home directory, is refused, and
    // its mode is left alone.
    fs::set_permissions(&out, fs::Permissions::from_mode(0o755))?;
    let refused = machine.run(&["cert", "client", "dev", "--dir", dir, "--out", out_dir])?;
    assert_eq!(refused.status.code(), Some(1), "{}", printed(&refused));
    assert!(
        stderr(&refused).contains("isn't empty"),
        "{}",
        printed(&refused)
    );
    assert_eq!(mode(&out)?, 0o755);
    assert!(!out.join("client.dev.key").exists());
    // An empty one is used as it is, with a warning if others can read it.
    let empty = machine.home().join("empty");
    fs::create_dir(&empty)?;
    fs::set_permissions(&empty, fs::Permissions::from_mode(0o755))?;
    let empty_dir = empty.to_str().ok_or("not UTF-8")?;
    let output = machine.run(&["cert", "client", "dev", "--dir", dir, "--out", empty_dir])?;
    assert!(output.status.success(), "{}", printed(&output));
    assert!(
        stderr(&output).contains("readable by other users"),
        "{}",
        printed(&output)
    );
    assert_eq!(mode(&empty)?, 0o755);
    assert_eq!(mode(&empty.join("client.dev.key"))?, 0o600);
    Ok(())
}

/// A control node in this process, served in plaintext, whose one member runs `software`: a
/// fleet whose members report `delegation` or not, whatever this build reports.
struct StubFleet {
    address: SocketAddr,
    _node: ControlNode,
    _dir: tempfile::TempDir,
}

impl StubFleet {
    async fn start(software: Software) -> TestResult<Self> {
        let dir = tempfile::tempdir()?;
        let options = ControlOptions::new(ControlConfig::new()).with_storage_io(StorageIo::Inline);
        let node = ControlNode::open(dir.path().join("control.db"), options, SystemClock).await?;
        node.handle()
            .submit(Action::Join {
                member: Member::new("data-1".parse()?).with_software(software),
            })
            .await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(
            tonic::transport::Server::builder()
                .layer(AuthLayer::new(Authorizer::plaintext()))
                .add_service(control_server(ControlService::new(node.handle())))
                .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
        );
        Ok(Self {
            address,
            _node: node,
            _dir: dir,
        })
    }

    /// A fleet whose one member runs 0.7.2, which doesn't report `delegation`.
    async fn lacking() -> TestResult<Self> {
        Self::start(Software::new(Release::new(0, 7, 2), 1)).await
    }

    /// A fleet whose one member reports `delegation`.
    async fn delegating() -> TestResult<Self> {
        Self::start(fleetringd::software::running().with_feature(Feature::Delegation)).await
    }
}

/// The note `agent add` and `agent renew` print for agent `id` on [`StubFleet::lacking`].
fn lacking_note(id: &str) -> String {
    format!(
        "note: agent {id} can delegate after `fleetring agent renew {id}` once every node runs \
         0.8; node 'data-1' runs fleetringd 0.7.2, which doesn't report the delegation feature"
    )
}

/// A fleet `prod` that isn't dev mode's: its authority, made in `<home>/prod-tls`, and the
/// context `prod`, which reaches `fleet` in plaintext.
fn prod_context(machine: &Machine, fleet: &StubFleet) -> TestResult<PathBuf> {
    let tls = machine.home().join("prod-tls");
    machine.ok(&["cert", "init", "--fleet", "prod", "--dir", path_str(&tls)?])?;
    point_prod_at(machine, fleet.address)?;
    Ok(tls)
}

/// Points the context `prod` at the control node at `address`.
fn point_prod_at(machine: &Machine, address: SocketAddr) -> TestResult {
    machine.contexts(&format!(
        "version = 1\n\n[contexts.prod]\nfleet = \"prod\"\ncontrol = [\"{address}\"]\n"
    ))
}

fn path_str(path: &Path) -> TestResult<&str> {
    Ok(path.to_str().ok_or("not UTF-8")?)
}

/// `agent <verb> <id>` on the context `prod`, signed by the authority in `tls`, with `extra`.
fn on_prod(
    machine: &Machine,
    tls: &Path,
    verb: &str,
    id: &str,
    extra: &[&str],
) -> TestResult<Output> {
    let mut args = vec![
        "agent",
        verb,
        id,
        "--context",
        "prod",
        "--insecure-plaintext",
        "--authority",
        path_str(tls)?,
    ];
    args.extend_from_slice(extra);
    machine.run(&args)
}

/// The scopes agent `id` of the context `prod` records.
fn prod_scopes(machine: &Machine, id: &str) -> TestResult<Vec<String>> {
    let text = fs::read_to_string(machine.context_agent_dir("prod", id).join("identity.toml"))?;
    let record: toml::Table = text.parse()?;
    Ok(record["scopes"]
        .as_array()
        .ok_or("no scopes")?
        .iter()
        .filter_map(|scope| scope.as_str().map(str::to_owned))
        .collect())
}

/// The claims of agent `id`'s token on the context `prod`.
fn prod_claims(machine: &Machine, id: &str) -> TestResult<Value> {
    claims(fs::read_to_string(machine.context_agent_dir("prod", id).join("token"))?.trim())
}

// ADR-0047, point 2.6: a root agent's token carries `fleetring.kind`, `fleetring.owner`, and
// `grant` once every member reports `delegation`, as every node of this release does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_agent_holds_grant_kind_and_owner_when_every_member_reports_delegation() -> TestResult
{
    use fleetring_proto::v1::DelegateTokenRequest;
    use fleetringd::Network;
    use fleetringd::auth::BearerToken;
    use fleetringd::tls::{Identity, Security};
    use fleetringd::transport::Credentials;

    let machine = Machine::new()?;
    let mut session = machine.mcp(&["--agent", "42"]).await?;
    session
        .ok("db_create", json!({"name": "agent-42-memory"}))
        .await?;
    let log = session.close().await?;
    assert!(!log.contains("can delegate after"), "{log}");
    let defaults = delegating_defaults("42", "admin");
    assert_eq!(
        recorded_access(&machine, "42")?,
        (defaults.clone(), Some(false))
    );
    let token = machine.token("42")?;
    let claimed = claims(&token)?;
    assert_eq!(claimed["fleetring.scopes"], json!(defaults), "{claimed}");
    assert_eq!(claimed["fleetring.kind"], "agent", "{claimed}");
    assert_eq!(claimed["fleetring.owner"], "admin", "{claimed}");
    // Still no field a 0.7 reader of the record lacks.
    assert_eq!(machine.record("42")?["version"].as_integer(), Some(1));

    // The token delegates: the dev node's control plane signs a child for it.
    let dir = machine.agent_dir("42");
    let identity = Identity::load(
        &dir.join("ca.crt"),
        &dir.join("client.agent-42.crt"),
        &dir.join("client.agent-42.key"),
    )?;
    let control = Network::default()
        .with_security(Security::Tls(identity))
        .with_credentials(Credentials::bearer(&BearerToken::new(&token))?)
        .control_client(format!("127.0.0.1:{}", machine.port()));
    let child = control
        .delegate_token(DelegateTokenRequest {
            operation_id: vec![7; 16],
            segment: "c7".to_owned(),
            ..DelegateTokenRequest::default()
        })
        .await?;
    assert_eq!(child.principal, "agent-42.c7");
    assert_eq!(
        child.scopes,
        ["admin:agent-42.c7-*", "stream:agent-42.c7-*"]
    );
    let child_claims = claims(&child.token)?;
    assert_eq!(child_claims["fleetring.owner"], "admin", "{child_claims}");
    assert_eq!(
        child_claims["fleetring.parent"], "agent-42",
        "{child_claims}"
    );

    // A read-only agent's defaults read, stream, and grant; one with --scope keeps its scopes
    // and still says it is an agent's.
    machine.ok(&["agent", "add", "43", "--read-only"])?;
    assert_eq!(
        recorded_access(&machine, "43")?,
        (delegating_defaults("43", "read"), Some(true))
    );
    assert_eq!(claims(&machine.token("43")?)?["fleetring.kind"], "agent");
    let chosen = machine.run(&["agent", "add", "44", "--scope", "admin:agent-44-*"])?;
    assert!(chosen.status.success(), "{}", printed(&chosen));
    assert!(
        !stderr(&chosen).contains("can delegate after"),
        "{}",
        printed(&chosen)
    );
    assert_eq!(token_scopes(&machine.token("44")?)?, ["admin:agent-44-*"]);
    assert_eq!(claims(&machine.token("44")?)?["fleetring.kind"], "agent");
    Ok(())
}

// ADR-0047, point 12.2: while a member lacks `delegation`, or the fleet can't be asked, a root
// agent gets FR-081's defaults without the new claims, and the command says why.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_add_on_a_fleet_lacking_delegation_mints_fr081_defaults_and_says_why() -> TestResult {
    let machine = Machine::new()?;
    let fleet = StubFleet::lacking().await?;
    let tls = prod_context(&machine, &fleet)?;
    let added = on_prod(&machine, &tls, "add", "42", &[])?;
    assert!(added.status.success(), "{}", printed(&added));
    assert!(
        stderr(&added).contains(&lacking_note("42")),
        "{}",
        printed(&added)
    );
    assert_eq!(prod_scopes(&machine, "42")?, fr081_defaults("42", "admin"));
    let claimed = prod_claims(&machine, "42")?;
    assert_eq!(claimed.get("fleetring.kind"), None, "{claimed}");
    assert_eq!(claimed.get("fleetring.owner"), None, "{claimed}");

    let reading = on_prod(&machine, &tls, "add", "43", &["--read-only"])?;
    assert!(reading.status.success(), "{}", printed(&reading));
    assert!(
        stderr(&reading).contains(&lacking_note("43")),
        "{}",
        printed(&reading)
    );
    assert_eq!(prod_scopes(&machine, "43")?, fr081_defaults("43", "read"));

    // Scopes chosen with --scope, without grant, could never delegate: nothing to say.
    let chosen = on_prod(
        &machine,
        &tls,
        "add",
        "44",
        &["--scope", "admin:agent-44-*"],
    )?;
    assert!(chosen.status.success(), "{}", printed(&chosen));
    assert!(
        !stderr(&chosen).contains("can delegate after"),
        "{}",
        printed(&chosen)
    );
    assert_eq!(prod_scopes(&machine, "44")?, ["admin:agent-44-*"]);

    // A fleet that can't be reached can't be shown to report it either.
    let unreachable = TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    point_prod_at(&machine, unreachable)?;
    let unknown = on_prod(&machine, &tls, "add", "45", &[])?;
    assert!(unknown.status.success(), "{}", printed(&unknown));
    assert!(
        stderr(&unknown).contains(
            "note: agent 45 can delegate after `fleetring agent renew 45` once every node runs \
             0.8; the fleet couldn't be asked whether every node can"
        ),
        "{}",
        printed(&unknown)
    );
    assert_eq!(prod_scopes(&machine, "45")?, fr081_defaults("45", "admin"));
    assert_eq!(prod_claims(&machine, "45")?.get("fleetring.kind"), None);
    Ok(())
}

// ADR-0047, point 12.4: `agent renew` upgrades only an identity whose scopes are exactly 0.7's
// or FR-081's defaults, and only once every member reports `delegation`; scopes chosen with
// `--scope` are never widened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renew_upgrades_exactly_0_7_or_fr081_defaults_never_flag_scopes() -> TestResult {
    const BEFORE_DELEGATION: &str = "the defaults minted before every node could delegate";
    const BEFORE_STREAM: &str = "only 0.7's default";
    let machine = Machine::new()?;
    let lacking = StubFleet::lacking().await?;
    let tls = prod_context(&machine, &lacking)?;
    let add = |id: &str, extra: &[&str]| -> TestResult {
        let output = on_prod(&machine, &tls, "add", id, extra)?;
        assert!(output.status.success(), "{id}: {}", printed(&output));
        Ok(())
    };
    let renew = |id: &str| -> TestResult<Output> {
        let output = on_prod(&machine, &tls, "renew", id, &[])?;
        assert!(output.status.success(), "{id}: {}", printed(&output));
        Ok(output)
    };
    // 42 and 45 hold FR-081's defaults; 43 chose its scopes; 44 and 47 are as 0.7 minted them;
    // 46 chose exactly FR-081's defaults with --scope.
    add("42", &[])?;
    add("43", &["--scope", "admin:agent-43-*"])?;
    for id in ["44", "47"] {
        let scope = format!("admin:agent-{id}-*");
        add(id, &["--scope", &scope])?;
        back_to_0_7(
            &machine,
            &machine.context_agent_dir("prod", id),
            &tls,
            id,
            &scope,
        )?;
    }
    add("45", &["--read-only"])?;
    add(
        "46",
        &[
            "--scope",
            "admin:agent-46-*",
            "--scope",
            "stream:agent-46-*",
        ],
    )?;

    // While a member lacks delegation, FR-081's defaults stay, and 0.7's become them; each
    // renewal says why the agent can't delegate yet.
    let kept = renew("42")?;
    assert!(
        stdout(&kept).contains("  scopes: admin:agent-42-*, stream:agent-42-* (unchanged)\n"),
        "{}",
        printed(&kept)
    );
    assert!(
        stderr(&kept).contains(&lacking_note("42")),
        "{}",
        printed(&kept)
    );
    assert_eq!(prod_claims(&machine, "42")?.get("fleetring.kind"), None);
    let widened = renew("44")?;
    assert!(
        stdout(&widened).contains(
            "  scopes: admin:agent-44-*, stream:agent-44-* (added stream:agent-44-*, a default \
             since 0.8; the identity held only 0.7's default)\n"
        ),
        "{}",
        printed(&widened)
    );
    assert!(
        stderr(&widened).contains(&lacking_note("44")),
        "{}",
        printed(&widened)
    );

    // Once every member reports delegation, 0.7's and FR-081's defaults become this release's,
    // and the token says it is an agent's that answers to the record's owner.
    let delegating = StubFleet::delegating().await?;
    point_prod_at(&machine, delegating.address)?;
    for (id, access, before) in [
        ("42", "admin", BEFORE_DELEGATION),
        ("44", "admin", BEFORE_DELEGATION),
        ("45", "read", BEFORE_DELEGATION),
        ("47", "admin", BEFORE_STREAM),
    ] {
        let output = renew(id)?;
        let defaults = delegating_defaults(id, access);
        let old = if before == BEFORE_STREAM {
            vec![format!("{access}:agent-{id}-*")]
        } else {
            fr081_defaults(id, access)
        };
        let added: Vec<&str> = defaults
            .iter()
            .filter(|scope| !old.contains(scope))
            .map(String::as_str)
            .collect();
        assert!(
            stdout(&output).contains(&format!(
                "  scopes: {} (added {}, defaults since 0.8 that let it delegate; the identity \
                 held {before})\n",
                defaults.join(", "),
                added.join(", ")
            )),
            "{id}: {}",
            printed(&output)
        );
        for unsaid in ["can delegate after", "nodes before 0.8"] {
            assert!(
                !stderr(&output).contains(unsaid),
                "{id}: {}",
                printed(&output)
            );
        }
        assert_eq!(prod_scopes(&machine, id)?, defaults, "{id}");
        let claimed = prod_claims(&machine, id)?;
        assert_eq!(
            claimed["fleetring.scopes"],
            json!(defaults),
            "{id}: {claimed}"
        );
        assert_eq!(claimed["fleetring.kind"], "agent", "{id}: {claimed}");
        assert_eq!(claimed["fleetring.owner"], "authority", "{id}: {claimed}");
    }
    // Scopes chosen with --scope are kept, even FR-081's defaults, though the token now says
    // it is an agent's.
    for (id, scopes) in [
        ("43", vec!["admin:agent-43-*"]),
        ("46", vec!["admin:agent-46-*", "stream:agent-46-*"]),
    ] {
        let output = renew(id)?;
        assert!(
            stdout(&output).contains("(unchanged: chosen with --scope"),
            "{id}: {}",
            printed(&output)
        );
        assert_eq!(prod_scopes(&machine, id)?, scopes, "{id}");
        assert_eq!(
            prod_claims(&machine, id)?["fleetring.kind"],
            "agent",
            "{id}"
        );
    }
    // Once upgraded, a renewal has nothing to add.
    let again = renew("42")?;
    assert!(
        stdout(&again).contains(&format!(
            "  scopes: {} (unchanged)\n",
            delegating_defaults("42", "admin").join(", ")
        )),
        "{}",
        printed(&again)
    );
    Ok(())
}

// ADR-0047, point 2.6: `token issue` gains no way to set `kind`, so it never mints a token that
// can delegate.
#[test]
fn token_issue_cannot_mint_kind_agent() -> TestResult {
    let machine = Machine::new()?;
    let tls = machine.home().join("prod-tls");
    let dir = path_str(&tls)?;
    machine.ok(&["cert", "init", "--fleet", "prod", "--dir", dir])?;
    let base = [
        "token",
        "issue",
        "--principal",
        "agent-42",
        "--scope",
        "admin:agent-42-*",
        "--scope",
        "grant:agent-42.*",
        "--dir",
        dir,
    ];
    let issued = machine.run(&base)?;
    assert!(issued.status.success(), "{}", printed(&issued));
    let claimed = claims(stdout(&issued).trim())?;
    assert_eq!(claimed.get("fleetring.kind"), None, "{claimed}");
    assert_eq!(claimed.get("fleetring.owner"), None, "{claimed}");
    for extra in [&["--kind", "agent"][..], &["--owner", "alice"]] {
        let mut args = base.to_vec();
        args.extend_from_slice(extra);
        let refused = machine.run(&args)?;
        assert_eq!(
            refused.status.code(),
            Some(2),
            "{extra:?}: {}",
            printed(&refused)
        );
        assert!(
            refused.stdout.is_empty(),
            "{extra:?}: {}",
            printed(&refused)
        );
    }
    Ok(())
}

// ADR-0047, point 12.1: a version 1 identity whose token has none of the new claims, as 0.7
// minted it, still serves, and `agent renew` upgrades it as far as the fleet allows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_1_identity_still_serves() -> TestResult {
    let machine = Machine::new()?;
    as_minted_by_0_7(&machine, "42", "admin:agent-42-*")?;
    assert_eq!(machine.record("42")?["version"].as_integer(), Some(1));
    let old_token = machine.token("42")?;
    machine.ok(&["db", "create", "agent-42-notes", "--agent", "42"])?;
    let listed = machine.ok(&["db", "list", "--agent", "42"])?;
    assert!(listed.contains("agent-42-notes"), "{listed}");
    let mut session = machine.mcp(&["--agent", "42"]).await?;
    session.ok("db_list", json!({})).await?;
    let log = session.close().await?;
    assert!(log.contains("predates the stream scope"), "{log}");
    assert_eq!(
        machine.token("42")?,
        old_token,
        "a start never renews it early"
    );

    // A renewal gives it the defaults the dev fleet allows this build to mint.
    let renewed = machine.run(&["agent", "renew", "42"])?;
    assert!(renewed.status.success(), "{}", printed(&renewed));
    assert_eq!(
        recorded_access(&machine, "42")?,
        (dev_defaults("42", "admin"), Some(false))
    );
    let claimed = claims(&machine.token("42")?)?;
    assert_eq!(claimed["fleetring.kind"], "agent", "{claimed}");
    assert!(
        !stderr(&renewed).contains("can delegate after"),
        "{}",
        printed(&renewed)
    );
    assert_eq!(machine.record("42")?["version"].as_integer(), Some(1));
    let listed = machine.ok(&["db", "list", "--agent", "42"])?;
    assert!(listed.contains("agent-42-notes"), "{listed}");
    Ok(())
}

// ADR-0047, point 10: a delegated agent is created by `agent delegate`, never minted by `agent
// add`; `agent renew` and `agent remove` take its ID, and create nothing for one that has no
// identity.
#[test]
fn agent_add_dotted_is_refused_naming_delegate() -> TestResult {
    let machine = Machine::new()?;
    let output = machine.run(&["agent", "add", "42.c7"])?;
    assert_eq!(output.status.code(), Some(2), "{}", printed(&output));
    assert!(
        stderr(&output).contains("delegated agents are created by `fleetring agent delegate`"),
        "{}",
        printed(&output)
    );
    for args in [
        &["agent", "renew", "42.c7"][..],
        &["agent", "remove", "42.c7", "--yes"],
    ] {
        let output = machine.run(args)?;
        assert_eq!(
            output.status.code(),
            Some(1),
            "{args:?}: {}",
            printed(&output)
        );
        assert!(
            stderr(&output).contains("agent '42.c7' has no identity"),
            "{args:?}: {}",
            printed(&output)
        );
        assert!(!machine.agent_dir("42.c7").exists(), "{args:?}");
    }
    // `agent delegate` takes a delegated agent's ID, and `--agent` and `agent show` either.
    let root = machine.run(&["agent", "delegate", "42"])?;
    assert_eq!(root.status.code(), Some(2), "{}", printed(&root));
    assert!(
        stderr(&root).contains("invalid delegated agent ID '42'"),
        "{}",
        printed(&root)
    );
    let missing = machine.run(&["db", "list", "--agent", "42.c7"])?;
    assert_eq!(missing.status.code(), Some(1), "{}", printed(&missing));
    assert!(
        stderr(&missing).contains("agent '42.c7' has no identity in"),
        "{}",
        printed(&missing)
    );
    assert!(
        stderr(&missing).contains("fleetring agent delegate 42.c7"),
        "{}",
        printed(&missing)
    );
    assert!(!machine.dev_dir().join("agents").exists(), "nothing minted");
    Ok(())
}

/// Runs `agent delegate` with `args` after the child's ID.
fn delegate(machine: &Machine, child: &str, args: &[&str]) -> TestResult<Output> {
    let mut all = vec!["agent", "delegate", child];
    all.extend_from_slice(args);
    machine.run(&all)
}

/// The key and value lines `agent delegate` prints, by key.
fn fields(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

// ADR-0047, points 4.2, 5 and 10: a root agent delegates a child, whose identity holds only its
// token and its record, and the child acts with its root's certificate and its own token.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delegate_then_act_as_the_child_creates_agent_42_c7_scratch() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    let root_token = machine.token("42")?;
    let output = delegate(&machine, "42.c7", &[])?;
    assert!(output.status.success(), "{}", printed(&output));
    let printed_out = stdout(&output);
    let shown = fields(&printed_out);
    assert_eq!(shown["agent"], "42.c7", "{printed_out}");
    assert_eq!(shown["principal"], "agent-42.c7");
    assert_eq!(shown["parent"], "42");
    assert_eq!(shown["root"], "42");
    assert_eq!(shown["owner"], "admin");
    assert_eq!(shown["scopes"], "admin:agent-42.c7-*, stream:agent-42.c7-*");
    assert!(
        !shown["token expires"].contains("capped"),
        "a 30-day parent leaves the 2-hour default whole: {printed_out}"
    );
    assert!(
        printed_out.ends_with(&format!("{TRUSTED_LOCAL}\n")),
        "{printed_out}"
    );
    let token = machine.token("42.c7")?;
    assert!(!printed_out.contains(&token) && !stderr(&output).contains(&token));
    assert!(!printed_out.contains(&root_token) && !stderr(&output).contains(&root_token));
    // The operation ID was said on stderr before the call, and is the record's.
    let record = machine.record("42.c7")?;
    let operation = record["operation"].as_str().ok_or("no operation")?;
    assert_eq!(shown["operation"], operation);
    assert!(
        stderr(&output).contains(&format!("operation {operation}: ")),
        "{}",
        printed(&output)
    );
    assert_eq!(record["version"].as_integer(), Some(2));
    assert_eq!(record["issued_by"].as_str(), Some("delegation"));
    assert_eq!(record["parent"].as_str(), Some("agent-42"));
    assert_eq!(record["root"].as_str(), Some("agent-42"));
    assert_eq!(
        record["token_id"].as_str(),
        Some(shown["delegation"].as_str())
    );
    assert!(record.get("issuer_key_id").is_some());
    assert!(record.get("cert_expires_at").is_none());
    let claimed = claims(&token)?;
    assert_eq!(claimed["jti"].as_str(), Some(shown["delegation"].as_str()));
    assert_eq!(claimed["sub"], "agent-42.c7");
    assert_eq!(claimed["fleetring.parent"], "agent-42");

    // As the child: its own databases, with the root's certificate.
    machine.ok(&["db", "create", "agent-42.c7-scratch", "--agent", "42.c7"])?;
    let listed = machine.ok(&["db", "list", "--agent", "42.c7"])?;
    assert!(listed.contains("agent-42.c7-scratch"), "{listed}");
    let from_variable = machine
        .command(&["db", "list"])
        .env("FLEETRING_AGENT", "42.c7")
        .output()?;
    assert!(
        from_variable.status.success(),
        "{}",
        printed(&from_variable)
    );
    assert_eq!(stdout(&from_variable), listed);
    // The root sees its child's database: its defaults cover agent-42.*.
    let by_root = machine.ok(&["db", "list", "--agent", "42"])?;
    assert!(by_root.contains("agent-42.c7-scratch"), "{by_root}");

    // `agent list` shows the child after its parent; `agent show` its lineage.
    let list = machine.ok(&["agent", "list"])?;
    let rows: Vec<&str> = list.lines().collect();
    assert!(rows[0].contains("PARENT"), "{list}");
    assert!(rows[1].starts_with("42 "), "{list}");
    assert!(rows[2].starts_with("42.c7 "), "{list}");
    assert!(
        rows[2].split_whitespace().nth(2) == Some("42"),
        "the child's parent: {list}"
    );
    let json: Value = serde_json::from_str(&machine.ok(&["agent", "list", "-o", "json"])?)?;
    let child = &json["agents"][1];
    assert_eq!(child["id"], "42.c7", "{json}");
    assert_eq!(child["parent"], "42");
    assert_eq!(child["root"], "42");
    assert_eq!(child["delegation_id"], shown["delegation"].as_str());
    assert_eq!(child["operation_id"], operation);
    assert_eq!(child["cert_expires_at"], Value::Null);
    assert_eq!(json["agents"][0]["parent"], Value::Null);
    let show = machine.ok(&["agent", "show", "42.c7"])?;
    for line in [
        "  parent       42\n".to_owned(),
        "  root         42\n".to_owned(),
        format!("  delegation   {}\n", shown["delegation"]),
        format!("  operation    {operation}\n"),
        "mcp --agent 42.c7 --context dev".to_owned(),
    ] {
        assert!(show.contains(&line), "{line:?} in {show}");
    }
    assert!(show.contains("  issuer key   "), "{show}");
    assert!(!show.contains("cert expires"), "{show}");
    assert!(!show.contains(&token), "{show}");

    // Delegating the same child again is refused, naming the way out.
    let again = delegate(&machine, "42.c7", &[])?;
    assert_eq!(again.status.code(), Some(1), "{}", printed(&again));
    assert!(
        stderr(&again).contains("pick another segment"),
        "{}",
        printed(&again)
    );
    Ok(())
}

// ADR-0047, points 3 and 5.3: a child acts only within the scopes it was given.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_is_refused() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    machine.ok(&["db", "create", "agent-42-memory", "--agent", "42"])?;
    machine.ok(&["db", "create", "agent-42.d1-x", "--agent", "42"])?;
    let output = delegate(&machine, "42.c7", &[])?;
    assert!(output.status.success(), "{}", printed(&output));
    let as_child = |args: &[&str]| -> TestResult<Output> {
        let mut all = args.to_vec();
        all.extend_from_slice(&["--agent", "42.c7"]);
        machine.run(&all)
    };
    for args in [
        &["db", "exec", "agent-42-memory", "CREATE TABLE t (x)"][..],
        &["db", "create", "agent-42-x"],
        &["db", "create", "agent-43-x"],
        &["db", "create", "agent-42.c70-x"],
        &["db", "query", "agent-42.d1-x", "SELECT 1"],
    ] {
        let refused = as_child(args)?;
        assert_eq!(
            refused.status.code(),
            Some(3),
            "{args:?}: {}",
            printed(&refused)
        );
    }
    // A default child holds no grant, so it can't delegate.
    let grandchild = delegate(&machine, "42.c7.r1", &[])?;
    assert_eq!(
        grandchild.status.code(),
        Some(3),
        "{}",
        printed(&grandchild)
    );
    assert!(
        stderr(&grandchild).contains("your token holds no grant over agent-42.c7.r1-"),
        "{}",
        printed(&grandchild)
    );
    assert!(
        stderr(&grandchild).contains("hint: agent 42.c7 needs a grant scope"),
        "{}",
        printed(&grandchild)
    );
    assert!(!machine.agent_dir("42.c7.r1").join("identity.toml").exists());
    assert!(
        !machine.agent_dir("42.c7.r1").join("operation").exists(),
        "a refusal recorded nothing, so the next run asks anew"
    );
    Ok(())
}

// ADR-0047, point 5.3 and ADR-0044's forking: a child given `fork` on its parent's memory forks
// it into its own prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_with_fork_scope_forks_agent_42_memory() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    machine.ok(&["db", "create", "agent-42-memory", "--agent", "42"])?;
    for sql in ["CREATE TABLE facts (x)", "INSERT INTO facts VALUES (1)"] {
        machine.ok(&["db", "exec", "agent-42-memory", sql, "--agent", "42"])?;
    }
    let output = delegate(
        &machine,
        "42.c7",
        &[
            "--scope",
            "admin:agent-42.c7-*",
            "--scope",
            "fork:agent-42-memory",
        ],
    )?;
    assert!(output.status.success(), "{}", printed(&output));
    let record = machine.record("42.c7")?;
    assert_eq!(record["scopes_from"].as_str(), Some("flags"));
    machine.ok(&[
        "db",
        "fork",
        "agent-42-memory",
        "agent-42.c7-memory",
        "--agent",
        "42.c7",
    ])?;
    let rows = machine.ok(&[
        "db",
        "query",
        "agent-42.c7-memory",
        "SELECT x FROM facts",
        "--agent",
        "42.c7",
    ])?;
    assert!(rows.contains('1'), "{rows}");
    // It may read its fork, never write the source.
    let write = machine.run(&[
        "db",
        "exec",
        "agent-42-memory",
        "INSERT INTO facts VALUES (2)",
        "--agent",
        "42.c7",
    ])?;
    assert_eq!(write.status.code(), Some(3), "{}", printed(&write));
    Ok(())
}

// ADR-0047, point 5.3: a read-only parent's default child reads and streams, and serves only the
// reading tools.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_only_parents_child_shows_read_only() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "43", "--read-only"])?;
    let output = delegate(&machine, "43.r", &[])?;
    assert!(output.status.success(), "{}", printed(&output));
    assert_eq!(
        fields(&stdout(&output))["scopes"],
        "read:agent-43.r-*, stream:agent-43.r-*"
    );
    assert_eq!(machine.record("43.r")?["read_only"].as_bool(), Some(true));
    let show = machine.ok(&["agent", "show", "43.r"])?;
    assert!(show.contains("  read-only    yes\n"), "{show}");
    let mut session = machine.mcp(&["--agent", "43.r"]).await?;
    assert_eq!(tool_names(&mut session).await?, READING_TOOLS);
    session.close().await?;
    Ok(())
}

// ADR-0047, point 5.4: an explicit lifetime is refused, never capped; the default is capped at
// the parent's expiry and says so; and a child with `grant` delegates a grandchild that acts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ttl_and_scope_refusals_and_the_capped_note() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    for (args, code, says) in [
        (&["--ttl", "25h"][..], 1, "at most 24 h"),
        (&["--ttl", "30s"], 1, "at least 60 s"),
        (
            &["--not-before", "2000-01-01T00:00:00Z"],
            1,
            "at most 30 s in the past",
        ),
        (
            &["--scope", "admin:agent-43-*"],
            3,
            "your token doesn't cover",
        ),
        (&["--scope", "grant:agent-42.*"], 1, "can't delegate"),
    ] {
        let refused = delegate(&machine, "42.x", args)?;
        assert_eq!(
            refused.status.code(),
            Some(code),
            "{args:?}: {}",
            printed(&refused)
        );
        assert!(
            stderr(&refused).contains(says),
            "{args:?}: {}",
            printed(&refused)
        );
        assert!(!machine.agent_dir("42.x").join("identity.toml").exists());
    }
    // A child that may delegate below itself, for an hour.
    let parent = delegate(
        &machine,
        "42.g",
        &[
            "--scope",
            "admin:agent-42.g-*",
            "--scope",
            "admin:agent-42.g.*",
            "--scope",
            "stream:agent-42.g.*",
            "--scope",
            "grant:agent-42.g.*",
            "--ttl",
            "1h",
        ],
    )?;
    assert!(parent.status.success(), "{}", printed(&parent));
    let grandchild = delegate(&machine, "42.g.r1", &[])?;
    assert!(grandchild.status.success(), "{}", printed(&grandchild));
    let shown = fields(&stdout(&grandchild));
    assert!(
        shown["token expires"].ends_with(" (capped at the parent's expiry)"),
        "{}",
        printed(&grandchild)
    );
    assert_eq!(shown["parent"], "42.g");
    assert_eq!(shown["root"], "42");
    assert_eq!(
        machine.record("42.g.r1")?["token_expires_at"].as_str(),
        machine.record("42.g")?["token_expires_at"].as_str(),
        "never past its parent"
    );
    let claimed = claims(&machine.token("42.g.r1")?)?;
    assert_eq!(claimed["fleetring.parent"], "agent-42.g");
    assert_eq!(
        claimed["fleetring.chain"].as_array().map(Vec::len),
        Some(2),
        "{claimed}"
    );
    machine.ok(&["db", "create", "agent-42.g.r1-x", "--agent", "42.g.r1"])?;
    // The grandchild rides the root's certificate too, and stays inside its own prefix.
    let outside = machine.run(&["db", "create", "agent-42.g-y", "--agent", "42.g.r1"])?;
    assert_eq!(outside.status.code(), Some(3), "{}", printed(&outside));
    let list = machine.ok(&["agent", "list"])?;
    let ids: Vec<&str> = list
        .lines()
        .skip(1)
        .filter_map(|line| line.split_whitespace().next())
        .collect();
    assert_eq!(ids, ["42", "42.g", "42.g.r1"], "{list}");
    Ok(())
}

// ADR-0047, point 4.1: a child's token copied next to another agent's certificate is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copied_child_token_with_agent_43_certificate_is_refused() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    machine.ok(&["agent", "add", "43"])?;
    let output = delegate(&machine, "42.c7", &[])?;
    assert!(output.status.success(), "{}", printed(&output));
    let token_file = machine.agent_dir("42.c7").join("token");
    let refused = machine.run(&[
        "db",
        "list",
        "--tls-dir",
        path_str(&machine.agent_dir("43"))?,
        "--as",
        "agent-43",
        "--token-file",
        path_str(&token_file)?,
    ])?;
    assert_eq!(refused.status.code(), Some(3), "{}", printed(&refused));
    let accepted = machine.run(&[
        "db",
        "list",
        "--tls-dir",
        path_str(&machine.agent_dir("42"))?,
        "--as",
        "agent-42",
        "--token-file",
        path_str(&token_file)?,
    ])?;
    assert!(accepted.status.success(), "{}", printed(&accepted));
    Ok(())
}

// ADR-0047, points 5.7 and 10: a run after a crash finds the operation ID in the child's
// directory and resends it, so the control plane records the delegation once; and a control
// plane that stays unavailable exits 4 and keeps the operation ID.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rerun_after_crash_reuses_the_operation_id_and_records_once() -> TestResult {
    use fleetring_control::fork::OperationId;
    use fleetring_proto::v1::DelegateTokenRequest;
    use fleetringd::Network;
    use fleetringd::auth::BearerToken;
    use fleetringd::tls::{Identity, Security};
    use fleetringd::transport::Credentials;

    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    // The first run asked and recorded, then crashed before it wrote the child's files.
    let operation = OperationId::from_bytes([7; 16]);
    let dir = machine.agent_dir("42");
    let identity = Identity::load(
        &dir.join("ca.crt"),
        &dir.join("client.agent-42.crt"),
        &dir.join("client.agent-42.key"),
    )?;
    let control = Network::default()
        .with_security(Security::Tls(identity))
        .with_credentials(Credentials::bearer(&BearerToken::new(
            &machine.token("42")?,
        ))?)
        .control_client(format!("127.0.0.1:{}", machine.port()));
    let first = control
        .delegate_token(DelegateTokenRequest {
            operation_id: operation.to_bytes().to_vec(),
            segment: "c7".to_owned(),
            ..DelegateTokenRequest::default()
        })
        .await?;
    let child = machine.agent_dir("42.c7");
    fs::create_dir_all(&child)?;
    fs::set_permissions(&child, fs::Permissions::from_mode(0o700))?;
    fs::write(child.join("operation"), format!("{operation}\n"))?;

    let rerun = delegate(&machine, "42.c7", &[])?;
    assert!(rerun.status.success(), "{}", printed(&rerun));
    assert!(
        stderr(&rerun).contains(&format!("resending operation {operation}")),
        "{}",
        printed(&rerun)
    );
    let record = machine.record("42.c7")?;
    assert_eq!(
        record["token_id"].as_str(),
        Some(first.delegation_id.as_str())
    );
    assert_eq!(
        record["operation"].as_str(),
        Some(operation.to_string().as_str())
    );
    let resent = claims(&machine.token("42.c7")?)?;
    assert_eq!(resent, claims(&first.token)?, "the same claims");
    assert!(!child.join("operation").exists());
    let listed: Value =
        serde_json::from_str(&machine.ok(&["agent", "list", "--fleet", "-o", "json"])?)?;
    let records: Vec<&Value> = listed["delegations"]
        .as_array()
        .ok_or("no delegations")?
        .iter()
        .filter(|record| record["principal"] == "agent-42.c7")
        .collect();
    assert_eq!(records.len(), 1, "recorded once: {listed}");

    // With the control plane gone, the resends run out: exit 4, and the operation ID stays for
    // the next run.
    machine.ok(&["dev", "stop"])?;
    let unavailable = delegate(&machine, "42.c9", &[])?;
    assert_eq!(
        unavailable.status.code(),
        Some(4),
        "{}",
        printed(&unavailable)
    );
    let kept = fs::read_to_string(machine.agent_dir("42.c9").join("operation"))?;
    assert!(
        stderr(&unavailable).contains(&format!(
            "fleetring agent delegate 42.c9 --operation-id {}",
            kept.trim()
        )),
        "{}",
        printed(&unavailable)
    );
    assert!(!machine.agent_dir("42.c9").join("identity.toml").exists());
    Ok(())
}

// ADR-0047, point 6.4: `agent list --fleet` lists the live records, never tokens, filtered by
// the recording node and by time, and needs the fleet scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_list_fleet_prints_live_records_by_window_and_needs_fleet() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    let before = time::OffsetDateTime::now_utc() - time::Duration::minutes(1);
    let output = delegate(&machine, "42.c7", &[])?;
    assert!(output.status.success(), "{}", printed(&output));
    let delegation = fields(&stdout(&output))["delegation"].clone();
    let rfc3339 = |at: time::OffsetDateTime| -> TestResult<String> {
        Ok(at.format(&time::format_description::well_known::Rfc3339)?)
    };

    let listed = machine.run(&["agent", "list", "--fleet"])?;
    assert!(listed.status.success(), "{}", printed(&listed));
    let table = stdout(&listed);
    assert!(table.starts_with("PRINCIPAL"), "{table}");
    assert!(table.contains("agent-42.c7"), "{table}");
    assert!(table.contains(&delegation), "{table}");
    assert!(
        stderr(&listed).contains("after a compromise, list with --since"),
        "{}",
        printed(&listed)
    );
    assert!(!table.contains(&machine.token("42.c7")?));
    let json: Value = serde_json::from_str(&machine.ok(&[
        "agent",
        "list",
        "--fleet",
        "--recorded-by",
        "dev",
        "--since",
        &rfc3339(before)?,
        "-o",
        "json",
    ])?)?;
    let record = &json["delegations"][0];
    assert_eq!(record["principal"], "agent-42.c7", "{json}");
    assert_eq!(record["parent"], "agent-42");
    assert_eq!(record["delegation_id"], delegation.as_str());
    assert_eq!(record["recorded_by"], "dev");
    assert_eq!(record["chain"].as_array().map(Vec::len), Some(1));
    for args in [
        &["--recorded-by", "elsewhere"][..],
        &[
            "--since",
            &rfc3339(time::OffsetDateTime::now_utc() + time::Duration::hours(1))?,
        ],
    ] {
        let mut all = vec!["agent", "list", "--fleet", "-o", "json"];
        all.extend_from_slice(args);
        let none: Value = serde_json::from_str(&machine.ok(&all)?)?;
        assert_eq!(none["delegations"], json!([]), "{args:?}");
    }
    // The filters need --fleet, and the listing the fleet scope, which no agent holds.
    let usage = machine.run(&["agent", "list", "--since", &rfc3339(before)?])?;
    assert_eq!(usage.status.code(), Some(2), "{}", printed(&usage));
    let refused = machine.run(&[
        "agent",
        "list",
        "--fleet",
        "--tls-dir",
        path_str(&machine.agent_dir("42"))?,
        "--as",
        "agent-42",
        "--token-file",
        path_str(&machine.agent_dir("42").join("token"))?,
    ])?;
    assert_eq!(refused.status.code(), Some(3), "{}", printed(&refused));
    Ok(())
}

// ADR-0047, points 7 and 10: `mcp --agent <child>` serves as the child with its own token, says
// whose it is, and never mints.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_as_child_serves_only_its_prefix() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    let output = delegate(&machine, "42.c7", &[])?;
    assert!(output.status.success(), "{}", printed(&output));
    let before = snapshot(&machine.agent_dir("42.c7"))?;
    let mut session = machine.mcp(&["--agent", "42.c7"]).await?;
    let created = session
        .ok("db_create", json!({"name": "agent-42.c7-notes"}))
        .await?;
    assert_eq!(created["database"], "agent-42.c7-notes", "{created}");
    for name in ["agent-42-x", "agent-43-x", "agent-42.c70-x"] {
        let refused = session.failed("db_create", json!({"name": name})).await?;
        assert_eq!(refused["code"], "permission_denied", "{name}: {refused}");
    }
    let tools = tool_names(&mut session).await?;
    assert!(
        tools
            .iter()
            .all(|tool| !tool.contains("delegat") && !tool.contains("token")),
        "{tools:?}"
    );
    let log = session.close().await?;
    assert!(
        log.contains(
            "serving MCP as agent-42.c7 on context 'dev', delegated by agent 42 for admin"
        ),
        "{log}"
    );
    assert!(log.contains(TRUSTED_LOCAL), "{log}");
    assert!(!log.contains(&machine.token("42.c7")?), "{log}");
    assert!(!log.contains("minted"), "{log}");
    assert_eq!(
        snapshot(&machine.agent_dir("42.c7"))?,
        before,
        "nothing minted"
    );
    Ok(())
}

/// What a child needs to delegate a grandchild: its own databases, and `grant` below itself.
fn delegating_child_scopes(child: &str) -> Vec<String> {
    let name = format!("agent-{child}");
    ["admin", "stream"]
        .iter()
        .flat_map(|action| [format!("{action}:{name}-*"), format!("{action}:{name}.*")])
        .chain([format!("grant:{name}.*")])
        .flat_map(|scope| ["--scope".to_owned(), scope])
        .collect()
}

/// Delegates `child` with [`delegating_child_scopes`], which must succeed.
fn delegate_delegating(machine: &Machine, child: &str) -> TestResult {
    let scopes = delegating_child_scopes(child);
    let args: Vec<&str> = scopes.iter().map(String::as_str).collect();
    let output = delegate(machine, child, &args)?;
    assert!(output.status.success(), "{child}: {}", printed(&output));
    Ok(())
}

/// Whether `token`, presented with the certificate in `cert_dir`, is accepted.
fn works(machine: &Machine, cert_dir: &Path, token: &str) -> TestResult<bool> {
    Ok(list_with_token(machine, cert_dir, token)?.status.success())
}

/// The time in `text` after `prefix`, up to the next `;`.
fn time_after(text: &str, prefix: &str) -> TestResult<time::OffsetDateTime> {
    let (_, rest) = text.split_once(prefix).ok_or("no such line")?;
    let value = rest.split(';').next().ok_or("no time")?;
    Ok(time::OffsetDateTime::parse(
        value.trim(),
        &time::format_description::well_known::Rfc3339,
    )?)
}

// ADR-0047, points 10 and 11: the root removes a child as a principal, which ends the child and
// its own child, while the root keeps working.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_remove_child_ends_child_and_grandchild_parent_keeps_working() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    let certificate = copy_certificate(&machine, "42")?;
    delegate_delegating(&machine, "42.c7")?;
    let grandchild = delegate(&machine, "42.c7.r1", &[])?;
    assert!(grandchild.status.success(), "{}", printed(&grandchild));
    let c7 = machine.token("42.c7")?;
    let r1 = machine.token("42.c7.r1")?;
    assert!(works(&machine, certificate.path(), &c7)?);
    assert!(works(&machine, certificate.path(), &r1)?);

    // Without a terminal it asks for --yes and keeps everything.
    let refused = machine.run(&["agent", "remove", "42.c7"])?;
    assert_eq!(refused.status.code(), Some(2), "{}", printed(&refused));
    assert!(machine.agent_dir("42.c7").exists());

    let output = machine.run(&["agent", "remove", "42.c7", "--yes"])?;
    assert!(output.status.success(), "{}", printed(&output));
    let removed = stdout(&output);
    assert!(
        removed.starts_with(
            "removed agent 42.c7: revoked agent-42.c7 and every agent below it until "
        ),
        "{removed}"
    );
    assert!(removed.contains(", as agent 42, and deleted "), "{removed}");
    assert!(
        stderr(&output).contains("this also ends 42.c7.r1"),
        "{}",
        printed(&output)
    );
    assert!(!machine.agent_dir("42.c7").exists());
    for token in [&c7, &r1] {
        assert!(!removed.contains(token.as_str()) && !stderr(&output).contains(token.as_str()));
    }
    wait_until_revoked(&machine, certificate.path(), &c7)?;
    wait_until_revoked(&machine, certificate.path(), &r1)?;
    machine.ok(&["db", "list", "--agent", "42"])?;
    // The grandchild's directory goes without a revocation of its own.
    machine.ok(&["agent", "remove", "42.c7.r1", "--yes", "--no-revoke"])?;
    Ok(())
}

// ADR-0047, point 11.7: a root agent removes itself without the fleet scope, by principal and by
// its own token ID, which ends everything below it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_remove_root_without_fleet_ends_the_subtree_by_principal_and_token_id() -> TestResult
{
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    let certificate = copy_certificate(&machine, "42")?;
    let root = machine.token("42")?;
    assert!(
        !token_scopes(&root)?.iter().any(|scope| scope == "fleet"),
        "the agent has no fleet scope"
    );
    delegate_delegating(&machine, "42.c7")?;
    let grandchild = delegate(&machine, "42.c7.r1", &[])?;
    assert!(grandchild.status.success(), "{}", printed(&grandchild));
    let c7 = machine.token("42.c7")?;
    let r1 = machine.token("42.c7.r1")?;

    let output = machine.run(&["agent", "remove", "42", "--yes"])?;
    assert!(output.status.success(), "{}", printed(&output));
    let removed = stdout(&output);
    let id = token_id(&root)?;
    assert!(
        removed.starts_with(&format!(
            "removed agent 42: revoked agent-42 and every agent below it, and token {id} until "
        )),
        "{removed}"
    );
    assert!(
        !stderr(&output).contains("couldn't revoke"),
        "the agent revoked itself, not the fleet scope: {}",
        printed(&output)
    );
    let from = time_after(&removed, "a new token for agent-42 may be minted from ")?;
    let now = time::OffsetDateTime::now_utc();
    assert!(
        from > now + time::Duration::seconds(25) && from <= now + time::Duration::seconds(32),
        "30 s after the revocation, in whole seconds: {removed}"
    );
    for token in [&root, &c7, &r1] {
        wait_until_revoked(&machine, certificate.path(), token)?;
    }
    Ok(())
}

// ADR-0047, point 11.5: a root token signed within 30 seconds of its principal's revocation is
// refused; one signed after the time `agent remove` printed works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_root_token_minted_31s_after_self_revocation_works() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    let removed = machine.ok(&["agent", "remove", "42", "--yes"])?;
    let from = time_after(&removed, "a new token for agent-42 may be minted from ")?;

    machine.ok(&["agent", "add", "42"])?;
    let soon = machine.token("42")?;
    let certificate = copy_certificate(&machine, "42")?;
    wait_until_revoked(&machine, certificate.path(), &soon)?;
    machine.ok(&["agent", "remove", "42", "--yes", "--no-revoke"])?;

    // By the wall clock the command signs with, which may be stepped while a monotonic sleep
    // runs.
    while time::OffsetDateTime::now_utc() < from {
        std::thread::sleep(Duration::from_millis(200));
    }
    machine.ok(&["agent", "add", "42"])?;
    let later = machine.token("42")?;
    let issued = claims(&later)?["iat"].as_i64().ok_or("no iat")?;
    assert!(
        issued >= from.unix_timestamp(),
        "signed at {issued}, from {from}"
    );
    let certificate = copy_certificate(&machine, "42")?;
    assert!(works(&machine, certificate.path(), &later)?);
    machine.ok(&["db", "list", "--agent", "42"])?;
    Ok(())
}

// ADR-0047, point 11.5: a child removed and delegated again at once may be answered "revoked
// less than a second ago", which the command retries until it works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remove_then_delegate_the_same_child_within_a_second_retries_and_works() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    let first = delegate(&machine, "42.c7", &[])?;
    assert!(first.status.success(), "{}", printed(&first));
    let old = machine.token("42.c7")?;
    machine.ok(&["agent", "remove", "42.c7", "--yes"])?;
    let again = delegate(&machine, "42.c7", &[])?;
    assert!(again.status.success(), "{}", printed(&again));
    let new = machine.token("42.c7")?;
    assert_ne!(token_id(&new)?, token_id(&old)?);
    let certificate = copy_certificate(&machine, "42")?;
    assert!(works(&machine, certificate.path(), &new)?);
    machine.ok(&["db", "create", "agent-42.c7-scratch", "--agent", "42.c7"])?;
    wait_until_revoked(&machine, certificate.path(), &old)?;
    Ok(())
}

// ADR-0047, point 10: `agent renew <child>` delegates again as the parent with the scopes it
// records, under a new operation ID, then revokes the old delegation ID.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renew_child_revokes_the_old_delegation_id() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    let certificate = copy_certificate(&machine, "42")?;
    let first = delegate(
        &machine,
        "42.c7",
        &["--scope", "admin:agent-42.c7-*", "--ttl", "1h"],
    )?;
    assert!(first.status.success(), "{}", printed(&first));
    let before = machine.record("42.c7")?;
    let old = machine.token("42.c7")?;

    let refused = machine.run(&["agent", "renew", "42.c7", "--authority", "/nowhere"])?;
    assert_eq!(refused.status.code(), Some(2), "{}", printed(&refused));

    let output = machine.run(&["agent", "renew", "42.c7"])?;
    assert!(output.status.success(), "{}", printed(&output));
    let renewed = stdout(&output);
    assert!(
        renewed.starts_with("renewed agent 42.c7: token expires "),
        "{renewed}"
    );
    assert!(
        renewed.contains("  scopes: admin:agent-42.c7-*\n"),
        "{renewed}"
    );
    let after = machine.record("42.c7")?;
    let new = machine.token("42.c7")?;
    assert_ne!(after["token_id"], before["token_id"]);
    assert_ne!(after["operation"], before["operation"]);
    assert_eq!(after["scopes"], before["scopes"]);
    assert_eq!(after["scopes_from"], before["scopes_from"]);
    assert_eq!(after["token_id"].as_str(), Some(token_id(&new)?.as_str()));
    assert!(
        renewed.contains(&format!("  delegation: {}\n", token_id(&new)?)),
        "{renewed}"
    );
    assert!(
        stderr(&output).contains(&format!("revoked token {}", token_id(&old)?)),
        "{}",
        printed(&output)
    );
    for token in [&old, &new] {
        assert!(!renewed.contains(token.as_str()) && !stderr(&output).contains(token.as_str()));
    }
    assert!(!machine.agent_dir("42.c7").join("operation").exists());
    wait_until_revoked(&machine, certificate.path(), &old)?;
    assert!(works(&machine, certificate.path(), &new)?);
    Ok(())
}

// ADR-0047, point 10 and ADR-0044, point 3.10: renewing a parent never renews its children,
// which the old token's revocation ends; each is renewed in turn, as the hints say.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renewing_a_parent_never_renews_children() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    let certificate = copy_certificate(&machine, "42")?;
    delegate_delegating(&machine, "42.c7")?;
    let grandchild = delegate(&machine, "42.c7.r1", &[])?;
    assert!(grandchild.status.success(), "{}", printed(&grandchild));
    let r1_record = machine.record("42.c7.r1")?;
    let r1 = machine.token("42.c7.r1")?;

    let output = machine.run(&["agent", "renew", "42.c7"])?;
    assert!(output.status.success(), "{}", printed(&output));
    assert!(
        stderr(&output).contains("this also ends 42.c7.r1"),
        "{}",
        printed(&output)
    );
    assert_eq!(machine.record("42.c7.r1")?, r1_record, "never renewed");
    assert_eq!(machine.token("42.c7.r1")?, r1);
    wait_until_revoked(&machine, certificate.path(), &r1)?;
    machine.ok(&["agent", "renew", "42.c7.r1"])?;
    assert!(works(
        &machine,
        certificate.path(),
        &machine.token("42.c7.r1")?
    )?);

    // Renewing the root ends its children by their chains, and the hint's command renews them.
    let c7 = machine.token("42.c7")?;
    let output = machine.run(&["agent", "renew", "42"])?;
    assert!(output.status.success(), "{}", printed(&output));
    assert!(
        stderr(&output).contains("this also ends 42.c7, 42.c7.r1"),
        "{}",
        printed(&output)
    );
    let certificate = copy_certificate(&machine, "42")?;
    wait_until_revoked(&machine, certificate.path(), &c7)?;
    machine.ok(&["agent", "renew", "42.c7"])?;
    assert!(works(
        &machine,
        certificate.path(),
        &machine.token("42.c7")?
    )?);
    Ok(())
}

// ADR-0047, invariant I4, from the command line: `agent remove 42.c7` racing `agent delegate
// 42.c7.r1`, run as 42.c7, never leaves 42.c7.r1 usable once both are done.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_agent_remove_and_agent_delegate_never_leave_r1_usable() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    let certificate = copy_certificate(&machine, "42")?;
    for round in 0..3 {
        let child = format!("42.c{round}");
        let grandchild = format!("{child}.r1");
        delegate_delegating(&machine, &child)?;
        let mut remove = machine
            .command(&["agent", "remove", &child, "--yes"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let mut delegating = machine
            .command(&["agent", "delegate", &grandchild])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let removed = remove.wait()?;
        let delegated = delegating.wait()?;
        assert!(removed.success(), "round {round}: the removal failed");
        if delegated.success() {
            let token = machine.token(&grandchild)?;
            wait_until_revoked(&machine, certificate.path(), &token)?;
        } else {
            assert!(
                !machine.agent_dir(&grandchild).join("token").exists(),
                "round {round}: a refused delegation writes no token"
            );
        }
    }
    machine.ok(&["db", "list", "--agent", "42"])?;
    Ok(())
}

// `docs/cli.md`, section 16: `agent delegate` acts as the parent alone, so identity flags beside
// it are a usage error, and its JSON, like its text, holds no token.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delegate_refuses_identity_flags_and_its_json_holds_no_token() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    let root_dir = machine.agent_dir("42");
    for flags in [
        &["--tls-dir", path_str(&root_dir)?][..],
        &["--as", "agent-42"],
        &["--token", "x.y.z"],
        &["--token-file", path_str(&root_dir.join("token"))?],
    ] {
        let output = delegate(&machine, "42.c7", flags)?;
        assert_eq!(
            output.status.code(),
            Some(2),
            "{flags:?}: {}",
            printed(&output)
        );
        assert!(
            stderr(&output).contains("acts as the parent"),
            "{flags:?}: {}",
            printed(&output)
        );
        assert!(!machine.agent_dir("42.c7").exists(), "{flags:?}");
    }
    let output = delegate(&machine, "42.c7", &["-o", "json"])?;
    assert!(output.status.success(), "{}", printed(&output));
    let json: Value = serde_json::from_slice(&output.stdout)?;
    let token = machine.token("42.c7")?;
    assert!(!stdout(&output).contains(&token), "{json}");
    assert!(!stderr(&output).contains(&token));
    assert_eq!(json["agents"][0]["id"], "42.c7", "{json}");
    assert_eq!(
        json["agents"][0]["delegation_id"].as_str(),
        Some(token_id(&token)?.as_str())
    );
    Ok(())
}

// R3's finding 4: an operation the control plane knows with other flags keeps its ID, and the
// hint names the file to delete; one known for another child is given up, so the next run asks
// under a new ID instead of conflicting forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_conflicting_operation_is_resent_with_its_flags_or_given_up() -> TestResult {
    let machine = Machine::new()?;
    machine.ok(&["agent", "add", "42"])?;
    let first = delegate(&machine, "42.c7", &["--ttl", "1h"])?;
    assert!(first.status.success(), "{}", printed(&first));
    let record = machine.record("42.c7")?;
    let operation = record["operation"]
        .as_str()
        .ok_or("no operation")?
        .to_owned();
    let delegation = record["token_id"].as_str().ok_or("no token id")?.to_owned();

    // As a reply lost after the control plane recorded it leaves the directory.
    let dir = machine.agent_dir("42.c7");
    fs::remove_file(dir.join("identity.toml"))?;
    fs::remove_file(dir.join("token"))?;
    fs::write(dir.join("operation"), format!("{operation}\n"))?;
    let conflict = delegate(&machine, "42.c7", &[])?;
    assert_eq!(conflict.status.code(), Some(1), "{}", printed(&conflict));
    assert!(
        stderr(&conflict).contains("another --ttl")
            && stderr(&conflict).contains(path_str(&dir.join("operation"))?),
        "{}",
        printed(&conflict)
    );
    assert!(dir.join("operation").exists(), "kept, to be resent");
    let resent = delegate(&machine, "42.c7", &["--ttl", "1h"])?;
    assert!(resent.status.success(), "{}", printed(&resent));
    assert_eq!(
        machine.record("42.c7")?["token_id"].as_str(),
        Some(delegation.as_str())
    );

    // The same operation ID left in another child's directory can never be resent there.
    let other = machine.agent_dir("42.c8");
    fs::create_dir_all(&other)?;
    fs::set_permissions(&other, fs::Permissions::from_mode(0o700))?;
    fs::write(other.join("operation"), format!("{operation}\n"))?;
    let given_up = delegate(&machine, "42.c8", &["--ttl", "1h"])?;
    assert_eq!(given_up.status.code(), Some(1), "{}", printed(&given_up));
    assert!(
        stderr(&given_up).contains("can't be resent"),
        "{}",
        printed(&given_up)
    );
    assert!(!other.join("operation").exists(), "given up");
    let fresh = delegate(&machine, "42.c8", &["--ttl", "1h"])?;
    assert!(fresh.status.success(), "{}", printed(&fresh));
    assert_ne!(
        machine.record("42.c8")?["operation"].as_str(),
        Some(operation.as_str())
    );
    Ok(())
}
