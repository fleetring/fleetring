//! FR-068: `fleetringd dev`, one local node for development (ADR-0039).
//!
//! Each test runs the real binary in a scratch directory on ports of its own: it creates its
//! state once and reuses it, refuses any address that isn't loopback, serves the same client code
//! a three-node fleet does, and runs the README's quickstart as written.
#![cfg(unix)]

#[path = "../upgrade/common.rs"]
#[expect(
    dead_code,
    reason = "the upgrade tests' helpers; these tests use the process fleet and its waits"
)]
mod common;
#[path = "../support/mod.rs"]
mod support;

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::DatabaseName;
use fleetringd::Network;
use fleetringd::data::{Client, DatabaseSpec};
use fleetringd::engine::{Statement, Value};
use fleetringd::tls::{Identity, Security};

use crate::common::{PATIENCE, ProcessFleet, TestResult, printed, wait_for, wait_for_members};

/// The next port [`port_pair`] tries.
static NEXT_PORT: AtomicU16 = AtomicU16::new(0);

/// A port that is free with the one after it, for a dev node and its admin listener. Ports come
/// from below the systems' ephemeral ranges, where no other socket is handed one by the OS, from
/// a start that differs between processes; each is checked free, together with its neighbor.
fn port_pair() -> TestResult<u16> {
    let start = 20_000 + u16::try_from(std::process::id() % 400)? * 20;
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

/// What the banner starts with once the node serves.
const BANNER: &str = "fleetringd is running in dev mode";

/// The variables a developer's shell may hold that would point a command elsewhere.
const CLIENT_VARS: [&str; 8] = [
    "FLEETRING_CONTROL",
    "FLEETRING_TLS_DIR",
    "FLEETRING_AS",
    "FLEETRING_CA",
    "FLEETRING_CERT",
    "FLEETRING_KEY",
    "FLEETRING_TOKEN",
    "FLEETRING_LOG_FORMAT",
];

/// The files a first run creates that a second must leave byte for byte as they are.
const STATE: [&str; 7] = [
    "tls/ca.crt",
    "tls/ca.key",
    "tls/node.dev.crt",
    "tls/node.dev.key",
    "tls/client.admin.crt",
    "tls/client.admin.key",
    "fleetringd.toml",
];

fn binary() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_fleetringd"))
}

/// `fleetringd` with `args`, in a clean environment whose home is `home`.
fn fleetringd(home: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(binary());
    command.args(args).env("HOME", home);
    for variable in CLIENT_VARS {
        command.env_remove(variable);
    }
    command
}

/// A `fleetringd dev` process, killed when dropped.
struct DevNode {
    dir: PathBuf,
    port: u16,
    log: PathBuf,
    child: Option<Child>,
}

impl DevNode {
    /// Starts `fleetringd dev` on `dir` and a free port, and waits for its banner.
    fn start(root: &Path, dir: &Path) -> TestResult<Self> {
        let mut node = Self {
            dir: dir.to_owned(),
            port: port_pair()?,
            log: root.join(format!(
                "dev-{}.log",
                dir.file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("dev")
            )),
            child: None,
        };
        node.spawn(root, Some(node.port))?;
        Ok(node)
    }

    /// Starts the process, appending to its log, and waits for a new banner.
    fn spawn(&mut self, home: &Path, port: Option<u16>) -> TestResult {
        let before = fs::read_to_string(&self.log)
            .unwrap_or_default()
            .matches(BANNER)
            .count();
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)?;
        let mut command = fleetringd(home, &["dev", "--data-dir"]);
        command.arg(&self.dir);
        if let Some(port) = port {
            command.args(["--port", &port.to_string()]);
        }
        let mut child = command.stdout(Stdio::null()).stderr(log).spawn()?;
        let log = self.log.clone();
        let mut exited = None;
        let started = wait_for("the dev node prints its banner", || {
            let text = fs::read_to_string(&log).unwrap_or_default();
            if text.matches(BANNER).count() > before {
                return Some(true);
            }
            // A node that exits never prints it; stop waiting.
            exited = child.try_wait().ok().flatten();
            exited.map(|_| false)
        });
        self.child = Some(child);
        match started {
            Ok(true) => Ok(()),
            Ok(false) => {
                Err(format!("the dev node exited ({exited:?})\n{}", self.log_text()).into())
            }
            Err(error) => Err(format!("{error}\n{}", self.log_text()).into()),
        }
    }

    /// Stops the process with SIGTERM, as a service manager does, and waits for it to exit.
    fn stop(&mut self) -> TestResult {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        let killed = Command::new("kill")
            .arg("-TERM")
            .arg(child.id().to_string())
            .status()?;
        assert!(killed.success(), "kill -TERM {}", child.id());
        let status = child.wait()?;
        assert!(
            status.success(),
            "dev node exited with {status}\n{}",
            self.log_text()
        );
        Ok(())
    }

    fn control(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// A client acting as the admin principal, from the files dev mode wrote, as an
    /// application on this machine would build one.
    fn client(&self) -> TestResult<Client> {
        let tls = self.dir.join("tls");
        let identity = Identity::load(
            &tls.join("ca.crt"),
            &tls.join("client.admin.crt"),
            &tls.join("client.admin.key"),
        )?;
        Ok(Client::new(
            self.control(),
            Network::default().with_security(Security::Tls(identity)),
        ))
    }

    fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for DevNode {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// How long to wait between attempts while a new database finds its primary.
const POLL: Duration = Duration::from_millis(100);

/// The client code both a dev node and a fleet run: creates `name` with the fleet's default
/// replication factor, which must be `expected`, creates a table once it has a primary, writes a
/// row that every replica acknowledges, and reads it back.
async fn create_write_read(client: &Client, name: &str, expected: ReplicationFactor) -> TestResult {
    let name: DatabaseName = name.parse()?;
    let location = client
        .create_database_with(&name, DatabaseSpec::default())
        .await?;
    assert_eq!(location.replication_factor, expected, "{location:?}");
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while let Err(status) = client
        .execute(
            &name,
            Statement::new("CREATE TABLE note (id INTEGER PRIMARY KEY, body TEXT)"),
        )
        .await
    {
        if tokio::time::Instant::now() > deadline {
            return Err(format!("{name} never took a write: {status}").into());
        }
        tokio::time::sleep(POLL).await;
    }
    let written = client
        .execute(
            &name,
            Statement::with_params(
                "INSERT INTO note (id, body) VALUES (?, ?)",
                vec![Value::Integer(1), Value::Text("hello".to_owned())],
            ),
        )
        .await?;
    assert_eq!(written.rows_affected, 1);
    assert_eq!(written.replicas, u32::from(expected.get()), "{written:?}");
    assert!(!written.under_replicated, "{written:?}");
    read_back(client, &name).await
}

/// Reads the row [`create_write_read`] wrote.
async fn read_back(client: &Client, name: &DatabaseName) -> TestResult {
    let rows = client
        .query(name, Statement::new("SELECT id, body FROM note"))
        .await?;
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(1), Value::Text("hello".to_owned())]]
    );
    Ok(())
}

fn read_state(dir: &Path) -> TestResult<Vec<Vec<u8>>> {
    STATE
        .iter()
        .map(|file| Ok(fs::read(dir.join(file))?))
        .collect()
}

#[test]
fn a_first_run_creates_the_state_and_a_second_reuses_it_byte_for_byte() -> TestResult {
    let root = tempfile::tempdir()?;
    let dir = root.path().join("first");
    let mut node = DevNode::start(root.path(), &dir)?;
    assert!(
        node.log_text().contains(
            "created the authority, the node certificate, the admin certificate, and \
             fleetringd.toml"
        ),
        "{}",
        node.log_text()
    );
    let runtime = tokio::runtime::Runtime::new()?;
    let client = node.client()?;
    runtime.block_on(wait_for_members(&client, &["dev"], None))?;
    let one = ReplicationFactor::new(1).ok_or("zero")?;
    runtime.block_on(create_write_read(&client, "notes", one))?;
    let created = read_state(&dir)?;
    node.stop()?;

    // Again, without --port: the configuration has it.
    node.spawn(root.path(), None)?;
    assert!(
        node.log_text()
            .contains("reused the authority, the certificates, and the configuration"),
        "{}",
        node.log_text()
    );
    assert_eq!(read_state(&dir)?, created, "a second run changed the state");
    // The database outlived the stop, as `on_shutdown = "stay"` keeps it placed.
    let client = node.client()?;
    let notes: DatabaseName = "notes".parse()?;
    runtime.block_on(async {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            match read_back(&client, &notes).await {
                Ok(()) => return Ok(()),
                Err(error) if tokio::time::Instant::now() > deadline => return Err(error),
                Err(_) => tokio::time::sleep(POLL).await,
            }
        }
    })?;
    node.stop()
}

#[test]
fn a_non_loopback_address_is_refused_from_the_config_and_from_flags() -> TestResult {
    let root = tempfile::tempdir()?;
    let dir = root.path().join("refused");
    let port = port_pair()?;
    let prepared = fleetringd::dev::prepare(
        &dir,
        Some(port),
        fleetring_control::Timestamp::from_millis(1_790_000_000_000),
    )?;
    let config = dir.join("fleetringd.toml");
    let text = fs::read_to_string(&config)?;
    let refused = |output: &Output, field: &str| {
        assert!(!output.status.success(), "{}", printed(output));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&format!("config field `{field}` is invalid"))
                && stderr.contains("isn't a loopback address"),
            "{}",
            printed(output)
        );
    };

    // `--listen` on top of the generated configuration.
    let output = fleetringd(root.path(), &["--config"])
        .arg(&config)
        .args(["--listen", &format!("0.0.0.0:{port}")])
        .output()?;
    refused(&output, "listen");

    // A configuration edited to listen everywhere, run either way.
    fs::write(
        &config,
        text.replace(&format!("127.0.0.1:{port}"), &format!("0.0.0.0:{port}")),
    )?;
    let output = fleetringd(root.path(), &["dev", "--data-dir"])
        .arg(&dir)
        .output()?;
    refused(&output, "listen");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("invalid dev mode configuration"),
        "{}",
        printed(&output)
    );
    let output = fleetringd(root.path(), &["--config"])
        .arg(&config)
        .output()?;
    refused(&output, "listen");

    // The admin listener too.
    let admin = prepared.config().admin.listen;
    fs::write(
        &config,
        text.replace(&admin.to_string(), &format!("0.0.0.0:{}", admin.port())),
    )?;
    let output = fleetringd(root.path(), &["dev", "--data-dir"])
        .arg(&dir)
        .output()?;
    refused(&output, "admin.listen");

    // The only address flag `dev` takes is a port, and one that leaves no room is refused.
    let output = fleetringd(root.path(), &["dev", "--port", "0", "--data-dir"])
        .arg(root.path().join("other"))
        .output()?;
    assert!(
        !output.status.success()
            && String::from_utf8_lossy(&output.stderr).contains("port 0 can't be used"),
        "{}",
        printed(&output)
    );
    Ok(())
}

#[test]
fn the_same_client_code_runs_against_dev_mode_and_a_three_node_fleet() -> TestResult {
    let root = tempfile::tempdir()?;
    let runtime = tokio::runtime::Runtime::new()?;

    let node = DevNode::start(root.path(), &root.path().join("dev"))?;
    let dev = node.client()?;
    runtime.block_on(wait_for_members(&dev, &["dev"], None))?;
    runtime.block_on(create_write_read(
        &dev,
        "notes",
        ReplicationFactor::new(1).ok_or("zero")?,
    ))?;

    let fleet = ProcessFleet::start(&root.path().join("fleet"), binary(), 3)?;
    let client = fleet.client()?;
    runtime.block_on(wait_for_members(
        &client,
        &["node-1", "node-2", "node-3"],
        None,
    ))?;
    runtime
        .block_on(create_write_read(
            &client,
            "notes",
            ReplicationFactor::new(3).ok_or("zero")?,
        ))
        .map_err(|error| format!("{error}\n{}", fleet.logs()))?;
    Ok(())
}

/// Runs `script` in `sh` with this build's `fleetringd` first on `PATH`.
fn shell(home: &Path, script: &str) -> TestResult<Output> {
    let bin = binary().parent().ok_or("no directory")?;
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(script)
        .env("HOME", home)
        .env("PATH", path);
    for variable in CLIENT_VARS {
        command.env_remove(variable);
    }
    Ok(command.output()?)
}

#[test]
fn the_quickstart_runs_as_documented() -> TestResult {
    let root = tempfile::tempdir()?;
    let dir = root.path().join("quickstart");
    let node = DevNode::start(root.path(), &dir)?;
    let env = dir.join("env");
    let run = |command: &str| -> TestResult<String> {
        let output = shell(root.path(), &format!(". '{}' && {command}", env.display()))?;
        assert!(output.status.success(), "{command}: {}", printed(&output));
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    };
    let status = wait_for("the node is a live member", || {
        run("fleetringd status")
            .ok()
            .filter(|status| status.contains(" live "))
    })?;
    assert!(
        status.starts_with("dev mode: one node on loopback, for development only\n"),
        "{status}"
    );
    let created = run("fleetringd db create notes")?;
    assert!(created.starts_with("created notes\n"), "{created}");
    assert!(created.contains("\nreplication factor: 1\n"), "{created}");
    wait_for("notes takes a write", || {
        run("fleetringd db exec notes \"CREATE TABLE note (body TEXT)\"").ok()
    })?;
    let written = run("fleetringd db exec notes \"INSERT INTO note VALUES (?)\" hello")?;
    assert_eq!(
        written,
        "1 row affected; acknowledged by 1 of 1 replica\n",
        "{}",
        node.log_text()
    );
    let read = run("fleetringd db query notes \"SELECT body FROM note\"")?;
    assert_eq!(read, "body\nhello\n");
    let changes = dir.join("changes").join("notes.ndjson");
    let streamed = wait_for("the change reaches the dev sink", || {
        fs::read_to_string(&changes)
            .ok()
            .filter(|text| text.contains("hello"))
    })?;
    assert!(streamed.contains("\"note\""), "{streamed}");

    // `status` doesn't warn that the one-node ring replicates fewer ways than a fleet's ring.
    let status = shell(
        root.path(),
        &format!(". '{}' && fleetringd status", env.display()),
    )?;
    let warnings = String::from_utf8_lossy(&status.stderr);
    assert!(!warnings.contains("replicates"), "{warnings}");

    // A factor larger than the fleet is accepted, with a warning that names both numbers.
    let wide = shell(
        root.path(),
        &format!(
            ". '{}' && fleetringd db create wide --replication-factor 3",
            env.display()
        ),
    )?;
    assert!(wide.status.success(), "{}", printed(&wide));
    let warning = String::from_utf8_lossy(&wide.stderr);
    assert!(
        warning.contains(
            "warning: wide has a replication factor of 3, but the fleet has 1 data node: its \
             writes fail until 1 more joins"
        ),
        "{warning}"
    );
    Ok(())
}
