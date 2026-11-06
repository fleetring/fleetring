//! FR-056: `--json` on every read command, exit codes, hints, `-q` and the `--token` warning
//! (`docs/cli.md`, sections 5, 6, 8 and 11).
//!
//! The JSON a command prints is a contract: each test here pins one command's fields and their
//! types with a snapshot of its shape, so a rename, a retype or a removal fails CI. One node
//! serves every command; the commands run with their streams piped, as a script runs them.

use std::error::Error;
use std::io::{BufRead as _, BufReader};
use std::process::{Output, Stdio};
use std::time::Duration;

use fleetringd::{Config, Daemon};
use serde_json::{Value, json};

use crate::support::Tls;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// What the checks run on a blocking thread return: their error crosses back to the test.
type Sendable<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

/// Guards against a hang only.
const HANG_GUARD: Duration = Duration::from_secs(60);

/// A value's shape: every leaf replaced by its JSON type's name, so the snapshot pins field names
/// and types, not values that differ between runs.
fn shape(value: &Value) -> Value {
    match value {
        Value::Null => json!("null"),
        Value::Bool(_) => json!("bool"),
        Value::Number(_) => json!("number"),
        Value::String(_) => json!("string"),
        Value::Array(items) => Value::Array(items.iter().map(shape).collect()),
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(name, value)| (name.clone(), shape(value)))
                .collect(),
        ),
    }
}

/// How often [`exited`] looks, and how many times before it gives up: [`HANG_GUARD`] in all.
const POLL: Duration = Duration::from_millis(50);
const POLLS: u32 = 1_200;

/// The status `child` exits with, killing it if it hasn't after [`HANG_GUARD`].
fn exited(child: &mut std::process::Child, what: &str) -> Sendable<std::process::ExitStatus> {
    for _ in 0..POLLS {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        std::thread::sleep(POLL);
    }
    let _ = child.kill();
    let _ = child.wait();
    Err(format!("{what} didn't end").into())
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// The command line against one node, as the principal the test authority's `client.crt` names.
struct Fleet {
    tls: Tls,
    control: String,
}

impl Fleet {
    fn command(&self, args: &[&str]) -> std::process::Command {
        let mut command = crate::support::fleetring();
        command
            .env("FLEETRING_TLS_DIR", self.tls.dir())
            .env("FLEETRING_CONTROL", &self.control)
            .env_remove("CI")
            .env_remove("NO_COLOR")
            .args(args)
            .stdin(Stdio::null());
        command
    }

    fn run(&self, args: &[&str]) -> Sendable<Output> {
        Ok(self.command(args).output()?)
    }

    /// Runs `args`, which must succeed, and returns stdout parsed as one JSON value, after
    /// checking that nothing else is on stdout and no spinner byte is on stderr.
    fn json(&self, args: &[&str]) -> Sendable<Value> {
        let output = self.run(args)?;
        assert!(
            output.status.success(),
            "{args:?}: {}{}",
            stdout(&output),
            stderr(&output)
        );
        assert!(
            !stderr(&output).contains('\r') && !stderr(&output).contains('\u{2713}'),
            "{args:?}: a spinner on stderr: {:?}",
            stderr(&output)
        );
        Ok(serde_json::from_slice(&output.stdout)
            .map_err(|error| format!("{args:?}: {error}: {}", stdout(&output)))?)
    }
}

/// Starts one node that is both the control plane and a data node, and waits until it has
/// joined its own fleet.
async fn fleet(dir: &std::path::Path) -> Result<(Daemon, Fleet), Box<dyn Error>> {
    let tls = Tls::create(dir)?;
    let config = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"both\"\n\n[node]\nid = \"node-1\"\n\
         \n[membership]\nheartbeat_interval_ms = 100\n{}",
        dir.join("node-1"),
        tls.node_table("node-1")?,
    );
    let daemon = Daemon::start(Config::from_toml(&config)?).await?;
    let control = daemon
        .control_address()
        .ok_or("no control address")?
        .to_string();
    let admin = fleetringd::data::Client::new(control.clone(), tls.client_network("tester")?);
    tokio::time::timeout(HANG_GUARD, async {
        loop {
            match admin.status().await {
                Ok(view) if view.members().len() == 1 => return,
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .map_err(|_| "the node never joined its own fleet")?;
    Ok((daemon, Fleet { tls, control }))
}

/// One replica's shape in a placement.
fn replica_shape() -> Value {
    json!({
        "address": "string",
        "liveness": "string",
        "node": "string",
        "position": "string",
        "reported_role": "string",
    })
}

/// A one-replica placement's shape: no candidate, no followers.
fn placement_shape() -> Value {
    json!({
        "acks": "string",
        "candidate": "null",
        "engine": "string",
        "followers": [],
        "lease_epoch": "number",
        "min_replicas": "number",
        "primary": "string",
        "replicas": [replica_shape()],
        "replication_factor": "number",
    })
}

fn member_shape() -> Value {
    json!({
        "address": "string",
        "draining": "bool",
        "host": "null",
        "liveness": "string",
        "node": "string",
        "release": "string",
        "weight": "number",
        "zone": "null",
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_read_command_prints_json_with_pinned_fields() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (daemon, fleet) = fleet(dir.path()).await?;
    let checks = tokio::task::spawn_blocking(move || -> Sendable {
        for args in [
            &["db", "create", "notes", "--replication-factor", "1"][..],
            &["db", "exec", "notes", "CREATE TABLE t (a, b)"],
            &[
                "db",
                "exec",
                "notes",
                "INSERT INTO t VALUES (?, ?)",
                "1",
                "blob:00ff",
            ],
        ] {
            let output = fleet.run(args)?;
            assert!(output.status.success(), "{args:?}: {}", stderr(&output));
        }

        // The replica reports its role and position with its next heartbeats; until then they
        // are null, or the log's beginning, and the snapshot below pins them as strings.
        for _ in 0..POLLS {
            let located = fleet.json(&["db", "locate", "notes", "--json"])?;
            let replica = &located["placement"]["replicas"][0];
            if replica["reported_role"].is_string()
                && replica["position"]
                    .as_str()
                    .is_some_and(|position| !position.is_empty())
            {
                break;
            }
            std::thread::sleep(POLL);
        }

        let status = fleet.json(&["status", "--json"])?;
        assert_eq!(
            shape(&status),
            json!({
                "dev_mode": "bool",
                "members": [member_shape()],
                "membership_epoch": "number",
                "ring_fingerprint": "string",
            })
        );
        assert_eq!(status["members"][0]["node"], "node-1");
        assert_eq!(status["members"][0]["liveness"], "live");

        let node_status = fleet.json(&["status", "--address", &fleet.control, "-o", "json"])?;
        assert_eq!(
            shape(&node_status),
            json!({
                "databases": [{
                    "database": "string",
                    "lease_epoch": "number",
                    "position": "string",
                    "role": "string",
                }],
                "membership_epoch": "number",
                "node": "string",
                "orphans": [],
                "ring_fingerprint": "string",
                "sinks": [],
            })
        );
        assert_eq!(node_status["databases"][0]["role"], "primary");

        let located = fleet.json(&["db", "locate", "notes", "--json"])?;
        let location_shape = json!({
            "database": "string",
            "metadata": "null",
            "placement": placement_shape(),
        });
        assert_eq!(shape(&located), location_shape);
        assert_eq!(located["database"], "notes");

        let listed = fleet.json(&["db", "list", "--json"])?;
        assert_eq!(shape(&listed), json!({ "databases": [location_shape] }));

        let rows = fleet.json(&["db", "query", "notes", "SELECT a, b, NULL FROM t", "--json"])?;
        assert_eq!(
            rows,
            json!({
                "columns": ["a", "b", "NULL"],
                "rows": [[1, { "blob": "AP8=" }, null]],
                "truncated": false,
            })
        );

        let verified = fleet.json(&["db", "verify", "notes", "--json"])?;
        assert_eq!(
            shape(&verified),
            json!({
                "agree": "bool",
                "chain_hash": "string",
                "database": "string",
                "expected": "null",
                "position": "string",
                "replicas": [{ "chain_hash": "string", "error": "null", "node": "string" }],
                "unchanged": "null",
            })
        );
        assert_eq!(verified["agree"], true);
        let hash = verified["chain_hash"].as_str().ok_or("a hash")?.to_owned();
        // Nothing is written since, so the latest position every replica holds is the same.
        let expected = fleet.json(&["db", "verify", "notes", "--expect", &hash, "--json"])?;
        assert_eq!(expected["unchanged"], true);
        assert_eq!(expected["expected"], hash.as_str());

        let shown = fleet.json(&["db", "show", "notes", "--json"])?;
        assert_eq!(
            shape(&shown)["placement"],
            placement_shape(),
            "db show and db locate share the placement"
        );

        let nodes = fleet.json(&["node", "list", "--json"])?;
        assert_eq!(shape(&nodes), json!({ "nodes": [member_shape()] }));

        let orphans = fleet.json(&["orphan", "list", "--json"])?;
        assert_eq!(orphans, json!({ "orphans": [] }));

        // jsonl is the same object on one line.
        let line = fleet.run(&["db", "locate", "notes", "-o", "jsonl"])?;
        assert!(line.status.success(), "{}", stderr(&line));
        assert_eq!(stdout(&line).lines().count(), 1, "{}", stdout(&line));
        let line: Value = serde_json::from_slice(&line.stdout)?;
        assert_eq!(shape(&line), shape(&located));
        assert_eq!(line["database"], "notes");
        Ok(())
    })
    .await;
    daemon.run_until(async {}).await;
    Ok(checks?.map_err(|error| error.to_string())?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_watch_prints_one_object_per_line_and_ends_when_stdout_closes() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (daemon, fleet) = fleet(dir.path()).await?;
    let checks = tokio::task::spawn_blocking(move || -> Sendable {
        let created = fleet.run(&["db", "create", "notes", "--replication-factor", "1"])?;
        assert!(created.status.success(), "{}", stderr(&created));
        let mut watch = fleet
            .command(&["lease", "watch", "--address", &fleet.control, "-o", "jsonl"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let reader = watch.stdout.take().ok_or("no stdout")?;
        let mut first = String::new();
        BufReader::new(reader).read_line(&mut first)?;
        // The reader is gone now: the next line the watch writes ends it, with 0.
        let held: Value = serde_json::from_str(&first)?;
        assert_eq!(
            shape(&held),
            json!({
                "database": "string",
                "event": "string",
                "lease_epoch": "number",
                "node": "string",
            })
        );
        assert_eq!(held["event"], "held");
        assert_eq!(held["node"], "node-1");
        assert_eq!(held["database"], "notes");
        // A second database's lease is an event the watch writes into the closed pipe.
        let second = fleet.run(&["db", "create", "more", "--replication-factor", "1"])?;
        assert!(second.status.success(), "{}", stderr(&second));
        let status = exited(&mut watch, "the watch, once its reader went away,")?;
        assert_eq!(status.code(), Some(0), "{status:?}");
        Ok(())
    })
    .await;
    daemon.run_until(async {}).await;
    Ok(checks?.map_err(|error| error.to_string())?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exit_codes_say_what_happened() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (daemon, fleet) = fleet(dir.path()).await?;
    let unused = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let checks = tokio::task::spawn_blocking(move || -> Sendable {
        let created = fleet.run(&["db", "create", "notes", "--replication-factor", "1"])?;
        assert_eq!(created.status.code(), Some(0), "{}", stderr(&created));

        // 1: a database that doesn't exist, with the next step.
        let missing = fleet.run(&["db", "locate", "nothing"])?;
        assert_eq!(missing.status.code(), Some(1), "{}", stderr(&missing));
        assert!(
            stderr(&missing).contains(
                "hint: list databases with `fleetring db list`, or create it with `fleetring db \
                 create nothing`"
            ),
            "{}",
            stderr(&missing)
        );
        assert!(stdout(&missing).is_empty(), "{}", stdout(&missing));

        // 2: a usage error, and a confirmation without a terminal.
        let usage = fleet.run(&["db", "list", "--no-such-flag"])?;
        assert_eq!(usage.status.code(), Some(2), "{}", stderr(&usage));
        let unconfirmed = fleet.run(&["db", "drop", "notes"])?;
        assert_eq!(
            unconfirmed.status.code(),
            Some(2),
            "{}",
            stderr(&unconfirmed)
        );
        assert!(
            stderr(&unconfirmed).contains("pass --yes"),
            "{}",
            stderr(&unconfirmed)
        );
        let drain = fleet.run(&["node", "drain", "node-1"])?;
        assert_eq!(drain.status.code(), Some(2), "{}", stderr(&drain));
        assert!(
            stderr(&drain).contains("node-1 holds the only replica of 'notes'"),
            "{}",
            stderr(&drain)
        );
        let revoke = fleet.run(&["token", "revoke", "--key-id", "k1"])?;
        assert_eq!(revoke.status.code(), Some(2), "{}", stderr(&revoke));
        assert!(
            stderr(&revoke).contains("pass --yes"),
            "{}",
            stderr(&revoke)
        );
        let kept = fleet.run(&["db", "locate", "notes"])?;
        assert_eq!(
            kept.status.code(),
            Some(0),
            "nothing was dropped: {}",
            stderr(&kept)
        );

        // 3: a token whose scopes don't allow the call, with the --token warning.
        let issued = fleet.run(&[
            "token",
            "issue",
            "--principal",
            "tester",
            "--scope",
            "read:nothing",
            "--dir",
            &fleet.tls.dir().display().to_string(),
        ])?;
        assert!(issued.status.success(), "{}", stderr(&issued));
        let token = stdout(&issued).trim().to_owned();
        let refused = fleet.run(&["db", "create", "denied", "--token", &token])?;
        assert_eq!(refused.status.code(), Some(3), "{}", stderr(&refused));
        assert!(
            stderr(&refused).contains(
                "warning: --token on the command line is visible to other processes; use \
                 FLEETRING_TOKEN or --token-file"
            ),
            "{}",
            stderr(&refused)
        );
        assert!(
            stderr(&refused).contains("  hint: "),
            "{}",
            stderr(&refused)
        );
        let quiet = fleet.run(&["-q", "db", "create", "denied", "--token", &token])?;
        assert_eq!(quiet.status.code(), Some(3), "{}", stderr(&quiet));
        assert!(!stderr(&quiet).contains("warning:"), "{}", stderr(&quiet));

        // 4: nothing listens at the control address.
        let unavailable = fleet
            .command(&["db", "list"])
            .env("FLEETRING_CONTROL", unused.to_string())
            .output()?;
        assert_eq!(
            unavailable.status.code(),
            Some(4),
            "{}",
            stderr(&unavailable)
        );
        assert!(
            stderr(&unavailable).contains("hint: retry"),
            "{}",
            stderr(&unavailable)
        );
        Ok(())
    })
    .await;
    daemon.run_until(async {}).await;
    Ok(checks?.map_err(|error| error.to_string())?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quiet_leaves_the_result_and_errors_only() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (daemon, fleet) = fleet(dir.path()).await?;
    let checks = tokio::task::spawn_blocking(move || -> Sendable {
        // A replication factor the one-node fleet can't meet warns, unless -q.
        let loud = fleet.run(&["db", "create", "loud", "--replication-factor", "2"])?;
        assert!(loud.status.success(), "{}", stderr(&loud));
        assert!(
            stderr(&loud).contains("warning: loud has a replication factor of 2"),
            "{}",
            stderr(&loud)
        );
        let quiet = fleet.run(&["db", "create", "quiet", "--replication-factor", "2", "-q"])?;
        assert!(quiet.status.success(), "{}", stderr(&quiet));
        assert_eq!(stderr(&quiet), "", "-q leaves no warning");
        assert!(
            stdout(&quiet).starts_with("created quiet\n"),
            "{}",
            stdout(&quiet)
        );
        Ok(())
    })
    .await;
    daemon.run_until(async {}).await;
    Ok(checks?.map_err(|error| error.to_string())?)
}

/// 130: Ctrl-C ends a command that follows a stream, here `dev logs -f`, which needs no node.
#[cfg(unix)]
#[test]
fn an_interrupted_follow_exits_130() -> TestResult {
    use rustix::process::{Pid, Signal, kill_process};

    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join(fleetringd::dev::LOG_FILE), "a line\n")?;
    let mut follow = crate::support::fleetring()
        .args(["dev", "logs", "-f", "--data-dir"])
        .arg(dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut first = String::new();
    BufReader::new(follow.stdout.take().ok_or("no stdout")?).read_line(&mut first)?;
    assert_eq!(first, "a line\n");
    // Long enough for the command to listen for Ctrl-C once it has printed the log.
    std::thread::sleep(Duration::from_millis(500));
    let pid = Pid::from_raw(i32::try_from(follow.id())?).ok_or("no pid")?;
    kill_process(pid, Signal::INT)?;
    let status =
        exited(&mut follow, "dev logs -f, on Ctrl-C,").map_err(|error| error.to_string())?;
    assert_eq!(status.code(), Some(130), "{status:?}");
    Ok(())
}
