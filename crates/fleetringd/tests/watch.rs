//! FR-075 item 7: `fleetring db watch` (ADR-0045), against a control daemon and three data
//! daemons over real TCP with mutual TLS. Every data daemon names the `notes*` databases in a
//! logical file sink, so those record their row changes and `other` doesn't.

use std::error::Error;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{DatabaseName, Liveness};
use fleetringd::data::Client;
use fleetringd::engine::{Statement, Value};
use fleetringd::{Config, Daemon, Network};
use serde_json::Value as Json;
use tokio::io::{AsyncBufReadExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStderr, ChildStdout};

use crate::support::Tls;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only.
const HANG_GUARD: Duration = Duration::from_secs(60);

/// What #103 asks of a write's rows is that they arrive within a second of its acknowledgement;
/// the test allows three, for a loaded runner.
const WITHIN_ON_A_LOADED_RUNNER: Duration = Duration::from_secs(3);

const DATA: [&str; 3] = ["data-1", "data-2", "data-3"];

/// A principal that may read every database, and nothing else: no `stream`.
const READER: &str = "\n[[principal]]\nname = \"reader\"\nscopes = [\"read:*\"]\n";

fn config(
    dir: &Path,
    tls: &Tls,
    id: &str,
    role: &str,
    control: Option<String>,
) -> Result<String, Box<dyn Error>> {
    let control = control.map_or_else(String::new, |address| {
        format!("\n[control]\naddress = \"{address}\"\n")
    });
    let sink = if role == "data" {
        format!(
            "\n[[changes.sink]]\nname = \"log\"\nkind = \"file\"\nformat = \"logical\"\n\
             databases = [\"notes*\"]\npath = {:?}\n",
            dir.join(format!("{id}-sink"))
        )
    } else {
        String::new()
    };
    let replication = if role == "data" {
        "\n[replication]\nack_timeout_ms = 20000\n"
    } else {
        ""
    };
    Ok(format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"{role}\"\n\n[node]\nid = \"{id}\"\n\
         {replication}\n[membership]\nheartbeat_interval_ms = 100\n{}{READER}{sink}{control}",
        dir.join(id),
        tls.node_table(id)?,
    ))
}

/// A control daemon and three data daemons that have all joined and published one ring.
struct Fleet {
    control: Daemon,
    data: Vec<Daemon>,
    tls: Tls,
}

impl Fleet {
    async fn start(dir: &Path) -> Result<Self, Box<dyn Error>> {
        let tls = Tls::create(dir)?;
        tls.authority()
            .issue_client(&"reader".parse()?, Tls::validity())?
            .write(tls.dir(), "client.reader")?;
        let control = Daemon::start(Config::from_toml(&config(
            dir, &tls, "control", "control", None,
        )?)?)
        .await?;
        let address = control
            .control_address()
            .ok_or("no control address")?
            .to_string();
        let mut data = Vec::new();
        for id in DATA {
            let text = config(dir, &tls, id, "data", Some(address.clone()))?;
            data.push(Daemon::start(Config::from_toml(&text)?).await?);
        }
        let fleet = Self { control, data, tls };
        fleet.converge().await?;
        Ok(fleet)
    }

    async fn converge(&self) -> TestResult {
        let handle = self.control.control().ok_or("no control handle")?;
        let mut states = handle.watch();
        let state = tokio::time::timeout(
            HANG_GUARD,
            states.wait_for(|state| {
                state.members().len() == DATA.len()
                    && state.members().all(|member| {
                        state.liveness(member.id(), state.now()) == Some(Liveness::Live)
                    })
            }),
        )
        .await??
        .clone();
        let fingerprint = state.ring()?.fingerprint();
        for daemon in &self.data {
            let membership = daemon.membership().ok_or("no membership")?;
            tokio::time::timeout(
                HANG_GUARD,
                membership.wait_for(|view| view.fingerprint() == Some(fingerprint)),
            )
            .await?
            .ok_or("membership stopped")?;
        }
        Ok(())
    }

    fn control_address(&self) -> Result<String, Box<dyn Error>> {
        Ok(self
            .control
            .control_address()
            .ok_or("no control address")?
            .to_string())
    }

    fn client(&self) -> Result<Client, Box<dyn Error>> {
        Ok(Client::new(self.control_address()?, network(&self.tls)?))
    }

    /// `db watch <args>` as the principal `principal`, with stdout and stderr piped, killed if
    /// the test drops it.
    fn watch(&self, principal: &str, args: &[&str]) -> Result<Watch, Box<dyn Error>> {
        let mut child = support::fleetring_tokio()
            .env("FLEETRING_TLS_DIR", self.tls.dir())
            .args(["db", "watch"])
            .args(args)
            .args(["--control", &self.control_address()?])
            .args(identity(principal))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?).lines();
        let stderr = BufReader::new(child.stderr.take().ok_or("no stderr")?).lines();
        Ok(Watch {
            child,
            stdout,
            stderr,
        })
    }

    async fn stop(self) {
        for daemon in self.data {
            daemon.run_until(async {}).await;
        }
        self.control.run_until(async {}).await;
    }
}

/// Real TCP as `tester`, with a request timeout that outlasts a slow runner.
fn network(tls: &Tls) -> Result<Network, Box<dyn Error>> {
    tls.client_network_with(
        "tester",
        fleetringd::transport::TransportConfig {
            request_timeout: Duration::from_secs(60),
            ..fleetringd::transport::TransportConfig::default()
        },
    )
}

/// A running `db watch`.
struct Watch {
    child: Child,
    stdout: Lines<BufReader<ChildStdout>>,
    stderr: Lines<BufReader<ChildStderr>>,
}

impl Watch {
    /// Waits for the stderr line that starts with `prefix`, and returns it.
    async fn note(&mut self, prefix: &str) -> Result<String, Box<dyn Error>> {
        let mut before = Vec::new();
        loop {
            let line = tokio::time::timeout(HANG_GUARD, self.stderr.next_line())
                .await??
                .ok_or_else(|| {
                    format!("stderr ended before a line starting {prefix:?}: {before:?}")
                })?;
            if line.starts_with(prefix) {
                return Ok(line);
            }
            before.push(line);
        }
    }

    /// The next stdout line, within `limit`.
    async fn line(&mut self, limit: Duration) -> Result<String, Box<dyn Error>> {
        Ok(tokio::time::timeout(limit, self.stdout.next_line())
            .await
            .map_err(|_| format!("no line within {limit:?}"))??
            .ok_or("stdout ended")?)
    }

    /// The next stdout line as JSON.
    async fn json(&mut self) -> Result<Json, Box<dyn Error>> {
        Ok(serde_json::from_str(&self.line(HANG_GUARD).await?)?)
    }

    /// Stops the command and waits for it, so no process is left.
    async fn stop(mut self) -> TestResult {
        self.child.start_kill()?;
        tokio::time::timeout(HANG_GUARD, self.child.wait()).await??;
        Ok(())
    }
}

/// Runs `db watch <args>` to its end, as `principal`, and returns its exit code and stderr.
async fn watch_to_end(
    fleet: &Fleet,
    principal: &str,
    args: &[&str],
) -> Result<(Option<i32>, String), Box<dyn Error>> {
    let output = tokio::time::timeout(
        HANG_GUARD,
        support::fleetring_tokio()
            .env("FLEETRING_TLS_DIR", fleet.tls.dir())
            .args(["db", "watch"])
            .args(args)
            .args(["--control", &fleet.control_address()?])
            .args(identity(principal))
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    Ok((output.status.code(), String::from_utf8(output.stderr)?))
}

/// The flags that act as `principal`: none for `tester`, whose certificate is the TLS
/// directory's `client.crt`.
fn identity(principal: &str) -> Vec<String> {
    if principal == support::PRINCIPAL {
        Vec::new()
    } else {
        vec!["--as".to_owned(), principal.to_owned()]
    }
}

fn insert(value: i64) -> Statement {
    Statement::with_params(
        "INSERT INTO note (id, body) VALUES (?1, ?2)",
        [Value::Integer(value), Value::Text(format!("note {value}"))],
    )
}

fn name(text: &str) -> Result<DatabaseName, Box<dyn Error>> {
    Ok(text.parse()?)
}

/// The `id` each row change of `line` inserted.
fn inserted_ids(line: &Json) -> Vec<i64> {
    line["changes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|change| change["operation"] == "insert")
        .filter_map(|change| change["after"]["id"].as_i64())
        .collect()
}

/// A write's rows arrive within a second, in the JSON contract; a watch resumes after a printed
/// position; it is refused without `stream`, for a database that records nothing, after a
/// position no longer held, and past the log's end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn db_watch_streams_a_writes_rows_and_resumes_by_position() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let notes = name("notes")?;
    client
        .create_database(&notes, ReplicationFactor::ONE)
        .await?;
    client
        .execute(
            &notes,
            Statement::new("CREATE TABLE note (id INTEGER PRIMARY KEY, body TEXT)"),
        )
        .await?;

    // Acceptance: the write's rows arrive within a second of its acknowledgement.
    let mut watch = fleet.watch("tester", &["notes", "-o", "jsonl"])?;
    let opened = watch.note("watching notes after ").await?;
    assert!(opened.contains("capture mode after"), "{opened}");
    client.execute(&notes, insert(1)).await?;
    let acknowledged = tokio::time::Instant::now();
    let first: Json = serde_json::from_str(&watch.line(HANG_GUARD).await?)?;
    let took = acknowledged.elapsed();
    // The target is a second, and a quiet machine takes milliseconds. The bound is looser so a
    // loaded CI runner (Windows, emulation) doesn't fail it; a regression to a polling interval
    // or a missed wake-up would still exceed it.
    assert!(
        took <= WITHIN_ON_A_LOADED_RUNNER,
        "the row arrived after {took:?}"
    );

    // The JSON contract (ADR-0045, point 10; docs/cli.md).
    assert_eq!(first["database"], "notes");
    assert!(first["start"].is_string(), "{first}");
    assert!(first["position"].is_string(), "{first}");
    assert!(
        first["lease_epoch"]
            .as_u64()
            .is_some_and(|epoch| epoch >= 1),
        "{first}"
    );
    let change = &first["changes"][0];
    assert_eq!(change["table"], "note");
    assert_eq!(change["operation"], "insert");
    assert_eq!(change["key"], serde_json::json!({"id": 1}));
    assert_eq!(
        change["after"],
        serde_json::json!({"id": 1, "body": "note 1"})
    );
    assert_eq!(
        change["before"],
        Json::Null,
        "`after` mode records no old values"
    );
    assert_eq!(change["updated"], Json::Null);
    let fields: Vec<&str> = first
        .as_object()
        .ok_or("an object")?
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        fields,
        ["changes", "database", "lease_epoch", "position", "start"]
    );
    assert!(first["start"].as_str() < first["position"].as_str());

    client.execute(&notes, insert(2)).await?;
    let second = watch.json().await?;
    assert_eq!(inserted_ids(&second), [2]);
    assert!(
        second["start"].as_str() >= first["position"].as_str(),
        "in order, with no repeat"
    );
    watch.stop().await?;

    // Resuming after the first transaction's position starts with the second.
    let position = first["position"].as_str().ok_or("a position")?.to_owned();
    let mut resumed = fleet.watch("tester", &["notes", "--from", &position, "--json"])?;
    assert_eq!(inserted_ids(&resumed.json().await?), [2]);
    resumed.stop().await?;

    // Text: one line per transaction, the position first.
    let mut text = fleet.watch("tester", &["notes", "--from", &position])?;
    let line = text.line(HANG_GUARD).await?;
    assert_eq!(
        line,
        format!(
            "{} insert note (id=2) -> {{id=2, body='note 2'}}",
            second["position"].as_str().ok_or("a position")?
        )
    );
    text.stop().await?;

    // Refused without `stream`, which `read` doesn't grant: exit 3, naming the action.
    let (code, stderr) = watch_to_end(&fleet, "reader", &["notes"]).await?;
    assert_eq!(code, Some(3), "{stderr}");
    assert!(
        stderr.contains("WatchChanges needs stream on 'notes'"),
        "{stderr}"
    );

    // A database no logical sink names records nothing to watch.
    let other = name("other")?;
    client
        .create_database(&other, ReplicationFactor::ONE)
        .await?;
    let (code, stderr) = watch_to_end(&fleet, "tester", &["other"]).await?;
    assert_eq!(code, Some(1), "{stderr}");
    assert!(stderr.contains("records no row changes"), "{stderr}");

    // A position past the log's end is refused.
    let past = "ff".repeat(20);
    let (code, stderr) = watch_to_end(&fleet, "tester", &["notes", "--from", &past]).await?;
    assert_eq!(code, Some(1), "{stderr}");
    assert!(stderr.contains("is past the end of"), "{stderr}");
    // One that isn't a position of the log at all is refused as such, whatever it sorts after.
    let malformed = "ff".repeat(30);
    let (code, stderr) = watch_to_end(&fleet, "tester", &["notes", "--from", &malformed]).await?;
    assert_eq!(code, Some(1), "{stderr}");
    assert!(
        stderr.contains("isn't a position of notes's log"),
        "{stderr}"
    );

    // Once the sink has delivered and released them, the changes after the first transaction's
    // start are pruned within 64 writes: a watch from there is refused, naming the oldest
    // position held, and never skips ahead.
    for round in 0..2 {
        for value in 0..70 {
            client
                .execute(&notes, insert(1_000 * (round + 1) + value))
                .await?;
        }
        let primary = client.locate(&notes).await?.primary.ok_or("a primary")?;
        let streams = fleet
            .data
            .iter()
            .find(|daemon| daemon.config().node.id == primary)
            .and_then(Daemon::change_streams)
            .ok_or("the primary's change streams")?;
        assert!(streams.await_caught_up(HANG_GUARD).await);
    }
    let start = first["start"].as_str().ok_or("a start")?;
    let (code, stderr) = watch_to_end(&fleet, "tester", &["notes", "--from", start]).await?;
    assert_eq!(code, Some(1), "{stderr}");
    assert!(stderr.contains("are no longer held"), "{stderr}");
    assert!(
        stderr.contains("the oldest position its primary holds changes from is "),
        "{stderr}"
    );
    assert!(
        stderr.contains("hint: watch from the oldest position held"),
        "{stderr}"
    );

    fleet.stop().await;
    Ok(())
}

/// When a drain moves the primary, `db watch` resumes on the new one after the last position it
/// printed, and prints every write once, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn db_watch_follows_the_database_to_its_new_primary() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let notes = name("notes-moving")?;
    let location = client
        .create_database(&notes, ReplicationFactor::new(3).ok_or("zero")?)
        .await?;
    client
        .execute(
            &notes,
            Statement::new("CREATE TABLE note (id INTEGER PRIMARY KEY, body TEXT)"),
        )
        .await?;
    let primary = location.primary.ok_or("a primary")?;

    let mut watch = fleet.watch("tester", &["notes-moving", "-o", "jsonl"])?;
    watch.note("watching notes-moving after ").await?;
    client.execute(&notes, insert(1)).await?;
    let mut seen = inserted_ids(&watch.json().await?);

    client.drain_node(&primary).await?;
    // The write goes to whichever node serves the database once the lease has moved.
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    loop {
        match client.execute(&notes, insert(2)).await {
            Ok(_) => break,
            Err(status) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                drop(status);
            }
            Err(status) => return Err(status.into()),
        }
    }
    let moved = client.locate(&notes).await?;
    assert_ne!(moved.primary.as_ref(), Some(&primary), "the drain moved it");
    let resumed = watch.note("resumed notes-moving after ").await?;
    assert!(
        resumed.contains(&format!("epoch {}", moved.epoch.get())),
        "{resumed}"
    );
    let line = watch.json().await?;
    assert_eq!(line["lease_epoch"].as_u64(), Some(moved.epoch.get()));
    seen.extend(inserted_ids(&line));
    assert_eq!(seen, [1, 2], "every write once, in order");
    watch.stop().await?;
    fleet.stop().await;
    Ok(())
}
