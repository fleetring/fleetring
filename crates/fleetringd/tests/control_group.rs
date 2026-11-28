//! FR-064 PR 5: the control group with real daemons over TCP. A fleet grows from one control node
//! to three while a client writes, and a control node started on a 0.9.0 control log keeps its
//! fleet.

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{Command, ControlConfig, DatabaseName, Liveness, State, apply};
use fleetring_proto::v1::ListDatabasesRequest;
use fleetringd::control::ControlHandle;
use fleetringd::data::Client;
use fleetringd::engine::Statement;
use fleetringd::{Config, Daemon, Network};

mod support;

use support::Tls;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only.
const HANG_GUARD: Duration = Duration::from_secs(60);

/// How long a data node stays live without a heartbeat in these tests.
const HEARTBEAT_TIMEOUT_MS: u64 = 2_000;

/// The longest gap between two acknowledged writes the growth may cause: one longest election
/// timeout (twice the default shortest, 1 s) plus a lease renewal (a fifth of the default 10 s
/// lease), as the plan's acceptance criterion says.
const LONGEST_WRITE_GAP: Duration = Duration::from_millis(2_000 + 2_000);

/// The configuration of node `id` with `role`, its `[control]` table holding `control`.
fn config(
    dir: &Path,
    tls: &Tls,
    id: &str,
    role: &str,
    control: &str,
) -> Result<Config, Box<dyn Error>> {
    Ok(Config::from_toml(&format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"{role}\"\n\n[node]\nid = \"{id}\"\n\n\
         [membership]\nheartbeat_interval_ms = 100\n\n\
         [control]\nheartbeat_timeout_ms = {HEARTBEAT_TIMEOUT_MS}\n{control}\n{}",
        dir.join(id),
        tls.node_table(id)?
    ))?)
}

/// A control node `id`, which starts a group (`new`) or waits to be added (`join`).
async fn control(
    dir: &Path,
    tls: &Tls,
    id: &str,
    bootstrap: &str,
) -> Result<Daemon, Box<dyn Error>> {
    let bootstrap = format!("bootstrap = \"{bootstrap}\"");
    Ok(Daemon::start(config(dir, tls, id, "control", &bootstrap)?).await?)
}

/// A data node `id` that knows the control plane only at `control`, once it has joined.
async fn data(
    dir: &Path,
    tls: &Tls,
    id: &str,
    control: SocketAddr,
) -> Result<Daemon, Box<dyn Error>> {
    let address = format!("address = \"{control}\"");
    let daemon = Daemon::start(config(dir, tls, id, "data", &address)?).await?;
    let membership = daemon.membership().ok_or("no membership")?;
    tokio::time::timeout(
        HANG_GUARD,
        membership.wait_for(|view| view.ring().is_some()),
    )
    .await?
    .ok_or("membership stopped")?;
    Ok(daemon)
}

fn handle(daemon: &Daemon) -> Result<ControlHandle, Box<dyn Error>> {
    Ok(daemon.control().ok_or("not a control node")?)
}

/// The operator's client, `tester`, reaching the control plane at `control`.
fn operator(tls: &Tls, control: SocketAddr) -> Result<Client, Box<dyn Error>> {
    let network: Network = tls.client_network("tester")?;
    Ok(Client::new(control.to_string(), network))
}

/// Waits until `check` holds, polling, for at most [`HANG_GUARD`].
async fn until(what: &str, mut check: impl FnMut() -> bool) -> TestResult {
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    while !check() {
        if tokio::time::Instant::now() > deadline {
            return Err(format!("timed out waiting until {what}").into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// `c1` alone, then `c2` and `c3` added with `control add` and made voters with `control
/// set-voters`, while a client keeps writing to a database on `d1` and `d2`. Then `c1` stops: it
/// hands its leadership over, and the data nodes, configured with `c1`'s address alone, reach the
/// new leader through the group they learned from their heartbeat replies. No write fails for
/// longer than one election plus a lease renewal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fleet_grows_from_one_control_node_to_three_while_writing() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let c1 = control(dir.path(), &tls, "c1", "new").await?;
    let at = c1.listen_address();
    let d1 = data(dir.path(), &tls, "d1", at).await?;
    let d2 = data(dir.path(), &tls, "d2", at).await?;
    let client = operator(&tls, at)?;
    let notes: DatabaseName = "notes".parse()?;
    let created = client
        .create_database(&notes, ReplicationFactor::new(2).ok_or("zero")?)
        .await?;
    // The writer goes straight to the primary, as an application holding the placement does;
    // it needs the control plane only through the primary's lease renewals.
    let primary = created
        .primary_replica()
        .and_then(|replica| replica.address.clone())
        .ok_or("no primary address")?;
    client
        .execute(&notes, Statement::new("CREATE TABLE notes (body TEXT)"))
        .await?;

    // Writes go on, each timed, until the growth is done.
    let writing = Arc::new(AtomicBool::new(true));
    let writer = {
        let writing = Arc::clone(&writing);
        let client = client.clone().with_entry(primary);
        let notes = notes.clone();
        tokio::spawn(async move {
            let mut last = tokio::time::Instant::now();
            let mut longest = Duration::ZERO;
            let mut written = 0_u32;
            while writing.load(Ordering::SeqCst) {
                let wrote = client
                    .execute(&notes, Statement::new("INSERT INTO notes VALUES ('x')"))
                    .await;
                if wrote.is_ok() {
                    longest = longest.max(last.elapsed());
                    last = tokio::time::Instant::now();
                    written += 1;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            (longest.max(last.elapsed()), written)
        })
    };

    let c2 = control(dir.path(), &tls, "c2", "join").await?;
    let c3 = control(dir.path(), &tls, "c3", "join").await?;
    for (id, daemon) in [("c2", &c2), ("c3", &c3)] {
        let group = client
            .add_control_node(&id.parse()?, &daemon.listen_address().to_string())
            .await?;
        assert!(
            group
                .members
                .iter()
                .any(|member| member.node == id && !member.voter),
            "{group:?}"
        );
    }
    let voters: Vec<NodeId> = ["c1", "c2", "c3"]
        .iter()
        .map(|id| id.parse())
        .collect::<Result<_, _>>()?;
    let group = client.set_control_voters(&voters).await?;
    assert_eq!(
        group.members.iter().filter(|member| member.voter).count(),
        3,
        "{group:?}"
    );
    // Every data node hears the group in a heartbeat reply, ten heartbeats on, before `c1` goes.
    tokio::time::sleep(Duration::from_secs(1)).await;

    tokio::time::timeout(HANG_GUARD, c1.run_until(async {})).await?;
    until("c2 or c3 leads", || {
        [&c2, &c3]
            .iter()
            .any(|daemon| handle(daemon).is_ok_and(|control| control.leadership().is_ready()))
    })
    .await?;
    let leader = if handle(&c2)?.leadership().is_ready() {
        &c2
    } else {
        &c3
    };
    // Each data node keeps heartbeating to the new leader, at an address it learned: past a
    // heartbeat timeout after `c1` stopped, both are still live in the new leader's state.
    tokio::time::sleep(Duration::from_millis(HEARTBEAT_TIMEOUT_MS * 2)).await;
    let state = handle(leader)?.state();
    for id in ["d1", "d2"] {
        assert_eq!(
            state.liveness(&id.parse()?, state.now()),
            Some(Liveness::Live),
            "{id} reaches the new leader"
        );
    }

    writing.store(false, Ordering::SeqCst);
    let (longest, written) = writer.await?;
    assert!(written > 0, "no write was acknowledged");
    assert!(
        longest < LONGEST_WRITE_GAP,
        "writes failed for {longest:?} in a row"
    );
    for daemon in [d1, d2, c2, c3] {
        tokio::time::timeout(HANG_GUARD, daemon.run_until(async {})).await?;
    }
    Ok(())
}

/// A control node started on the control log 0.9.0's code wrote migrates it to a one-voter
/// group, and serves its fleet: every database at its epoch, with itself as the only voter and
/// the leader.
#[tokio::test]
async fn a_migrated_control_node_keeps_its_fleet() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/control-0.9.0");
    let data_dir = dir.path().join("c1");
    std::fs::create_dir_all(&data_dir)?;
    std::fs::copy(fixture.join("control.db"), data_dir.join("control.db"))?;
    let c1 = control(dir.path(), &tls, "c1", "new").await?;
    let commands: Vec<Command> =
        serde_json::from_str(&std::fs::read_to_string(fixture.join("commands.json"))?)?;
    let mut replayed = State::new(ControlConfig::new());
    for command in commands {
        let _outcome = apply(&mut replayed, command);
    }
    let client = tls
        .client_network("tester")?
        .control_client(c1.listen_address().to_string());
    let databases = client
        .list_databases(ListDatabasesRequest::default())
        .await?
        .databases;
    let mut expected: Vec<(String, u64)> = replayed
        .databases()
        .map(|database| (database.name().to_string(), database.epoch().get()))
        .collect();
    expected.sort();
    let mut got: Vec<(String, u64)> = databases
        .into_iter()
        .map(|placement| (placement.database, placement.epoch))
        .collect();
    got.sort();
    assert_eq!(got, expected);
    let group = operator(&tls, c1.listen_address())?.control_group().await?;
    assert_eq!(group.members.len(), 1, "{group:?}");
    let only = &group.members[0];
    assert!(only.node == "c1" && only.voter && only.leader, "{group:?}");
    tokio::time::timeout(HANG_GUARD, c1.run_until(async {})).await?;
    Ok(())
}

/// Runs `fleetring control <args>` against the control plane at `control`, as the test's
/// operator, with no terminal on stdin.
async fn fleetring_control(
    tls: &Tls,
    control: SocketAddr,
    args: &[&str],
) -> Result<std::process::Output, Box<dyn Error>> {
    let tls_dir = tls.dir().to_owned();
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    Ok(tokio::task::spawn_blocking(move || {
        support::fleetring()
            .env("FLEETRING_TLS_DIR", tls_dir)
            .arg("control")
            .args(&args)
            .args(["--control", &control.to_string()])
            .stdin(std::process::Stdio::null())
            .output()
    })
    .await??)
}

/// `control add`, `control set-voters` and `control remove` as the binary runs them: an add
/// prints the new learner, one voter warns that it has no redundancy, and a removal without a
/// terminal refuses with exit code 2 unless `--yes`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_control_commands_change_the_group() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let c1 = control(dir.path(), &tls, "c1", "new").await?;
    let c2 = control(dir.path(), &tls, "c2", "join").await?;
    let at = c1.listen_address();
    let c2_address = c2.listen_address().to_string();

    let added = fleetring_control(&tls, at, &["add", "c2", "--address", &c2_address]).await?;
    let stdout = String::from_utf8(added.stdout)?;
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    let row: Vec<&str> = stdout
        .lines()
        .find(|line| line.starts_with("c2 "))
        .ok_or("no row for c2")?
        .split_whitespace()
        .collect();
    assert_eq!(
        row[..4],
        ["c2", c2_address.as_str(), "no", "no"],
        "{stdout}"
    );

    let one = fleetring_control(&tls, at, &["set-voters", "--voter", "c1"]).await?;
    assert!(
        one.status.success(),
        "{}",
        String::from_utf8_lossy(&one.stderr)
    );
    assert!(
        String::from_utf8_lossy(&one.stderr)
            .contains("warning: a control group of one voter has no redundancy"),
        "{}",
        String::from_utf8_lossy(&one.stderr)
    );

    let unconfirmed = fleetring_control(&tls, at, &["remove", "c2"]).await?;
    assert_eq!(
        unconfirmed.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&unconfirmed.stderr)
    );
    assert!(handle(&c1)?.group().contains("c2"));
    let removed = fleetring_control(&tls, at, &["remove", "c2", "--yes", "-o", "json"]).await?;
    assert!(
        removed.status.success(),
        "{}",
        String::from_utf8_lossy(&removed.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&removed.stdout)?;
    let nodes = parsed["nodes"].as_array().ok_or("no nodes")?;
    assert_eq!(nodes.len(), 1, "{parsed}");
    assert_eq!(parsed["joint"], false, "{parsed}");
    for daemon in [c1, c2] {
        tokio::time::timeout(HANG_GUARD, daemon.run_until(async {})).await?;
    }
    Ok(())
}
