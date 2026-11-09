//! FR-086: `fleetring db fork`, fork status in `db show` and `db list`, and the cancelling `db
//! drop` (`docs/cli.md`, section 15), driven through the real binary against one node.
//!
//! The tests that need a fork to progress are ignored until the fork data path lands (FR-085,
//! #135): a build without it doesn't report the `fork` feature, so no fork reaches `ready`. The
//! faults some of them need, a held fork reset and a dropped reply, are test-only hooks that
//! don't exist yet; each test names the hook it needs in a `TODO(#135)`.

use std::error::Error;
use std::process::{Output, Stdio};
use std::time::Duration;

use fleetring_control::fork::OperationId;
use fleetringd::data::Client;
use fleetringd::{Config, Daemon};
use serde_json::Value;

use crate::support::Tls;

mod support;

type TestResult = Result<(), Box<dyn Error>>;
type Sendable<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

/// Why the end-to-end tests wait.
const NEEDS_SEEDING: &str = "needs FR-085 seeding (#135)";

/// Guards against a hang only.
const HANG_GUARD: Duration = Duration::from_secs(60);

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// The exit code of `output`.
fn code(output: &Output) -> Option<i32> {
    output.status.code()
}

/// The command line against one node, as the test operator or as `other`.
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

    /// Runs `args` as the principal `other`, whose certificate the node's configuration knows.
    fn run_as_other(&self, args: &[&str]) -> Sendable<Output> {
        Ok(self.command(args).env("FLEETRING_AS", "other").output()?)
    }

    /// Runs `args`, which must succeed.
    fn ok(&self, args: &[&str]) -> Sendable<Output> {
        let output = self.run(args)?;
        assert!(
            output.status.success(),
            "{args:?}: {}{}",
            stdout(&output),
            stderr(&output)
        );
        Ok(output)
    }

    fn admin(&self) -> Result<Client, Box<dyn Error>> {
        Ok(Client::new(
            self.control.clone(),
            self.tls.client_network("tester")?,
        ))
    }
}

/// One node that is both the control plane and a data node, and a second principal, `other`,
/// who may administer everything.
async fn fleet(dir: &std::path::Path) -> Result<(Daemon, Fleet), Box<dyn Error>> {
    let tls = Tls::create(dir)?;
    tls.authority()
        .issue_client(&"other".parse()?, Tls::validity())?
        .write(tls.dir(), "client.other")?;
    let config = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"both\"\n\n[node]\nid = \"node-1\"\n\
         \n[membership]\nheartbeat_interval_ms = 100\n{}\
         \n[[principal]]\nname = \"other\"\nscopes = [\"admin:*\"]\n",
        dir.join("node-1"),
        tls.node_table("node-1")?,
    );
    let daemon = Daemon::start(Config::from_toml(&config)?).await?;
    let control = daemon
        .control_address()
        .ok_or("no control address")?
        .to_string();
    let fleet = Fleet { tls, control };
    let admin = fleet.admin()?;
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
    Ok((daemon, fleet))
}

/// A database `notes` with one row in it.
fn with_notes(fleet: &Fleet) -> Sendable {
    fleet.ok(&["db", "create", "notes", "--replication-factor", "1"])?;
    fleet.ok(&["db", "exec", "notes", "CREATE TABLE t (v)"])?;
    fleet.ok(&["db", "exec", "notes", "INSERT INTO t VALUES ('kept')"])?;
    Ok(())
}

/// The operation ID the command printed on stderr.
fn printed_operation(output: &Output) -> Sendable<OperationId> {
    let text = stderr(output);
    let id = text
        .lines()
        .find_map(|line| line.strip_prefix("operation "))
        .and_then(|rest| rest.split(':').next())
        .ok_or_else(|| format!("no operation line on stderr: {text}"))?;
    Ok(id.parse()?)
}

#[test]
fn a_bad_operation_id_or_two_expiries_are_usage_errors() -> Sendable {
    let command = |args: &[&str]| -> Sendable<Output> {
        let mut command = crate::support::fleetring();
        Ok(command.args(args).stdin(Stdio::null()).output()?)
    };
    let bad = command(&["db", "fork", "a", "b", "--operation-id", "not-an-id"])?;
    assert_eq!(code(&bad), Some(2), "{}", stderr(&bad));
    let both = command(&[
        "db",
        "fork",
        "a",
        "b",
        "--expires-in",
        "1h",
        "--expires-at",
        "2030-01-01T00:00:00Z",
    ])?;
    assert_eq!(code(&both), Some(2), "{}", stderr(&both));
    let short = command(&["db", "fork", "a", "b", "-d"])?;
    assert_eq!(code(&short), Some(2), "-d is dev's: {}", stderr(&short));
    Ok(())
}

#[test]
fn the_help_says_how_to_resume_cancel_and_how_long_an_id_lasts() -> Sendable {
    let mut command = crate::support::fleetring();
    let output = command.args(["db", "fork", "--help"]).output()?;
    assert!(output.status.success(), "{}", stderr(&output));
    let help = stdout(&output);
    for needed in [
        "Examples:",
        "--operation-id",
        "--detach",
        "7 days",
        "fleetring db drop <target>",
        "--timeout",
    ] {
        assert!(help.contains(needed), "{needed:?} missing from:\n{help}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_database_no_fork_made_shows_no_fork_and_lists_no_fork_column() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (daemon, fleet) = fleet(dir.path()).await?;
    let checks = tokio::task::spawn_blocking(move || -> Sendable {
        fleet.ok(&["db", "create", "plain", "--replication-factor", "1"])?;
        let shown = stdout(&fleet.ok(&["db", "show", "plain", "--no-schema"])?);
        assert!(!shown.contains("fork"), "{shown}");
        let listed = stdout(&fleet.ok(&["db", "list"])?);
        assert!(!listed.contains("FORK"), "{listed}");
        let json: Value = serde_json::from_str(&stdout(&fleet.ok(&["db", "list", "--json"])?))?;
        assert!(json["databases"][0]["fork"].is_null(), "{json}");
        assert_eq!(
            json["databases"][0]["placement"]["incarnation"]
                .as_str()
                .map(str::len),
            Some(16)
        );
        Ok(())
    });
    let checked = checks.await;
    daemon.run_until(async {}).await;
    checked?.map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs FR-085 seeding (#135)"]
async fn db_fork_prints_the_operation_id_waits_for_ready_and_exits_0() -> TestResult {
    let _ = NEEDS_SEEDING;
    let dir = tempfile::tempdir()?;
    let (daemon, fleet) = fleet(dir.path()).await?;
    let checks = tokio::task::spawn_blocking(move || -> Sendable {
        with_notes(&fleet)?;
        let forked = fleet.ok(&["db", "fork", "notes", "notes-exp"])?;
        let id = printed_operation(&forked)?;
        let text = stdout(&forked);
        assert!(text.contains("state: ready"), "{text}");
        assert!(text.contains(&id.to_string()), "{text}");
        let shown = stdout(&fleet.ok(&["db", "show", "notes-exp", "--no-schema"])?);
        assert!(shown.contains("fork: ready"), "{shown}");
        assert!(shown.contains("forked from: notes"), "{shown}");
        let queried = stdout(&fleet.ok(&["db", "query", "notes-exp", "SELECT v FROM t"])?);
        assert!(queried.contains("kept"), "{queried}");
        let listed = stdout(&fleet.ok(&["db", "list"])?);
        assert!(listed.contains("FORK"), "{listed}");
        Ok(())
    });
    let checked = checks.await;
    daemon.run_until(async {}).await;
    checked?.map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs FR-085 seeding (#135)"]
async fn db_fork_keeps_waiting_after_seeded_while_the_reset_is_held() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (daemon, fleet) = fleet(dir.path()).await?;
    // TODO(#135): hold the fork reset, a test-only hook on the data node that makes the primary
    // delay its fork-reset write until the test releases it.
    let checks = tokio::task::spawn_blocking(move || -> Sendable {
        with_notes(&fleet)?;
        let child = fleet
            .command(&["db", "fork", "notes", "notes-exp", "--timeout", "5s"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        // Seeded but not ready: the fork serves nobody.
        let mut opening = String::new();
        for _ in 0..200 {
            let shown = stdout(&fleet.ok(&["db", "show", "notes-exp", "--no-schema"])?);
            if shown.contains("opening: not yet usable") {
                opening = shown;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(opening.contains("opening: not yet usable"), "{opening}");
        let query = fleet.run(&["db", "query", "notes-exp", "SELECT 1"])?;
        assert!(!query.status.success());
        assert!(
            stderr(&query).contains("fork not ready"),
            "{}",
            stderr(&query)
        );
        // TODO(#135): release the held fork reset here, then the command exits 0.
        let output = child.wait_with_output()?;
        assert_eq!(
            code(&output),
            Some(4),
            "the wait ran out while the reset was held"
        );
        assert!(
            stderr(&output).contains("opening notes-exp"),
            "{}",
            stderr(&output)
        );
        Ok(())
    });
    let checked = checks.await;
    daemon.run_until(async {}).await;
    checked?.map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs FR-085 seeding (#135)"]
async fn a_lost_reply_is_resent_with_the_same_id_and_yields_one_fork() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (daemon, fleet) = fleet(dir.path()).await?;
    // TODO(#135): drop the first ForkDatabase reply, a test-only hook (or a proxy in front of
    // the control address) that lets the control plane record the fork and then fails the first
    // reply with UNAVAILABLE.
    let admin = fleet.admin()?;
    let checks = tokio::task::spawn_blocking(move || -> Sendable {
        with_notes(&fleet)?;
        let forked = fleet.ok(&["db", "fork", "notes", "notes-exp"])?;
        assert!(stdout(&forked).contains("state: ready"));
        let listed = stdout(&fleet.ok(&["db", "list", "--prefix", "notes-exp"])?);
        assert_eq!(listed.matches("notes-exp").count(), 1, "{listed}");
        Ok(())
    });
    let checked = checks.await;
    // One operation: reading it by the printed ID finds the one fork.
    let _ = admin;
    daemon.run_until(async {}).await;
    checked?.map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs FR-085 seeding (#135)"]
async fn an_operation_id_resumes_after_detach_and_a_changed_argument_is_a_conflict() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (daemon, fleet) = fleet(dir.path()).await?;
    let checks = tokio::task::spawn_blocking(move || -> Sendable {
        with_notes(&fleet)?;
        let detached = fleet.ok(&[
            "db",
            "fork",
            "notes",
            "notes-exp",
            "--replication-factor",
            "1",
            "--detach",
        ])?;
        assert!(stdout(&detached).contains("position: not yet"));
        let id = printed_operation(&detached)?.to_string();

        // Any other argument is refused, naming the conflict.
        let changed = fleet.run(&[
            "db",
            "fork",
            "notes",
            "notes-exp",
            "--replication-factor",
            "3",
            "--operation-id",
            &id,
        ])?;
        assert_eq!(code(&changed), Some(1), "{}", stderr(&changed));
        assert!(
            stderr(&changed).contains("was requested with --replication-factor 1, not 3"),
            "{}",
            stderr(&changed)
        );

        // The same arguments wait for the same operation.
        let resumed = fleet.ok(&[
            "db",
            "fork",
            "notes",
            "notes-exp",
            "--replication-factor",
            "1",
            "--operation-id",
            &id,
        ])?;
        assert!(stdout(&resumed).contains("state: ready"));
        assert!(stdout(&resumed).contains(&id));

        // Another principal's use of the ID is refused without revealing the operation.
        let other =
            fleet.run_as_other(&["db", "fork", "notes", "notes-exp", "--operation-id", &id])?;
        assert!(!other.status.success());
        let all = format!("{}{}", stdout(&other), stderr(&other));
        assert!(!all.contains("incarnation"), "{all}");
        assert!(!all.contains("position"), "{all}");
        Ok(())
    });
    let checked = checks.await;
    daemon.run_until(async {}).await;
    checked?.map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs FR-085 seeding (#135)"]
async fn dropping_the_target_during_seeding_cancels_the_fork() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (daemon, fleet) = fleet(dir.path()).await?;
    // TODO(#135): hold the seeding, a test-only hook on the data node that keeps a fork in
    // `seeding` until the test releases it, so that the drop lands while it is pending.
    let admin = fleet.admin()?;
    let checks = tokio::task::spawn_blocking(move || -> Sendable {
        with_notes(&fleet)?;
        let detached = fleet.ok(&["db", "fork", "notes", "notes-exp", "--detach"])?;
        let id = printed_operation(&detached)?;
        let shown = stdout(&fleet.ok(&["db", "show", "notes-exp", "--no-schema"])?);
        assert!(shown.contains("cancels the fork in progress"), "{shown}");
        let dropped = stdout(&fleet.ok(&["db", "drop", "notes-exp", "--yes"])?);
        assert!(
            dropped.contains(&format!("cancelled the fork in progress (operation {id})")),
            "{dropped}"
        );
        // The operation reports `cancelled`, and exits 1.
        let resumed = fleet.run(&[
            "db",
            "fork",
            "notes",
            "notes-exp",
            "--operation-id",
            &id.to_string(),
        ])?;
        assert_eq!(code(&resumed), Some(1));
        assert!(stdout(&resumed).contains("state: cancelled"));
        // A new ID starts afresh.
        let again = fleet.ok(&["db", "fork", "notes", "notes-exp"])?;
        assert!(stdout(&again).contains("state: ready"));
        Ok(())
    });
    let checked = checks.await;
    // TODO(#135): `admin.fork_operation(id)` reports `Cancelled` for the first operation.
    let _ = admin;
    daemon.run_until(async {}).await;
    checked?.map_err(|error| error.to_string())?;
    Ok(())
}
