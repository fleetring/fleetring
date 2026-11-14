//! FR-022: configuration, the data directory lock, and the binary's lifecycle.

use std::error::Error;
use std::time::Duration;

use fleetringd::replication::ReplicationSettings;
use fleetringd::{Config, ConfigError, DataDir, DataDirError, NodeRole};

mod support;

type TestResult = Result<(), Box<dyn Error>>;

const MINIMAL: &str = r#"
data_dir = "/tmp/fleetring-test"
listen = "127.0.0.1:7400"

[node]
id = "laptop-1"
"#;

#[test]
fn a_minimal_config_defaults_to_both_roles_and_weight_one() -> TestResult {
    let config = Config::from_toml(MINIMAL)?;
    assert_eq!(config.node.id.as_str(), "laptop-1");
    assert_eq!(config.node.weight.get(), 1);
    assert_eq!(config.role, NodeRole::Both);
    assert!(config.role.hosts_data() && config.role.runs_control());
    assert_eq!(config.control.address, None);
    assert_eq!(config.control, fleetringd::ControlSettings::default());
    Ok(())
}

#[test]
fn invalid_fields_are_named_in_the_error() {
    let cases = [
        (MINIMAL.replace("laptop-1", "Laptop-1"), "id"),
        (
            MINIMAL.replace("listen = \"127.0.0.1:7400\"\n", ""),
            "listen",
        ),
        (MINIMAL.replace("listen", "listne"), "listne"),
        (format!("{MINIMAL}weight = 0\n"), "weight"),
    ];
    for (text, field) in cases {
        let error = Config::from_toml(&text).expect_err(field);
        assert!(matches!(error, ConfigError::Parse(_)), "{error}");
        assert!(error.to_string().contains(field), "{field}: {error}");
    }
}

#[test]
fn the_replication_table_sets_replication_settings() -> TestResult {
    let defaults = Config::from_toml(MINIMAL)?.replication.settings();
    assert_eq!(defaults, ReplicationSettings::new());

    let text = format!(
        "{MINIMAL}\n[replication]\nack_timeout_ms = 750\ncheckpoint_threshold_bytes = 8192\n"
    );
    let settings = Config::from_toml(&text)?.replication.settings();
    assert_eq!(
        settings,
        ReplicationSettings {
            ack_timeout: Duration::from_millis(750),
            checkpoint_threshold: 8192,
            ..ReplicationSettings::new()
        }
    );

    let text = format!("{MINIMAL}\n[replication]\nretry_interval_ms = 0\n");
    let error = Config::from_toml(&text).expect_err("a zero retry interval");
    assert!(
        matches!(
            error,
            ConfigError::Invalid {
                field: "replication.retry_interval_ms",
                ..
            }
        ),
        "{error}"
    );
    Ok(())
}

#[test]
fn principals_are_parsed_and_validated_at_start() -> TestResult {
    let text = format!(
        "{MINIMAL}\n[[principal]]\nname = \"ops\"\nscopes = [\"admin:*\", \"fleet\"]\n\n\
         [[principal]]\nname = \"router\"\nforward_tokens = true\n"
    );
    let config = Config::from_toml(&text)?;
    assert_eq!(config.principals.len(), 2);
    assert_eq!(config.principals[0].name.as_str(), "ops");
    let scopes: Vec<String> = config.principals[0]
        .scopes
        .as_slice()
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(scopes, ["admin:*", "fleet"]);
    assert!(config.principals[1].scopes.is_empty());
    assert_eq!(
        config.principals[1].forward_tokens,
        fleetringd::TokenForwarding::Allowed
    );
    assert!(Config::from_toml(MINIMAL)?.principals.is_empty());

    for (bad, named) in [
        ("scopes = [\"delete:*\"]", "delete"),
        ("scopes = [\"read:\"]", "empty pattern"),
        ("scopes = [\"read\"]", "<action>:<pattern>"),
        // 0.2 spelled the fleet scope `cluster`; 0.3 refuses it and names `fleet`.
        ("scopes = [\"cluster\"]", "`fleet`, not 'cluster'"),
        ("role = \"admin\"", "role"),
    ] {
        let text = format!("{MINIMAL}\n[[principal]]\nname = \"ops\"\n{bad}\n");
        let error = Config::from_toml(&text).expect_err(bad);
        assert!(error.to_string().contains(named), "{bad}: {error}");
    }
    let text = format!("{MINIMAL}\n[[principal]]\nname = \"Ops\"\n");
    assert!(Config::from_toml(&text).is_err(), "an invalid name");

    let text =
        format!("{MINIMAL}\n[[principal]]\nname = \"ops\"\n\n[[principal]]\nname = \"ops\"\n");
    let error = Config::from_toml(&text).expect_err("a duplicate principal");
    assert!(
        matches!(
            error,
            ConfigError::Invalid {
                field: "principal",
                ..
            }
        ),
        "{error}"
    );
    Ok(())
}

#[test]
fn a_data_only_node_needs_the_control_address() {
    let text = MINIMAL.replace("listen", "role = \"data\"\nlisten");
    let error = Config::from_toml(&text).expect_err("data role without control");
    assert!(
        matches!(
            error,
            ConfigError::Missing {
                field: "control.address",
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn a_locked_data_directory_refuses_a_second_daemon() -> TestResult {
    let dir = tempfile::tempdir()?;
    let first = DataDir::lock(dir.path())?;
    let second = DataDir::lock(dir.path()).expect_err("second lock");
    assert!(matches!(second, DataDirError::InUse { .. }), "{second}");
    drop(first);
    // A child process forked by another test in this binary holds copies of our descriptors until
    // it execs, which can briefly keep the dropped lock alive, so allow the release a moment.
    for _ in 0..50 {
        match DataDir::lock(dir.path()) {
            Ok(_) => return Ok(()),
            Err(DataDirError::InUse { .. }) => {
                std::thread::sleep(std::time::Duration::from_millis(20))
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err("the data directory stayed locked after the first lock was dropped".into())
}

/// A daemon that stopped has freed its port by the time it returns, so a node started again on
/// it in the same process binds it at once, while a connection to the old one lingers.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_daemon_frees_its_port_before_it_returns() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = support::Tls::create(dir.path())?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n{}",
        dir.path().join("data"),
        tls.node_table("control-1")?
    );
    let daemon = fleetringd::Daemon::start(Config::from_toml(&text)?).await?;
    let address = daemon.listen_address();
    // A connection that is still open when the daemon stops.
    let lingering = tokio::net::TcpStream::connect(address).await?;
    daemon.run_until(async {}).await;
    let rebound = tokio::net::TcpListener::bind(address).await;
    assert!(rebound.is_ok(), "{address}: {rebound:?}");
    drop(lingering);
    Ok(())
}

/// `fleetring serve --check` (#95): whether a node would start with a configuration and its
/// flags, without starting anything.
mod check {
    use std::path::{Path, PathBuf};
    use std::process::Output;

    use super::TestResult;

    /// Writes `text` as `dir/node.toml`, with `data_dir` set to `dir/data`, which nothing creates.
    fn write(dir: &Path, text: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let path = dir.join("node.toml");
        let data_dir = dir.join("data");
        std::fs::write(&path, format!("data_dir = {data_dir:?}\n{text}"))?;
        Ok(path)
    }

    /// A configuration a node starts with: plaintext, so it names no file, on `listen`.
    fn valid(listen: &str) -> String {
        format!(
            "listen = \"{listen}\"\ninsecure_plaintext = true\n\n[node]\nid = \"node-1\"\n\n\
             [admin]\nenabled = false\n"
        )
    }

    fn check(config: &Path, extra: &[&str]) -> Result<Output, Box<dyn std::error::Error>> {
        Ok(crate::support::fleetring()
            .args(["serve", "--config"])
            .arg(config)
            .arg("--check")
            .args(extra)
            .output()?)
    }

    fn text(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).into_owned()
    }

    #[test]
    fn a_valid_configuration_passes_and_nothing_is_started_or_created() -> TestResult {
        let dir = tempfile::tempdir()?;
        // The listen address is taken: a check binds nothing, so it doesn't notice.
        let taken = std::net::TcpListener::bind("127.0.0.1:0")?;
        let config = write(dir.path(), &valid(&taken.local_addr()?.to_string()))?;
        let output = check(&config, &[])?;
        assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
        assert_eq!(
            text(&output.stdout),
            format!("{}: valid for node node-1, role both\n", config.display())
        );
        assert_eq!(text(&output.stderr), "");
        assert!(
            !dir.path().join("data").exists(),
            "the data directory was made"
        );
        // The flags apply as they do to a start, and the 0.6 form reads as `serve`.
        let flagged = crate::support::fleetring()
            .arg("--config")
            .arg(&config)
            .args(["--node-id", "node-9", "--role", "control", "--check"])
            .output()?;
        assert_eq!(
            text(&flagged.stdout),
            format!(
                "{}: valid for node node-9, role control\n",
                config.display()
            ),
            "{}",
            text(&flagged.stderr)
        );
        Ok(())
    }

    #[test]
    fn an_invalid_configuration_fails_naming_the_file_and_the_field() -> TestResult {
        let dir = tempfile::tempdir()?;
        // A rule across fields, which validation checks after parsing.
        let config = write(
            dir.path(),
            &valid("127.0.0.1:7400").replace("enabled = false", "listen = \"0.0.0.0:7401\""),
        )?;
        let output = check(&config, &[])?;
        assert_eq!(output.status.code(), Some(1));
        assert_eq!(text(&output.stdout), "");
        assert_eq!(
            text(&output.stderr),
            format!(
                "fleetring: {}: field admin.listen: 0.0.0.0:7401 isn't a loopback address; set \
                 admin.tls = true, or admin.allow_plaintext = true on a network you control\n",
                config.display()
            )
        );
        // A key the parser refuses is placed by line and column, and named in full.
        let config = write(
            dir.path(),
            &format!("{}port = 7401\n", valid("127.0.0.1:7400")),
        )?;
        let output = check(&config, &[])?;
        assert_eq!(output.status.code(), Some(1));
        let stderr = text(&output.stderr);
        assert!(
            stderr.starts_with(&format!(
                "fleetring: {}:10:1: field admin.port: unknown field `port`",
                config.display()
            )),
            "{stderr}"
        );
        // A flag that breaks a rule fails as the start would.
        let config = write(dir.path(), &valid("127.0.0.1:7400"))?;
        let output = check(&config, &["--role", "data"])?;
        assert_eq!(output.status.code(), Some(1));
        assert!(
            text(&output.stderr).contains("field control.address: is required: a data-only node"),
            "{}",
            text(&output.stderr)
        );
        // Without an identity, and without plaintext, the node has none.
        let config = write(
            dir.path(),
            &valid("127.0.0.1:7400").replace("insecure_plaintext = true\n", ""),
        )?;
        let output = check(&config, &[])?;
        assert_eq!(output.status.code(), Some(1));
        assert!(
            text(&output.stderr).contains("field tls: is required: every connection carries"),
            "{}",
            text(&output.stderr)
        );
        Ok(())
    }

    #[test]
    fn a_missing_file_fails_naming_it() -> TestResult {
        let dir = tempfile::tempdir()?;
        let missing = dir.path().join("missing.toml");
        let output = check(&missing, &[])?;
        assert_eq!(output.status.code(), Some(1));
        let stderr = text(&output.stderr);
        assert!(
            stderr.starts_with(&format!(
                "fleetring: {}: can't read it: ",
                missing.display()
            )),
            "{stderr}"
        );
        Ok(())
    }

    #[test]
    fn json_reports_the_result_and_its_problems() -> TestResult {
        let dir = tempfile::tempdir()?;
        let config = write(dir.path(), &valid("127.0.0.1:7400"))?;
        let output = check(&config, &["--json"])?;
        assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
        let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(
            report,
            serde_json::json!({
                "config": config.display().to_string(),
                "valid": true,
                "node": "node-1",
                "role": "both",
                "problems": [],
            })
        );

        let config = write(
            dir.path(),
            &valid("127.0.0.1:7400").replace("id = \"node-1\"", "id = \"node-1\"\nweight = 0"),
        )?;
        let output = check(&config, &["-o", "json"])?;
        assert_eq!(output.status.code(), Some(1));
        let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(report["valid"], false);
        assert_eq!(report["node"], serde_json::Value::Null);
        let problem = &report["problems"][0];
        assert_eq!(problem["field"], "node.weight", "{report}");
        assert_eq!(problem["line"], 7, "{report}");
        assert_eq!(problem["column"], 10, "{report}");
        assert!(problem["message"].is_string(), "{report}");
        // The failure goes to stderr too, as for any check that fails.
        assert!(
            text(&output.stderr).contains("field node.weight"),
            "{}",
            text(&output.stderr)
        );

        // The format flags are the check's: without `--check` they are a usage error.
        let unchecked = crate::support::fleetring()
            .args(["serve", "--config"])
            .arg(&config)
            .arg("--json")
            .output()?;
        assert_eq!(
            unchecked.status.code(),
            Some(2),
            "{}",
            text(&unchecked.stderr)
        );
        Ok(())
    }
}

#[cfg(unix)]
mod binary {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    use fleetringd::DataDir;

    use super::TestResult;
    use crate::support::Tls;

    fn write_config(
        dir: &std::path::Path,
    ) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
        write_config_with(dir, "")
    }

    /// Writes a one-node configuration with `membership` appended to its `[membership]` table,
    /// and the node's certificate beside it, under `dir/tls`.
    fn write_config_with(
        dir: &std::path::Path,
        membership: &str,
    ) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
        let tls = Tls::create(dir)?;
        let config = dir.join("node.toml");
        let text = format!(
            "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\n\n[node]\nid = \"node-1\"\n\n\
             [membership]\nheartbeat_interval_ms = 100\n{membership}{}",
            dir.join("data"),
            tls.node_table("node-1")?
        );
        std::fs::write(&config, text)?;
        Ok(config)
    }

    /// A running daemon with its stderr lines, and the address it listens on. Dropping it kills
    /// the daemon, so a failed test leaves no process behind.
    struct Started {
        daemon: std::process::Child,
        lines: std::io::Lines<BufReader<std::process::ChildStderr>>,
        listen: String,
    }

    impl Drop for Started {
        fn drop(&mut self) {
            // Already exited, or killed: either way the process is gone.
            let _ = self.daemon.kill();
        }
    }

    /// The line without its terminal colour codes, which the daemon's logs carry.
    fn plain(line: &str) -> String {
        let mut plain = String::with_capacity(line.len());
        let mut chars = line.chars();
        while let Some(c) = chars.next() {
            if c == '\u{1b}' {
                for c in chars.by_ref() {
                    if c == 'm' {
                        break;
                    }
                }
            } else {
                plain.push(c);
            }
        }
        plain
    }

    fn start(config: &std::path::Path) -> Result<Started, Box<dyn std::error::Error>> {
        let mut daemon = crate::support::fleetring()
            .args(["serve", "--config"])
            .arg(config)
            .env("RUST_LOG", "info")
            .stderr(Stdio::piped())
            .spawn()?;
        let mut lines = BufReader::new(daemon.stderr.take().ok_or("no stderr")?).lines();
        let started = lines
            .by_ref()
            .map_while(Result::ok)
            .map(|line| plain(&line))
            .find(|line| line.contains("fleetringd started"));
        let mut started_daemon = Started {
            daemon,
            lines,
            listen: String::new(),
        };
        let started = started.ok_or("daemon never logged its start")?;
        started_daemon.listen = started
            .split_whitespace()
            .find_map(|word| word.strip_prefix("listen="))
            .ok_or_else(|| format!("no listen address in {started}"))?
            .to_owned();
        Ok(started_daemon)
    }

    /// Creates a one-replica database on the daemon at `control`, so the node is a primary.
    ///
    /// The daemon logs its start before its node has joined, and a create before the join is
    /// refused because the fleet has no members, so the create is retried until the join lands.
    fn create_database(dir: &std::path::Path, control: &str) -> TestResult {
        for attempt in 0..600 {
            let created = crate::support::fleetring()
                .env("FLEETRING_TLS_DIR", dir.join("tls"))
                .args([
                    "db",
                    "create",
                    "notes",
                    "--replication-factor",
                    "1",
                    "--control",
                ])
                .arg(control)
                .output()?;
            if created.status.success() {
                return Ok(());
            }
            let stderr = String::from_utf8_lossy(&created.stderr);
            assert!(stderr.contains("no members"), "{stderr}");
            assert!(attempt < 599, "the node never joined: {stderr}");
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        Ok(())
    }

    /// Sends SIGINT, waits for the exit, and returns every line logged after the start.
    fn interrupt(mut started: Started) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let killed = Command::new("kill")
            .arg("-INT")
            .arg(started.daemon.id().to_string())
            .status()?;
        assert!(killed.success());
        let status = started.daemon.wait()?;
        assert!(status.success(), "exit status {status}");
        Ok(started
            .lines
            .by_ref()
            .map_while(Result::ok)
            .map(|line| plain(&line))
            .collect())
    }

    /// `Ctrl-C` on a primary that leaves on shutdown drains first: with one replica there is
    /// nowhere to hand the lease to, so the drain reports it and the node leaves anyway.
    #[test]
    fn sigint_drains_a_primary_before_it_leaves() -> TestResult {
        let dir = tempfile::tempdir()?;
        let config = write_config(dir.path())?;
        let started = start(&config)?;
        create_database(dir.path(), &started.listen)?;
        let rest = interrupt(started)?;
        assert!(
            rest.iter()
                .any(|line| line.contains("draining: handing leases over")),
            "{rest:?}"
        );
        // With one replica there is no failover to promise: the log says the database is
        // unavailable until the node returns.
        assert!(
            rest.iter().any(|line| line
                .contains("no other replica; the database is unavailable until this node returns")),
            "{rest:?}"
        );
        assert!(
            rest.iter()
                .all(|line| !line.contains("it will fail over once this node stops")),
            "{rest:?}"
        );
        assert!(
            rest.iter().any(|line| line.contains("left the fleet")),
            "{rest:?}"
        );
        assert!(
            rest.iter().any(|line| line.contains("fleetringd stopped")),
            "{rest:?}"
        );
        Ok(())
    }

    /// With `on_shutdown = "stay"`, `Ctrl-C` neither drains nor leaves.
    #[test]
    fn sigint_on_a_node_that_stays_neither_drains_nor_leaves() -> TestResult {
        let dir = tempfile::tempdir()?;
        let config = write_config_with(dir.path(), "on_shutdown = \"stay\"\n")?;
        let started = start(&config)?;
        create_database(dir.path(), &started.listen)?;
        let rest = interrupt(started)?;
        assert!(
            !rest.iter().any(|line| line.contains("draining")),
            "{rest:?}"
        );
        assert!(
            !rest.iter().any(|line| line.contains("left the fleet")),
            "{rest:?}"
        );
        assert!(
            rest.iter().any(|line| line.contains("fleetringd stopped")),
            "{rest:?}"
        );
        Ok(())
    }

    /// Started as 0.6 started a node, without `serve`: it runs, and warns once (ADR-0043, point
    /// 1.4).
    #[test]
    fn starts_logs_and_exits_cleanly_on_sigint_while_a_second_daemon_is_refused() -> TestResult {
        let dir = tempfile::tempdir()?;
        let config = write_config(dir.path())?;
        let mut daemon = crate::support::fleetring()
            .arg("--config")
            .arg(&config)
            .env("RUST_LOG", "info")
            .stderr(Stdio::piped())
            .spawn()?;
        let mut lines = BufReader::new(daemon.stderr.take().ok_or("no stderr")?).lines();
        let before: Vec<String> = lines
            .by_ref()
            .map_while(Result::ok)
            .take_while(|line| !line.contains("fleetringd started"))
            .collect();
        let warnings: Vec<&String> = before.iter().filter(|line| line.contains("WARN")).collect();
        assert_eq!(warnings.len(), 1, "{before:?}");
        assert!(
            warnings[0].contains(
                "starting without `serve` is kept until 1.0; run `fleetring serve --config <file>`"
            ),
            "{before:?}"
        );

        let second = crate::support::fleetring()
            .args(["serve", "--config"])
            .arg(&config)
            .output()?;
        assert!(!second.status.success());
        let stderr = String::from_utf8_lossy(&second.stderr);
        assert!(stderr.contains("in use by another fleetringd"), "{stderr}");

        let killed = Command::new("kill")
            .arg("-INT")
            .arg(daemon.id().to_string())
            .status()?;
        assert!(killed.success());
        let status = daemon.wait()?;
        assert!(status.success(), "exit status {status}");
        let rest: Vec<String> = lines.map_while(Result::ok).collect();
        assert!(
            rest.iter().any(|line| line.contains("fleetringd stopped")),
            "{rest:?}"
        );
        Ok(())
    }

    #[test]
    fn a_dropped_lock_frees_the_directory_while_children_spawn() -> TestResult {
        const CHILDREN: usize = 50;
        let dir = tempfile::tempdir()?;
        let spawned = AtomicUsize::new(0);
        thread::scope(|scope| {
            // Each child shares this process's open files, the lock's included, from fork to exec.
            let spawner = scope.spawn(|| {
                while spawned.load(Ordering::Relaxed) < CHILDREN {
                    crate::support::fleetring()
                        .arg("--version")
                        .stdout(Stdio::null())
                        .status()?;
                    spawned.fetch_add(1, Ordering::Relaxed);
                }
                Ok::<_, std::io::Error>(())
            });
            let mut relocked = Ok(());
            while relocked.is_ok() && spawned.load(Ordering::Relaxed) < CHILDREN {
                relocked = DataDir::lock(dir.path())
                    .and_then(|held| {
                        drop(held);
                        DataDir::lock(dir.path())
                    })
                    .map(drop);
            }
            // Let the spawner finish even if relocking failed early.
            spawned.store(CHILDREN, Ordering::Relaxed);
            let children = spawner.join().expect("spawner panicked");
            relocked?;
            children?;
            Ok(())
        })
    }
}
