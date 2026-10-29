//! FR-022: configuration, the data directory lock, and the binary's lifecycle.

use std::error::Error;
use std::time::Duration;

use fleetringd::replication::ReplicationSettings;
use fleetringd::{Config, ConfigError, DataDir, DataDirError, Role};

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
    assert_eq!(config.role, Role::Both);
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

#[cfg(unix)]
mod binary {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    use fleetringd::DataDir;

    use super::TestResult;

    fn write_config(dir: &std::path::Path) -> std::io::Result<std::path::PathBuf> {
        write_config_with(dir, "")
    }

    /// Writes a one-node configuration with `membership` appended to its `[membership]` table.
    fn write_config_with(
        dir: &std::path::Path,
        membership: &str,
    ) -> std::io::Result<std::path::PathBuf> {
        let config = dir.join("node.toml");
        let text = format!(
            "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\n\n[node]\nid = \"node-1\"\n\n\
             [membership]\nheartbeat_interval_ms = 100\n{membership}",
            dir.join("data")
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
        let mut daemon = Command::new(env!("CARGO_BIN_EXE_fleetringd"))
            .arg("--config")
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
    /// refused because the cluster has no members, so the create is retried until the join lands.
    fn create_database(control: &str) -> TestResult {
        for attempt in 0..600 {
            let created = Command::new(env!("CARGO_BIN_EXE_fleetringd"))
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
        create_database(&started.listen)?;
        let rest = interrupt(started)?;
        assert!(
            rest.iter()
                .any(|line| line.contains("draining: handing leases over")),
            "{rest:?}"
        );
        assert!(
            rest.iter()
                .any(|line| line.contains("can't hand the lease over")),
            "{rest:?}"
        );
        assert!(
            rest.iter().any(|line| line.contains("left the cluster")),
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
        create_database(&started.listen)?;
        let rest = interrupt(started)?;
        assert!(
            !rest.iter().any(|line| line.contains("draining")),
            "{rest:?}"
        );
        assert!(
            !rest.iter().any(|line| line.contains("left the cluster")),
            "{rest:?}"
        );
        assert!(
            rest.iter().any(|line| line.contains("fleetringd stopped")),
            "{rest:?}"
        );
        Ok(())
    }

    #[test]
    fn starts_logs_and_exits_cleanly_on_sigint_while_a_second_daemon_is_refused() -> TestResult {
        let dir = tempfile::tempdir()?;
        let config = write_config(dir.path())?;
        let mut daemon = Command::new(env!("CARGO_BIN_EXE_fleetringd"))
            .arg("--config")
            .arg(&config)
            .env("RUST_LOG", "info")
            .stderr(Stdio::piped())
            .spawn()?;
        let mut lines = BufReader::new(daemon.stderr.take().ok_or("no stderr")?).lines();
        let started = lines
            .by_ref()
            .map_while(Result::ok)
            .find(|line| line.contains("fleetringd started"))
            .ok_or("daemon never logged its start")?;
        assert!(started.contains("node-1"), "{started}");

        let second = Command::new(env!("CARGO_BIN_EXE_fleetringd"))
            .arg("--config")
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
                    Command::new(env!("CARGO_BIN_EXE_fleetringd"))
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
