//! FR-022: configuration, the data directory lock, and the binary's lifecycle.

use std::error::Error;

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
    DataDir::lock(dir.path())?;
    Ok(())
}

#[cfg(unix)]
mod binary {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};

    use super::TestResult;

    fn write_config(dir: &std::path::Path) -> std::io::Result<std::path::PathBuf> {
        let config = dir.join("node.toml");
        let text = format!(
            "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\n\n[node]\nid = \"node-1\"\n",
            dir.join("data")
        );
        std::fs::write(&config, text)?;
        Ok(config)
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
}
