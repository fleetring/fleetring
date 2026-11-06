//! FR-075: spinners on stderr for slow commands (ADR-0043, point 6). They show only on a
//! terminal, so a command run with its streams piped, as here, prints no spinner byte: a script,
//! `$(…)`, and the MCP server see the same output with or without the feature.
//!
//! The terminal case, where both streams are terminals, is covered by the table in
//! `src/progress.rs`, so the suite never needs a pty.

use std::error::Error;
use std::process::Output;

use fleetringd::{Config, Daemon};

use crate::support::Tls;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// Runs `fleetring` with `args` against the node at `control`, both output streams piped.
fn fleetring(tls: &Tls, control: &str, args: &[&str]) -> Result<Output, Box<dyn Error>> {
    Ok(crate::support::fleetring()
        .env("FLEETRING_TLS_DIR", tls.dir())
        .env_remove("CI")
        .env_remove("NO_COLOR")
        .args(args)
        .args(["--control", control])
        .output()?)
}

/// Fails unless `output` succeeded and its stderr holds no spinner byte.
fn assert_no_spinner(label: &str, output: &Output) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{label}: {stderr}");
    for (name, byte) in [
        ("carriage return", '\r'),
        ("escape", '\u{1b}'),
        ("check mark", '\u{2713}'),
    ] {
        assert!(
            !stderr.contains(byte),
            "{label}: stderr holds a {name}: {stderr:?}"
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains('\r') && !stdout.contains('\u{1b}') && !stdout.contains('\u{2713}'),
        "{label}: stdout holds a spinner byte: {stdout:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_spinner_bytes_when_piped() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let config = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"both\"\n\n[node]\nid = \"node-1\"\n\
         \n[membership]\nheartbeat_interval_ms = 100\n{}",
        dir.path().join("node-1"),
        tls.node_table("node-1")?,
    );
    let daemon = Daemon::start(Config::from_toml(&config)?).await?;
    let control = daemon
        .control_address()
        .ok_or("no control address")?
        .to_string();
    let admin = fleetringd::data::Client::new(control.clone(), tls.client_network("tester")?);
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            match admin.status().await {
                Ok(view) if view.members().len() == 1 => return,
                _ => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .map_err(|_| "the node never joined its own fleet")?;

    let tls_for_commands = tls;
    let commands = tokio::task::spawn_blocking(move || -> Result<(), String> {
        let run = |args: &[&str]| {
            fleetring(&tls_for_commands, &control, args).map_err(|error| error.to_string())
        };
        let created = run(&["db", "create", "x", "--replication-factor", "1"])?;
        assert_no_spinner("db create x", &created);
        assert!(
            String::from_utf8_lossy(&created.stdout).starts_with("created x\n"),
            "{created:?}"
        );
        assert_no_spinner("db verify x", &run(&["db", "verify", "x"])?);
        assert_no_spinner("db drop x", &run(&["db", "drop", "x", "--yes"])?);
        assert_no_spinner("node drain node-1", &run(&["node", "drain", "node-1"])?);
        Ok(())
    })
    .await?;
    daemon.run_until(async {}).await;
    commands?;
    Ok(())
}
