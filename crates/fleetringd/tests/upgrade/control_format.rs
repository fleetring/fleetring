//! FR-064: `upgrade` refuses a binary that can't read the node's control log, before it swaps
//! anything (ADR-0040 point 5).

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;

use fleetring_control::Release;

use crate::Bench;
use crate::common::{ProcessFleet, TestResult, free_port, line_behind, printed, wait_for_members};

/// A stand-in previous release that answers `--version` and `upgrade --software` as `release`,
/// reporting `control_format` if it is given, as a release that predates the field doesn't.
fn reporting_binary(release: &Release, control_format: Option<u32>) -> Vec<u8> {
    let format = control_format.map_or_else(String::new, |format| {
        format!(",\"control_format\":{format}")
    });
    format!(
        "#!/bin/sh\ncase \"$1 $2\" in\n\"--version \"*) echo \"fleetring {release}\" ;;\n\
         \"upgrade --software\") echo '{{\"version\":\"{release}\",\"features\":[],\
         \"config_check\":true{format}}}' ;;\nesac\n"
    )
    .into_bytes()
}

/// A control node whose `control.db` holds `format`, with `previous` as the bench's previous
/// binary; returns the node's configuration file. The node isn't running and no control plane
/// answers: the check refuses before it would ask one.
fn control_node_with_format(
    bench: &Bench,
    format: u32,
    previous: &[u8],
) -> TestResult<(PathBuf, PathBuf)> {
    let root = bench.dir.path().join("node");
    let tls = crate::support::Tls::create(&root)?;
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir)?;
    let connection = rusqlite::Connection::open(data_dir.join("control.db"))?;
    connection.execute_batch(&format!(
        "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
         INSERT INTO meta VALUES ('format', '{format}'), ('genesis', '{{}}');"
    ))?;
    drop(connection);
    let listen = format!("127.0.0.1:{}", free_port()?);
    let config = root.join("node.toml");
    fs::write(
        &config,
        format!(
            "data_dir = {data_dir:?}\nlisten = \"{listen}\"\nrole = \"both\"\n\n[node]\n\
             id = \"node-1\"\n\n{}",
            tls.node_tls_table("node-1")?
        ),
    )?;
    fs::write(bench.previous(), previous)?;
    fs::set_permissions(bench.previous(), fs::Permissions::from_mode(0o755))?;
    Ok((config, tls.dir().to_owned()))
}

/// Runs `upgrade --rollback --config <config>` with the client identity in `tls_dir`.
fn roll_back(
    bench: &Bench,
    (config, tls_dir): &(PathBuf, PathBuf),
) -> TestResult<std::process::Output> {
    let mut command = std::process::Command::new(&bench.bin);
    crate::support::isolate(&mut command);
    Ok(crate::support::output_copied(
        command
            .args(["upgrade", "--rollback", "--yes", "--no-restart", "--config"])
            .arg(config)
            .env("FLEETRING_TLS_DIR", tls_dir)
            .env("PATH", bench.path())
            .env_remove("FLEETRING_BASE_URL")
            .env_remove("FLEETRING_RELEASES_URL"),
    )?)
}

#[test]
fn a_rollback_to_a_binary_that_reads_an_older_control_log_is_refused() -> TestResult {
    let bench = Bench::new()?;
    let older = line_behind();
    let previous = reporting_binary(&older, Some(1));
    let node = control_node_with_format(&bench, 2, &previous)?;
    let refused = roll_back(&bench, &node)?;
    assert_eq!(refused.status.code(), Some(1), "{}", printed(&refused));
    let control_db = node.0.with_file_name("data").join("control.db");
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains(&format!(
            "fleetring {older} reads control log format 1, and {} is format 2: rolling this \
             control node back would leave it unable to start\n  hint: roll forward",
            control_db.display()
        )),
        "{}",
        printed(&refused)
    );
    assert_eq!(
        fs::read(&bench.bin)?,
        bench.original,
        "the binary was swapped"
    );
    assert_eq!(fs::read(bench.previous())?, previous);
    Ok(())
}

#[test]
fn a_binary_that_reports_no_control_format_reads_format_one() -> TestResult {
    let bench = Bench::new()?;
    let older = line_behind();
    let node = control_node_with_format(&bench, 2, &reporting_binary(&older, None))?;
    let refused = roll_back(&bench, &node)?;
    assert_eq!(refused.status.code(), Some(1), "{}", printed(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains(&format!(
            "fleetring {older} reads control log format 1, and"
        )),
        "{}",
        printed(&refused)
    );
    assert_eq!(
        fs::read(&bench.bin)?,
        bench.original,
        "the binary was swapped"
    );
    Ok(())
}

#[test]
fn a_data_only_node_has_no_control_format_to_check() -> TestResult {
    let bench = Bench::new()?;
    let fleet = ProcessFleet::start(&bench.dir.path().join("fleet"), &bench.bin, 2)?;
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(wait_for_members(
        &fleet.client()?,
        &["node-1", "node-2"],
        None,
    ))?;
    let node = &fleet.nodes[1];
    assert!(
        !node.data_dir.join("control.db").exists(),
        "a data-only node has no control log"
    );
    let older = line_behind();
    let name = node
        .bin
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let previous = node.bin.with_file_name(format!("{name}.previous"));
    fs::write(&previous, reporting_binary(&older, None))?;
    fs::set_permissions(&previous, fs::Permissions::from_mode(0o755))?;
    let rolled = fleet
        .command(&node.bin, &bench.path())
        .args(["upgrade", "--rollback", "--yes", "--no-restart"])
        .arg("--config")
        .arg(&node.config)
        .output()?;
    assert!(rolled.status.success(), "{}", printed(&rolled));
    assert_eq!(fs::read(&node.bin)?, reporting_binary(&older, None));
    Ok(())
}
