//! The rolling upgrade from the previous release to this build (FR-054, ADR-0038 point 9).
//!
//! A three-node fleet starts on the previous release's binary. A client writes and reads
//! throughout while each node in turn, the control node last, moves to this build, and the whole
//! history is checked at the end: no write failed beyond its one retry, no read was stale, and no
//! acknowledged write was lost.
//!
//! The previous release comes from one of:
//!
//! - `FLEETRINGD_BASELINE_VERSION`, such as `0.3.0`: this build's own `fleetringd upgrade
//!   --version <it> --no-restart` downloads the release from GitHub and verifies it with the real
//!   cosign, into a scratch directory. The nightly workflow uses this.
//! - `FLEETRINGD_BASELINE_BIN`: a binary built locally, as before the release exists.
//!
//! With neither, the test says so and passes. It is `#[ignore]`d: a run takes minutes.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fleetring::NodeId;

use crate::common::{
    Defect, ProcessFleet, ReleaseServer, TestResult, check_history, create_history_database,
    current, fake_cosign, path_with, printed, progress, wait_for_members, wait_for_reported,
    write_release, write_until,
};

/// The previous release's binary, from the environment, or `None` to skip.
fn baseline(scratch: &Path) -> TestResult<Option<PathBuf>> {
    if let Some(bin) = std::env::var_os("FLEETRINGD_BASELINE_BIN") {
        return Ok(Some(PathBuf::from(bin)));
    }
    let Ok(version) = std::env::var("FLEETRINGD_BASELINE_VERSION") else {
        return Ok(None);
    };
    // This build installs the release over a scratch copy of itself, verifying it as any
    // upgrade does, with the cosign on the real PATH.
    let bin = scratch.join("baseline").join("fleetringd");
    fs::create_dir_all(bin.parent().ok_or("no parent")?)?;
    fs::copy(env!("CARGO_BIN_EXE_fleetringd"), &bin)?;
    let installed = Command::new(&bin)
        .args(["upgrade", "--version", &version, "--no-restart"])
        .output()?;
    assert!(installed.status.success(), "{}", printed(&installed));
    progress(&format!("downloaded and verified fleetringd {version}"));
    Ok(Some(bin))
}

/// Whether the binary at `bin` has the `upgrade` command.
fn has_upgrade(bin: &Path) -> TestResult<bool> {
    Ok(Command::new(bin)
        .args(["upgrade", "--help"])
        .output()?
        .status
        .success())
}

/// Replaces the binary at `bin` with this build, atomically, as `install.sh` does.
fn install_this_build(bin: &Path) -> TestResult {
    let staged = bin.with_file_name(".fleetringd.new");
    fs::copy(env!("CARGO_BIN_EXE_fleetringd"), &staged)?;
    fs::rename(&staged, bin)?;
    Ok(())
}

#[test]
#[ignore = "needs the previous release; runs nightly and in the release pre-flight"]
fn a_fleet_rolls_from_the_previous_release_to_this_build_under_writes() -> TestResult {
    let scratch = tempfile::tempdir()?;
    let Some(baseline) = baseline(scratch.path())? else {
        progress(
            "skipped: set FLEETRINGD_BASELINE_VERSION (a release) or FLEETRINGD_BASELINE_BIN (a binary)",
        );
        return Ok(());
    };
    let mut fleet = ProcessFleet::start(&scratch.path().join("fleet"), &baseline, 3)?;
    let runtime = tokio::runtime::Runtime::new()?;
    let client = fleet.client()?;
    let ids = ["node-1", "node-2", "node-3"];
    runtime.block_on(wait_for_members(&client, &ids, None))?;
    let name = runtime.block_on(create_history_database(&client))?;
    progress("the fleet runs the previous release; writing");

    let stop = Arc::new(AtomicBool::new(false));
    let writer = runtime.spawn(write_until(
        fleet.client()?,
        name.clone(),
        Arc::clone(&stop),
    ));

    // This build, published as a release the stand-in cosign verifies.
    let releases = scratch.path().join("releases");
    let tag = format!("fleetringd-v{}", current());
    write_release(
        &releases.join(&tag),
        &current(),
        &fs::read(env!("CARGO_BIN_EXE_fleetringd"))?,
        Defect::None,
    )?;
    let server = ReleaseServer::serve(&releases)?;
    let url = server.url(&tag);
    let cosign = scratch.path().join("cosign");
    fake_cosign(&cosign, "2.4.3")?;
    let path = path_with(&cosign);
    let this_build = PathBuf::from(env!("CARGO_BIN_EXE_fleetringd"));

    // The data nodes first, then the control node.
    for index in [2, 1, 0] {
        let id: NodeId = fleet.nodes[index].id.parse()?;
        progress(&format!("upgrading {id}"));
        let bin = fleet.nodes[index].bin.clone();
        let mut restarted = false;
        if has_upgrade(&bin)? {
            let upgraded = fleet
                .command(&bin, &path)
                .args([
                    "upgrade",
                    "--version",
                    &current().to_string(),
                    "--base-url",
                    &url,
                ])
                .arg("--config")
                .arg(&fleet.nodes[index].config)
                .output()?;
            assert!(
                upgraded.status.success(),
                "{}\n{}",
                printed(&upgraded),
                fleet.logs()
            );
            restarted = String::from_utf8_lossy(&upgraded.stdout).contains("rejoined the fleet");
        } else {
            // A release without `upgrade`: the manual steps the daemon README documents. Drain,
            // replace the binary, and restart; the stop drains again, and leaves.
            runtime.block_on(client.drain_node(&id))?;
            runtime.block_on(async {
                loop {
                    let databases = client.list_databases().await?;
                    if databases
                        .iter()
                        .all(|location| location.primary.as_ref() != Some(&id))
                    {
                        return Ok::<_, tonic::Status>(());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            })?;
            install_this_build(&bin)?;
        }
        if !restarted {
            fleet.stop(index)?;
            fleet.spawn(index)?;
        }
        // A control node on the previous release may not record what members run, so until it is
        // upgraded, the rejoin is all there is to see; then every member reports this build.
        let expected = (index == 0).then(current);
        runtime.block_on(wait_for_members(&client, &[id.as_str()], expected.as_ref()))?;
        progress(&format!("{id} runs {}", current()));

        if index == 2 {
            // A mixed fleet refuses a command a member doesn't support, whichever release its
            // control node runs: the old one doesn't know it, the new one gates it.
            let refused = fleet
                .command(&this_build, &path)
                .args(["node", "undrain", "node-3", "--control", &fleet.control])
                .output()?;
            assert!(!refused.status.success(), "{}", printed(&refused));
        }
    }
    // Every member reports this build: the control node, restarted on it, learned the others' from
    // their heartbeats.
    runtime.block_on(wait_for_reported(&client, &ids, &current()))?;
    // Every member supports `undrain` now, so the fleet accepts it, and enables it for good.
    let undrained = fleet
        .command(&this_build, &path)
        .args(["node", "undrain", "node-3", "--control", &fleet.control])
        .output()?;
    assert!(undrained.status.success(), "{}", printed(&undrained));

    // And this build's own upgrade restarts a node in place under the same writes.
    let node = &fleet.nodes[1];
    let reinstalled = fleet
        .command(&node.bin, &path)
        .args([
            "upgrade",
            "--version",
            &current().to_string(),
            "--base-url",
            &url,
        ])
        .arg("--config")
        .arg(&node.config)
        .output()?;
    assert!(
        reinstalled.status.success()
            && String::from_utf8_lossy(&reinstalled.stdout).contains("caught up"),
        "{}\n{}",
        printed(&reinstalled),
        fleet.logs()
    );

    stop.store(true, Ordering::SeqCst);
    let history = runtime.block_on(writer)?;
    progress(&format!(
        "{} writes acknowledged, {} retried, {} refused cleanly and resent (at most {} in a row), \
         {} reads",
        history.acknowledged.len(),
        history.retried,
        history.refused,
        history.longest_refusal,
        history.reads
    ));
    runtime.block_on(check_history(&client, &name, &history))?;
    Ok(())
}
