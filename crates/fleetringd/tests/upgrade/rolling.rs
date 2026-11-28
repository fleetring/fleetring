//! The rolling upgrade from the previous release to this build (FR-054, ADR-0038 point 9).
//!
//! A three-node fleet starts on the previous release's binary, installed as `fleetringd` and run
//! as `fleetringd --config <file>`, as every release before 0.7 runs. A client writes and reads
//! throughout while each node in turn, the control node last, moves to this build, and the whole
//! history is checked at the end: no write failed beyond its one retry, no read was stale, and no
//! acknowledged write was lost. A previous release with `upgrade` installs this build itself, from
//! an archive laid out as the release workflow lays it out, with `fleetring` and a regular copy
//! named `fleetringd` (ADR-0043, point 3). Once every node runs this build, its `upgrade` refuses
//! a next release whose `serve --check` refuses a node's configuration, a stand-in that refuses
//! the heartbeat interval these nodes use, and leaves the node running; this build's own
//! release, which accepts it, then restarts the node in place.
//!
//! From a release without forks and delegation, the 0.7 line, it checks those features too
//! (FR-096, `features.rs`): the previous release's credentials keep working, nothing is enabled
//! by the upgrade alone, a node rolls back and returns while nothing is, each feature is enabled
//! by its first command, and rollback is refused after.
//!
//! Once the control node runs this build, its format-1 `control.db` has migrated to a control
//! group of one voter (FR-064, ADR-0040 points 5 and 8): the group's only voter is the control
//! node, every database is still listed at its epoch or a later one, `upgrade --rollback` to the
//! previous release is refused for the control log's format, the previous release run on the
//! data directory anyway refuses the format and leaves an empty `commands` table behind, and
//! this build, started again, drops that table and serves.
//!
//! The previous release comes from one of:
//!
//! - `FLEETRINGD_BASELINE_VERSION`, such as `0.6.0`: this build's own `fleetring upgrade
//!   --version <it> --no-restart --yes` downloads the release from GitHub and verifies it with
//!   the real cosign, into a scratch directory. The nightly workflow uses this.
//! - `FLEETRINGD_BASELINE_BIN`: a binary built locally, as before the release exists.
//!
//! With neither, it fails, naming both: it is `#[ignore]`d, since a run takes minutes, so it runs
//! only when asked for, and a run asked for without a baseline would otherwise pass having
//! checked nothing.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fleetring::NodeId;
use fleetring_control::{DatabaseName, LeaseEpoch};

use crate::common::{
    ArchiveLayout, Defect, ProcessFleet, ReleaseServer, TestResult, check_history,
    create_history_database, current, fake_cosign, path_with, printed, progress, refusing_release,
    release_of, wait_for_members, wait_for_reported, write_release, write_until,
};
use crate::features::Features;

/// The previous release's binary, from the environment, or `None` to skip.
fn baseline(scratch: &Path) -> TestResult<Option<PathBuf>> {
    if let Some(bin) = std::env::var_os("FLEETRINGD_BASELINE_BIN") {
        return Ok(Some(PathBuf::from(bin)));
    }
    let Ok(version) = std::env::var("FLEETRINGD_BASELINE_VERSION") else {
        return Ok(None);
    };
    // This build installs the release over a scratch copy of itself, verifying it as any
    // upgrade does, with the cosign on the real PATH. No node runs here, so `--yes` says that
    // none starts with `serve`, which a release before 0.7 can't run.
    let bin = scratch.join("baseline").join("fleetringd");
    fs::create_dir_all(bin.parent().ok_or("no parent")?)?;
    fs::copy(env!("CARGO_BIN_EXE_fleetring"), &bin)?;
    let mut install = Command::new(&bin);
    crate::support::isolate(&mut install);
    let installed = crate::support::output_copied(install.args([
        "upgrade",
        "--version",
        &version,
        "--no-restart",
        "--yes",
    ]))?;
    assert!(installed.status.success(), "{}", printed(&installed));
    progress(&format!("downloaded and verified fleetringd {version}"));
    Ok(Some(bin))
}

/// Whether the binary at `bin` has the `upgrade` command.
fn has_upgrade(bin: &Path) -> TestResult<bool> {
    let mut help = Command::new(bin);
    crate::support::isolate(&mut help);
    Ok(
        crate::support::output_copied(help.args(["upgrade", "--help"]))?
            .status
            .success(),
    )
}

/// The control group's voters, by node ID, and whether its membership is joint, as `fleetring
/// control list -o json`, run as `bin`, reports them from the leader.
fn voters(fleet: &ProcessFleet, bin: &Path, path: &str) -> TestResult<(Vec<String>, bool)> {
    let listed = fleet
        .command(bin, path)
        .args(["control", "list", "-o", "json", "--control", &fleet.control])
        .output()?;
    assert!(listed.status.success(), "{}", printed(&listed));
    let group: serde_json::Value = serde_json::from_slice(&listed.stdout)?;
    let nodes = group
        .get("nodes")
        .and_then(serde_json::Value::as_array)
        .ok_or("a control group without nodes")?;
    let voters = nodes
        .iter()
        .filter(|node| node.get("voter").and_then(serde_json::Value::as_bool) == Some(true))
        .map(|node| {
            node.get("node")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .ok_or("a control node without an ID")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let joint = group.get("joint").and_then(serde_json::Value::as_bool) == Some(true);
    Ok((voters, joint))
}

/// Every database the fleet lists, with its epoch and primary.
async fn epochs(
    client: &fleetringd::data::Client,
) -> TestResult<Vec<(DatabaseName, LeaseEpoch, Option<NodeId>)>> {
    Ok(client
        .list_databases()
        .await?
        .into_iter()
        .map(|location| (location.database, location.epoch, location.primary))
        .collect())
}

/// Runs `bin` on `config` until it exits, for at most a minute, and returns what it logged.
fn run_to_exit(bin: &Path, config: &Path) -> TestResult<(std::process::ExitStatus, String)> {
    let mut command = Command::new(bin);
    crate::support::isolate(&mut command);
    let mut child = crate::support::spawn_copied(
        command
            .arg("--config")
            .arg(config)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped()),
    )?;
    // A minute, in tenths of a second.
    let mut waits = 600_u32;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if waits == 0 {
            child.kill()?;
            return Err("the previous release kept running on a format-2 control log".into());
        }
        waits -= 1;
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    let output = child.wait_with_output()?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok((status, text))
}

/// Replaces the binary at `bin` with this build, atomically, as `install.sh` does.
fn install_this_build(bin: &Path) -> TestResult {
    let staged = bin.with_file_name(".fleetringd.new");
    fs::copy(env!("CARGO_BIN_EXE_fleetring"), &staged)?;
    fs::rename(&staged, bin)?;
    Ok(())
}

#[test]
#[ignore = "needs the previous release; runs nightly and in the release pre-flight"]
fn a_fleet_rolls_from_the_previous_release_to_this_build_under_writes() -> TestResult {
    let scratch = tempfile::tempdir()?;
    let Some(baseline) = baseline(scratch.path())? else {
        return Err(
            "no baseline: set FLEETRINGD_BASELINE_VERSION (a release, such as 0.7.0) or \
                    FLEETRINGD_BASELINE_BIN (a binary) to run the rolling upgrade"
                .into(),
        );
    };
    // `node undrain` is the first fleet feature (0.4.0): a baseline before it makes the fleet
    // refuse the command until every member is upgraded.
    let baseline_lacks_undrain = release_of(&baseline)? < fleetring_control::Release::new(0, 4, 0);
    let mut fleet = ProcessFleet::start_release(&scratch.path().join("fleet"), &baseline, 3)?;
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
        &fs::read(env!("CARGO_BIN_EXE_fleetring"))?,
        Defect::None,
        ArchiveLayout::Both,
    )?;
    let server = ReleaseServer::serve(&releases)?;
    let url = server.url(&tag);
    let cosign = scratch.path().join("cosign");
    fake_cosign(&cosign, "2.4.3")?;
    let path = path_with(&cosign);
    let this_build = PathBuf::from(env!("CARGO_BIN_EXE_fleetring"));
    // From a release without forks and delegation (0.7), the checks of FR-096 run too.
    let features = Features::on_previous_release(&fleet, &baseline, scratch.path(), &path)?;
    if features.is_none() {
        progress(
            "the fork and delegation checks don't apply: the baseline is before 0.7, or has \
             `fork` already",
        );
    }

    // The data nodes first, then the control node.
    let mut before_the_control_node = Vec::new();
    for index in [2, 1, 0] {
        let id: NodeId = fleet.nodes[index].id.parse()?;
        progress(&format!("upgrading {id}"));
        let bin = fleet.nodes[index].bin.clone();
        let mut restarted = false;
        if index == 0 {
            before_the_control_node = runtime.block_on(epochs(&client))?;
        }
        // This build's own settings, such as its admin listener, apply from its first start.
        fleet.enable_admin(index)?;
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
        // Whatever installed it, the node's file is still `fleetringd`, and says so.
        let printed_version = crate::support::output_copied(Command::new(&bin).arg("--version"))?;
        assert_eq!(
            String::from_utf8_lossy(&printed_version.stdout).trim(),
            format!("fleetringd {}", current())
        );
        progress(&format!("{id} runs {}", current()));

        if index == 0 {
            // The format-1 log migrated to format 2, a control group whose only voter is the
            // control node, as `control list` reports it.
            let control_db = rusqlite::Connection::open_with_flags(
                fleet.nodes[0].data_dir.join("control.db"),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            let format: String =
                control_db.query_row("SELECT value FROM meta WHERE key = 'format'", [], |row| {
                    row.get(0)
                })?;
            drop(control_db);
            assert_eq!(format, "2", "the control log migrated to format 2");
            assert_eq!(
                voters(&fleet, &this_build, &path)?,
                (vec![id.to_string()], false)
            );
            // Every database the fleet created is still listed at its epoch. The control node
            // also hosts data, so its upgrade drains it: a database it was primary for moves its
            // lease to another replica, at a later epoch. Every other keeps its epoch, since the
            // migration itself changes no lease.
            let after = runtime.block_on(epochs(&client))?;
            for (database, epoch, primary) in &before_the_control_node {
                let listed = after
                    .iter()
                    .find(|(name, _, _)| name == database)
                    .ok_or_else(|| format!("{database} is no longer listed"))?;
                if primary.as_ref() == Some(&id) {
                    assert!(listed.1 > *epoch, "{database}: {} <= {epoch}", listed.1);
                } else {
                    assert_eq!(listed.1, *epoch, "{database} changed epoch");
                }
            }
            // A rollback to the previous release is refused before anything is swapped.
            if has_upgrade(&baseline)? {
                let running = fs::read(&bin)?;
                let refused = fleet
                    .command(&bin, &path)
                    .args(["upgrade", "--rollback", "--yes", "--no-restart", "--config"])
                    .arg(&fleet.nodes[0].config)
                    .output()?;
                assert_eq!(refused.status.code(), Some(1), "{}", printed(&refused));
                let stderr = String::from_utf8_lossy(&refused.stderr);
                assert!(
                    stderr.contains("reads control log format 1, and")
                        && stderr.contains("is format 2"),
                    "{}",
                    printed(&refused)
                );
                assert_eq!(fs::read(&bin)?, running, "the rollback swapped the binary");
            }
            // The previous release, run on the data directory as a rollback that bypassed the
            // check would run it, refuses the format, and this build tolerates what it leaves.
            fleet.stop(0)?;
            let (status, logged) = run_to_exit(&baseline, &fleet.nodes[0].config)?;
            assert!(!status.success(), "{logged}");
            assert!(
                logged.contains("control log format 2 is not supported"),
                "{logged}"
            );
            fleet.spawn(0)?;
            runtime.block_on(wait_for_members(&client, &[id.as_str()], Some(&current())))?;
            assert!(
                fs::read_to_string(&fleet.nodes[0].log)?
                    .contains("dropped the empty commands table an older release created"),
                "{}",
                fleet.logs()
            );
            progress("the control node runs a one-voter control group, and refuses a rollback");
        }

        if index != 0
            && let Some(features) = &features
        {
            features.while_the_control_node_is_previous()?;
        }

        if index == 2 && baseline_lacks_undrain {
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

    if let Some(features) = &features {
        runtime.block_on(features.after_the_upgrade(&mut fleet, &client, &url))?;
    }

    // A next release that refuses the node's configuration is refused before it is swapped in,
    // and the node runs on under the writes.
    let node = &fleet.nodes[1];
    write_release(
        &releases.join("refusing"),
        &current(),
        &refusing_release(),
        Defect::None,
        ArchiveLayout::Both,
    )?;
    let running = fs::read(&node.bin)?;
    let refused = fleet
        .command(&node.bin, &path)
        .args([
            "upgrade",
            "--version",
            &current().to_string(),
            "--base-url",
            &server.url("refusing"),
        ])
        .arg("--config")
        .arg(&node.config)
        .output()?;
    assert_eq!(refused.status.code(), Some(1), "{}", printed(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains(&format!(
            "fleetring {} refuses node {}'s configuration, so the node wouldn't start on it; \
             nothing was changed",
            current(),
            node.id
        )),
        "{}",
        printed(&refused)
    );
    assert_eq!(
        fs::read(&node.bin)?,
        running,
        "the refused release was installed"
    );
    runtime.block_on(wait_for_members(
        &client,
        &[node.id.as_str()],
        Some(&current()),
    ))?;
    progress(&format!(
        "{} refused a release that refuses its configuration",
        node.id
    ));

    // And this build's own upgrade, which accepts it, restarts the node in place under the same
    // writes. A previous release from 0.7 on added the `fleetring` name when it installed this
    // build; otherwise this upgrade adds it.
    let named = fs::symlink_metadata(node.bin.with_file_name("fleetring")).is_ok();
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
    let stdout = String::from_utf8_lossy(&reinstalled.stdout);
    assert!(
        reinstalled.status.success()
            && stdout.contains(&format!(
                "fleetring {} accepts node {}'s configuration",
                current(),
                node.id
            ))
            && stdout.contains("caught up")
            && (named || stdout.contains("added fleetring beside fleetringd")),
        "{}\n{}",
        printed(&reinstalled),
        fleet.logs()
    );
    // The name is the same file.
    let printed_version = crate::support::output_copied(
        Command::new(node.bin.with_file_name("fleetring")).arg("--version"),
    )?;
    assert_eq!(
        String::from_utf8_lossy(&printed_version.stdout).trim(),
        format!("fleetring {}", current())
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
