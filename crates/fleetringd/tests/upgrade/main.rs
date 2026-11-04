//! FR-054: `fleetringd upgrade` and the rolling upgrade (ADR-0038).
//!
//! The fast tests serve fake releases from a local directory and verify them with a stand-in
//! `cosign` that checks the identity, issuer, and blob it is given, so they never reach the
//! network. The rolling upgrade from the previous release is `#[ignore]`d: it runs nightly and
//! before a release (`rolling.rs`).
#![cfg(unix)]

mod common;
mod rolling;
#[path = "../support/mod.rs"]
mod support;

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fleetring_control::Release;
use fleetringd::upgrade::ProcessFile;

use crate::common::{
    Defect, ProcessFleet, ReleaseServer, TestResult, check_history, create_history_database,
    current, fake_binary, fake_cosign, line_behind, lines_ahead, node_command, path_with, printed,
    wait_for, wait_for_members, write_release, write_releases_list, write_until,
};

/// A scratch install of this build, a release server, and a stand-in cosign.
struct Bench {
    dir: tempfile::TempDir,
    bin: PathBuf,
    original: Vec<u8>,
    server: ReleaseServer,
}

impl Bench {
    fn new() -> TestResult<Self> {
        let dir = tempfile::tempdir()?;
        let bin = dir.path().join("bin").join("fleetringd");
        fs::create_dir_all(bin.parent().ok_or("no parent")?)?;
        fs::copy(env!("CARGO_BIN_EXE_fleetringd"), &bin)?;
        let original = fs::read(&bin)?;
        fake_cosign(&dir.path().join("cosign"), "2.4.3")?;
        fs::create_dir_all(dir.path().join("releases"))?;
        let server = ReleaseServer::serve(&dir.path().join("releases"))?;
        Ok(Self {
            dir,
            bin,
            original,
            server,
        })
    }

    /// Writes `release` holding `binary`, with `defect`, and returns its `--base-url`.
    fn publish(&self, release: &Release, binary: &[u8], defect: Defect) -> TestResult<String> {
        let tag = format!("fleetringd-v{release}");
        write_release(
            &self.dir.path().join("releases").join(&tag),
            release,
            binary,
            defect,
        )?;
        Ok(self.server.url(&tag))
    }

    /// `PATH` with the stand-in cosign first.
    fn path(&self) -> String {
        path_with(&self.dir.path().join("cosign"))
    }

    /// Runs this install's binary, or `bin`, with `args` and `path`.
    fn run_as(&self, bin: &Path, args: &[&str], path: &str) -> TestResult<Output> {
        Ok(std::process::Command::new(bin)
            .args(args)
            .env("PATH", path)
            .env_remove("FLEETRING_BASE_URL")
            .env_remove("FLEETRING_RELEASES_URL")
            .output()?)
    }

    fn run(&self, args: &[&str]) -> TestResult<Output> {
        self.run_as(&self.bin, args, &self.path())
    }

    fn previous(&self) -> PathBuf {
        self.bin.with_file_name("fleetringd.previous")
    }

    /// Asserts that nothing was installed: the binary is as it was, and nothing was kept.
    fn assert_unchanged(&self) -> TestResult {
        assert_eq!(fs::read(&self.bin)?, self.original, "the binary changed");
        assert!(!self.previous().exists(), "a previous binary was kept");
        Ok(())
    }

    /// The stand-in cosign's calls so far.
    fn cosign_calls(&self) -> String {
        fs::read_to_string(self.dir.path().join("cosign").join("calls")).unwrap_or_default()
    }
}

#[test]
fn an_upgrade_verifies_swaps_atomically_and_rolls_back() -> TestResult {
    let bench = Bench::new()?;
    let release = lines_ahead(1);
    let url = bench.publish(&release, &fake_binary(&release), Defect::None)?;
    let version = release.to_string();
    let installed = bench.run(&["upgrade", "--version", &version, "--base-url", &url])?;
    assert!(installed.status.success(), "{}", printed(&installed));
    let stdout = String::from_utf8_lossy(&installed.stdout);
    assert!(
        stdout.contains(&format!("installed fleetringd {release}"))
            && stdout.contains("verified the signature and the checksum"),
        "{stdout}"
    );
    // cosign was asked about the release workflow's identity, as RELEASING.md names it.
    let calls = bench.cosign_calls();
    assert!(
        calls.contains(
            "--certificate-identity-regexp ^https://github.com/fleetring/fleetring/.github/workflows/release.yml@refs/tags/fleetringd-v --certificate-oidc-issuer https://token.actions.githubusercontent.com"
        ),
        "{calls}"
    );
    let printed_version = bench.run(&["--version"])?;
    assert_eq!(
        String::from_utf8_lossy(&printed_version.stdout).trim(),
        format!("fleetringd {release}")
    );
    assert_eq!(fs::read(bench.previous())?, bench.original);

    // The new binary can't roll itself back here (it is a stub), so the previous one does: it
    // swaps the pair it belongs to.
    let rolled = bench.run_as(&bench.previous(), &["upgrade", "--rollback"], &bench.path())?;
    assert!(rolled.status.success(), "{}", printed(&rolled));
    assert!(
        String::from_utf8_lossy(&rolled.stdout)
            .contains(&format!("rolled back to fleetringd {}", current())),
        "{}",
        printed(&rolled)
    );
    assert_eq!(fs::read(&bench.bin)?, bench.original);
    assert_eq!(fs::read(bench.previous())?, fake_binary(&release));
    let mut left: Vec<String> = fs::read_dir(bench.bin.parent().ok_or("no parent")?)?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_, _>>()?;
    left.sort();
    assert_eq!(
        left,
        ["fleetringd", "fleetringd.previous"],
        "temporary files left"
    );
    Ok(())
}

/// Each way a download can fail to verify is a refusal that says why and changes nothing.
#[test]
fn a_release_that_does_not_verify_is_refused_and_nothing_changes() -> TestResult {
    let cases = [
        (
            Defect::Signature,
            "the signature on SHA256SUMS doesn't verify as the fleetring release workflow's; not installing: Error: invalid signature",
        ),
        (
            Defect::Identity,
            "the signature on SHA256SUMS doesn't verify as the fleetring release workflow's; not installing: Error: none of the expected identities matched",
        ),
        (Defect::Checksum, "checksum mismatch for fleetringd-"),
    ];
    for (defect, reason) in cases {
        let bench = Bench::new()?;
        let release = lines_ahead(1);
        let url = bench.publish(&release, &fake_binary(&release), defect)?;
        let refused = bench.run(&[
            "upgrade",
            "--version",
            &release.to_string(),
            "--base-url",
            &url,
        ])?;
        assert_eq!(
            refused.status.code(),
            Some(1),
            "{defect:?}: {}",
            printed(&refused)
        );
        let stderr = String::from_utf8_lossy(&refused.stderr);
        assert!(stderr.contains(reason), "{defect:?}: {stderr}");
        bench.assert_unchanged()?;
    }
    Ok(())
}

#[test]
fn a_binary_that_is_another_release_is_refused() -> TestResult {
    let bench = Bench::new()?;
    let release = lines_ahead(1);
    let url = bench.publish(&release, &fake_binary(&lines_ahead(3)), Defect::None)?;
    let refused = bench.run(&[
        "upgrade",
        "--version",
        &release.to_string(),
        "--base-url",
        &url,
    ])?;
    assert_eq!(refused.status.code(), Some(1), "{}", printed(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains(&format!(
            "it says it is fleetringd {}, not {release}",
            lines_ahead(3)
        )),
        "{}",
        printed(&refused)
    );
    bench.assert_unchanged()
}

#[test]
fn without_a_new_enough_cosign_nothing_is_downloaded_or_installed() -> TestResult {
    let bench = Bench::new()?;
    let release = lines_ahead(1);
    let url = bench.publish(&release, &fake_binary(&release), Defect::None)?;
    let version = release.to_string();
    let args = [
        "upgrade",
        "--version",
        version.as_str(),
        "--base-url",
        url.as_str(),
    ];

    let missing = bench.run_as(&bench.bin, &args, "/usr/bin:/bin")?;
    assert_eq!(missing.status.code(), Some(1), "{}", printed(&missing));
    let stderr = String::from_utf8_lossy(&missing.stderr);
    assert!(
        stderr.contains("cosign isn't installed") && stderr.contains("hint: install cosign 2.4.2"),
        "{stderr}"
    );

    let old = bench.dir.path().join("old-cosign");
    fake_cosign(&old, "2.4.1")?;
    let too_old = bench.run_as(&bench.bin, &args, &path_with(&old))?;
    assert_eq!(too_old.status.code(), Some(1), "{}", printed(&too_old));
    assert!(
        String::from_utf8_lossy(&too_old.stderr).contains("the installed cosign is 2.4.1, too old"),
        "{}",
        printed(&too_old)
    );
    assert!(
        bench.server.requested().is_empty(),
        "{:?}",
        bench.server.requested()
    );
    bench.assert_unchanged()
}

/// `--check` reports the newest final release and the fleet's verdict and changes nothing; an
/// upgrade the fleet's window forbids is refused before anything is downloaded.
#[test]
fn the_fleet_window_is_checked_before_anything_is_downloaded() -> TestResult {
    let bench = Bench::new()?;
    let fleet_dir = bench.dir.path().join("fleet");
    let fleet = ProcessFleet::start(&fleet_dir, &bench.bin, 1)?;
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(wait_for_members(&fleet.client()?, &["node-1"], None))?;
    let releases = bench.dir.path().join("releases").join("releases.json");
    let releases_url = bench.server.url("releases.json");
    let check = |list: &[(&str, bool)]| -> TestResult<Output> {
        write_releases_list(&releases, list)?;
        Ok(fleet
            .command(&bench.bin, &bench.path())
            .args([
                "upgrade",
                "--check",
                "--releases-url",
                &releases_url,
                "--control",
            ])
            .arg(&fleet.control)
            .output()?)
    };

    let next = format!("fleetringd-v{}", lines_ahead(1));
    let allowed = check(&[(next.as_str(), false), ("fleetring-v9.0.0", false)])?;
    assert!(allowed.status.success(), "{}", printed(&allowed));
    assert_eq!(
        String::from_utf8_lossy(&allowed.stdout),
        format!(
            "installed: {}\navailable: {}\nfleet: allows {}\n",
            current(),
            lines_ahead(1),
            lines_ahead(1)
        )
    );

    let far = format!("fleetringd-v{}", lines_ahead(2));
    let candidate = format!("fleetringd-v{}-rc.1", lines_ahead(3));
    let forbidden = check(&[
        (candidate.as_str(), true),
        (far.as_str(), false),
        (next.as_str(), false),
    ])?;
    assert_eq!(forbidden.status.code(), Some(1), "{}", printed(&forbidden));
    let stdout = String::from_utf8_lossy(&forbidden.stdout);
    assert!(
        stdout.contains(&format!("available: {}\n", lines_ahead(2)))
            && stdout.contains(&format!(
                "fleet: doesn't allow {}: node 'node-1' runs {}",
                lines_ahead(2),
                current()
            )),
        "{stdout}"
    );
    bench.assert_unchanged()?;

    let release = lines_ahead(2);
    let url = bench.publish(&release, &fake_binary(&release), Defect::None)?;
    let refused = fleet
        .command(&bench.bin, &bench.path())
        .args([
            "upgrade",
            "--version",
            &release.to_string(),
            "--base-url",
            &url,
        ])
        .arg("--control")
        .arg(&fleet.control)
        .output()?;
    assert_eq!(refused.status.code(), Some(1), "{}", printed(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains(&format!(
            "the fleet doesn't allow fleetringd {release}: node 'node-1' runs {}",
            current()
        )),
        "{}",
        printed(&refused)
    );
    assert!(
        bench
            .server
            .requested()
            .iter()
            .all(|path| !path.contains(&format!("fleetringd-v{release}"))),
        "{:?}",
        bench.server.requested()
    );
    bench.assert_unchanged()
}

/// Upgrading a running node drains it, restarts it in place, and waits until it has rejoined and
/// caught up, while a client writes throughout: no write fails beyond its one retry, and none is
/// lost.
#[test]
fn upgrading_a_running_node_drains_restarts_and_rejoins_it() -> TestResult {
    let bench = Bench::new()?;
    let fleet_dir = bench.dir.path().join("fleet");
    let fleet = ProcessFleet::start(&fleet_dir, &bench.bin, 3)?;
    let runtime = tokio::runtime::Runtime::new()?;
    let client = fleet.client()?;
    runtime.block_on(wait_for_members(
        &client,
        &["node-1", "node-2", "node-3"],
        None,
    ))?;
    let name = runtime.block_on(create_history_database(&client))?;
    // The node to upgrade is the database's primary, so the drain has a lease to hand over.
    let primary = runtime
        .block_on(client.locate(&name))?
        .primary
        .ok_or("no primary")?;
    let index = fleet
        .nodes
        .iter()
        .position(|node| node.id == primary.as_str())
        .ok_or("the primary isn't a node of the fleet")?;
    let node = &fleet.nodes[index];
    let before = wait_for("the node writes its process file", || {
        ProcessFile::read(&node.data_dir).ok().flatten()
    })?;
    assert_eq!(Some(before.pid), node.pid());

    let stop = Arc::new(AtomicBool::new(false));
    let writer = runtime.spawn(write_until(
        fleet.client()?,
        name.clone(),
        Arc::clone(&stop),
    ));
    // A reinstall of this build: the release holds the same binary, so the node comes back.
    let url = bench.publish(&current(), &bench.original, Defect::None)?;
    let upgraded = fleet
        .command(&node.bin, &bench.path())
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
        upgraded.status.success(),
        "{}\n{}",
        printed(&upgraded),
        fleet.logs()
    );
    let stdout = String::from_utf8_lossy(&upgraded.stdout);
    for step in [
        format!("drained {primary}: handed over 1 database"),
        format!("restarted {primary} in place (pid {})", before.pid),
        format!("{primary} rejoined the fleet on {}", current()),
        format!("{primary} caught up"),
    ] {
        assert!(stdout.contains(&step), "{step} in {stdout}");
    }
    // Restarted in place: the same process, which counted the restart in its file.
    let after = ProcessFile::read(&node.data_dir)?.ok_or("no process file")?;
    assert_eq!(
        (after.pid, after.restarts),
        (before.pid, before.restarts + 1)
    );
    assert!(
        fs::read_to_string(&node.log)?.contains("SIGHUP: restarting in place"),
        "{}",
        fleet.logs()
    );
    stop.store(true, Ordering::SeqCst);
    let history = runtime.block_on(writer)?;
    assert!(history.acknowledged.len() > 10, "{history:?}");
    runtime.block_on(check_history(&client, &name, &history))?;
    Ok(())
}

/// Waits until the control plane says the fleet has enabled `feature`.
async fn wait_for_feature(client: &fleetringd::data::Client, feature: &str) -> TestResult {
    for _ in 0..1_200 {
        if let Ok(view) = client.status().await
            && view.features().iter().any(|enabled| enabled == feature)
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Err(format!("the fleet never enabled {feature}").into())
}

/// Once the fleet has used `undrain`, a release without it would be refused at its join, or, on a
/// control node, couldn't read the log: `--check`, `--version`, and `--rollback` refuse it before
/// anything is swapped, naming the feature, on a data node and on a control node that is no
/// member. The control-only node then restarts in place on this build.
#[test]
fn a_release_without_an_enabled_feature_is_refused_before_it_is_installed() -> TestResult {
    let bench = Bench::new()?;
    let fleet =
        ProcessFleet::start_with(&bench.dir.path().join("fleet"), &bench.bin, 2, "control")?;
    let runtime = tokio::runtime::Runtime::new()?;
    let client = fleet.client()?;
    runtime.block_on(wait_for_members(&client, &["node-2"], None))?;
    let undrained = node_command(&fleet, &["undrain", "node-2"])?;
    assert!(undrained.status.success(), "{}", printed(&undrained));
    runtime.block_on(wait_for_feature(&client, "undrain"))?;

    let older = line_behind();
    let url = bench.publish(&older, &fake_binary(&older), Defect::None)?;
    let version = older.to_string();
    let lacks = "lacks the fleet feature 'undrain' this fleet uses";
    for node in &fleet.nodes {
        let installed = fs::read(&node.bin)?;
        let upgrade = |args: &[&str]| -> TestResult<Output> {
            Ok(fleet
                .command(&node.bin, &bench.path())
                .arg("upgrade")
                .args(args)
                .arg("--config")
                .arg(&node.config)
                .output()?)
        };

        let check = upgrade(&["--check", "--version", &version, "--base-url", &url])?;
        assert_eq!(
            check.status.code(),
            Some(1),
            "{}: {}",
            node.id,
            printed(&check)
        );
        assert!(
            String::from_utf8_lossy(&check.stdout)
                .contains(&format!("fleet: doesn't allow {older}: it {lacks}")),
            "{}: {}",
            node.id,
            printed(&check)
        );

        let downgrade = upgrade(&["--version", &version, "--base-url", &url])?;
        assert_eq!(
            downgrade.status.code(),
            Some(1),
            "{}: {}",
            node.id,
            printed(&downgrade)
        );
        assert!(
            String::from_utf8_lossy(&downgrade.stderr).contains(&format!(
                "the fleet doesn't allow fleetringd {older}: it {lacks}"
            )),
            "{}: {}",
            node.id,
            printed(&downgrade)
        );
        assert_eq!(fs::read(&node.bin)?, installed, "{} was replaced", node.id);

        let previous = node.bin.with_file_name("fleetringd.previous");
        fs::write(&previous, fake_binary(&older))?;
        fs::set_permissions(&previous, fs::Permissions::from_mode(0o755))?;
        let rollback = upgrade(&["--rollback"])?;
        assert_eq!(
            rollback.status.code(),
            Some(1),
            "{}: {}",
            node.id,
            printed(&rollback)
        );
        assert!(
            String::from_utf8_lossy(&rollback.stderr).contains(lacks),
            "{}: {}",
            node.id,
            printed(&rollback)
        );
        assert_eq!(
            fs::read(&node.bin)?,
            installed,
            "{} was rolled back",
            node.id
        );
        assert_eq!(fs::read(&previous)?, fake_binary(&older));
        fs::remove_file(&previous)?;
    }

    // The control-only node is no member: it restarts in place without a drain, and is back
    // once the control plane answers.
    let control = &fleet.nodes[0];
    let url = bench.publish(&current(), &bench.original, Defect::None)?;
    let reinstalled = fleet
        .command(&control.bin, &bench.path())
        .args([
            "upgrade",
            "--version",
            &current().to_string(),
            "--base-url",
            &url,
        ])
        .arg("--config")
        .arg(&control.config)
        .output()?;
    assert!(
        reinstalled.status.success(),
        "{}\n{}",
        printed(&reinstalled),
        fleet.logs()
    );
    let stdout = String::from_utf8_lossy(&reinstalled.stdout);
    assert!(
        !stdout.contains("drained")
            && stdout.contains("restarted node-1 in place")
            && stdout.contains(&format!("the control plane answers again on {}", current())),
        "{stdout}"
    );
    Ok(())
}

/// A member that died for good on an old release holds the fleet back: it counts toward the
/// window and the feature gate until `node remove` takes it out. A live member is refused unless
/// `--force`, and without a terminal the command needs `--yes`.
#[test]
fn removing_a_member_that_is_gone_unblocks_the_fleet() -> TestResult {
    let bench = Bench::new()?;
    let fleet = ProcessFleet::start(&bench.dir.path().join("fleet"), &bench.bin, 2)?;
    let runtime = tokio::runtime::Runtime::new()?;
    let client = fleet.client()?;
    runtime.block_on(wait_for_members(&client, &["node-1", "node-2"], None))?;
    // "old" joins as 0.3.0 does, reporting nothing, and is never heard from again.
    let old = fleet
        .tls
        .node_network("old")?
        .control_client(fleet.control.clone());
    runtime.block_on(old.join(fleetring_proto::v1::JoinRequest {
        protocol: Some(fleetring_proto::PROTOCOL_VERSION),
        node: Some(fleetring_proto::v1::Node {
            id: "old".into(),
            zone: None,
            host: None,
            weight: 1,
            address: None,
        }),
        software: None,
    }))?;

    let blocked = node_command(&fleet, &["undrain", "node-2"])?;
    assert_eq!(blocked.status.code(), Some(1), "{}", printed(&blocked));
    assert!(
        String::from_utf8_lossy(&blocked.stderr).contains("node 'old' runs fleetringd 0.3.0"),
        "{}",
        printed(&blocked)
    );

    let live = node_command(&fleet, &["remove", "node-2", "--yes"])?;
    assert_eq!(live.status.code(), Some(1), "{}", printed(&live));
    assert!(
        String::from_utf8_lossy(&live.stderr).contains("node 'node-2' is live"),
        "{}",
        printed(&live)
    );

    let down = runtime.block_on(async {
        for _ in 0..1_200 {
            if let Ok(view) = client.status().await
                && view.members().any(|(member, liveness)| {
                    member.id().as_str() == "old" && liveness == fleetring_control::Liveness::Down
                })
            {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        false
    });
    assert!(down, "'old' never went down\n{}", fleet.logs());
    // Without a terminal, removing needs --yes.
    let unconfirmed = node_command(&fleet, &["remove", "old"])?;
    assert_eq!(
        unconfirmed.status.code(),
        Some(1),
        "{}",
        printed(&unconfirmed)
    );
    assert!(
        String::from_utf8_lossy(&unconfirmed.stderr).contains("pass --yes to remove old"),
        "{}",
        printed(&unconfirmed)
    );
    let removed = node_command(&fleet, &["remove", "old", "--yes"])?;
    assert!(removed.status.success(), "{}", printed(&removed));
    assert!(
        String::from_utf8_lossy(&removed.stdout).starts_with("removed old from the fleet"),
        "{}",
        printed(&removed)
    );
    let undrained = node_command(&fleet, &["undrain", "node-2"])?;
    assert!(undrained.status.success(), "{}", printed(&undrained));

    // `--force` removes a live member, which joins again at its next heartbeat.
    let forced = node_command(&fleet, &["remove", "node-2", "--yes", "--force"])?;
    assert!(forced.status.success(), "{}", printed(&forced));
    runtime.block_on(wait_for_members(&client, &["node-2"], None))?;
    Ok(())
}
