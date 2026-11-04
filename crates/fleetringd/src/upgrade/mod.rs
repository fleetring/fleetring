//! `fleetringd upgrade`: a verified self-upgrade, a rollback, and a restart in place
//! (ADR-0038).
//!
//! 1. **Find the release:** the newest final release, or the one `--version` names.
//! 2. **Check the fleet:** with a control plane to ask, refuse a release that would put the fleet
//!    outside its window of two adjacent release lines, before downloading anything.
//! 3. **Download and verify:** the archive, `SHA256SUMS`, and its Sigstore bundle; the signature
//!    with cosign against the release workflow's identity, then the archive's checksum, then the
//!    extracted binary's `--version`. Any failure is a refusal; nothing unverified is installed.
//! 4. **Swap:** the new binary replaces the installed one atomically, which is kept as
//!    `fleetringd.previous` for `--rollback`.
//! 5. **Restart:** with `--config` naming a running node, drain it, send it `SIGHUP` so it
//!    restarts in place, and wait until it has rejoined on the new release and caught up.

mod install;
mod process;
mod release;
mod verify;

use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{Liveness, Release};
use tonic::Status;

pub use process::{PROCESS_FILE, ProcessFile, RESTARTS_VAR};

use crate::data::Client;
use crate::engine::Position;
use crate::upgrade::install::{Installed, check_binary, extract, version_of};
use crate::upgrade::release::{BUNDLE, SUMS, Source, archive_name};
use crate::upgrade::verify::{Cosign, check_sum};

/// How often the restart's waits look again.
const POLL: Duration = Duration::from_millis(200);

/// Why `fleetringd upgrade` refused or failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum UpgradeError {
    /// A download failed.
    #[error("can't fetch {url}: {reason}")]
    Network {
        /// What was fetched.
        url: String,
        /// Why it failed.
        reason: String,
    },
    /// The releases list names no final release.
    #[error("{url} lists no fleetringd release; pass --version")]
    NoRelease {
        /// The releases list.
        url: String,
    },
    /// cosign isn't on `PATH`, so the release's signature can't be verified.
    #[error(
        "cosign isn't installed, so the release's signature can't be verified, and fleetringd \
         never installs an unverified release\n  hint: install cosign 2.4.2 or newer ({install}) \
         and run the command again"
    )]
    CosignMissing {
        /// Where to get it.
        install: &'static str,
    },
    /// The cosign on `PATH` is too old to read a release's bundle.
    #[error(
        "the installed cosign is {found}, too old to verify the release's signature (needs \
         2.4.2 or newer)\n  hint: upgrade cosign ({install}) and run the command again"
    )]
    CosignTooOld {
        /// The version found.
        found: String,
        /// Where to get a newer one.
        install: &'static str,
    },
    /// The signature on `SHA256SUMS` doesn't verify against the release workflow's identity.
    #[error(
        "the signature on SHA256SUMS doesn't verify as the fleetring release workflow's; not \
         installing: {reason}"
    )]
    Signature {
        /// cosign's reason.
        reason: String,
    },
    /// `SHA256SUMS` lists no checksum for the archive.
    #[error("SHA256SUMS lists no checksum for {file}; not installing")]
    NotListed {
        /// The archive.
        file: String,
    },
    /// The archive doesn't match its checksum.
    #[error("checksum mismatch for {file}: expected {expected}, got {actual}; not installing")]
    Checksum {
        /// The archive.
        file: String,
        /// The checksum `SHA256SUMS` lists.
        expected: String,
        /// The archive's.
        actual: String,
    },
    /// The archive can't be read, or holds no binary.
    #[error("the release archive is unusable: {reason}; not installing")]
    Archive {
        /// Why.
        reason: String,
    },
    /// The binary doesn't run, or is another release.
    #[error("the downloaded binary is unusable: {reason}; not installing")]
    Binary {
        /// Why.
        reason: String,
    },
    /// The fleet's window of release lines doesn't allow the release.
    #[error(
        "the fleet doesn't allow fleetringd {release}: node '{member}' runs {member_release}, \
         and a fleet runs at most two adjacent release lines at once\n  hint: upgrade every \
         member to the line between them first"
    )]
    Forbidden {
        /// The release asked for.
        release: Release,
        /// A member it can't run beside.
        member: NodeId,
        /// That member's release.
        member_release: Release,
    },
    /// The binary's directory can't be written.
    #[error(
        "can't write beside {path}: {source}\n  hint: run the command as the user that owns \
         the binary; in the container image, run the new image tag instead"
    )]
    NotWritable {
        /// The binary.
        path: PathBuf,
        /// Why.
        source: std::io::Error,
    },
    /// There is no previous binary to roll back to.
    #[error("there is no previous binary at {path} to roll back to")]
    NoPrevious {
        /// Where it would be.
        path: PathBuf,
    },
    /// A running daemon runs a binary other than the one this command would replace.
    #[error(
        "the daemon runs {daemon}, not {this}\n  hint: run `{daemon} upgrade` with the same \
         arguments"
    )]
    OtherBinary {
        /// The daemon's binary.
        daemon: PathBuf,
        /// This command's binary.
        this: PathBuf,
    },
    /// The control plane refused or didn't answer.
    #[error("the control plane: {0}")]
    Control(String),
    /// The restart didn't finish.
    #[error(
        "{reason}\n  hint: to go back, run `fleetringd upgrade --rollback` with the same --config"
    )]
    Restart {
        /// What didn't happen.
        reason: String,
    },
    /// This platform can't do what was asked.
    #[error("{reason}")]
    Unsupported {
        /// Why.
        reason: &'static str,
    },
    /// A file couldn't be read or written.
    #[error("{path}: {source}")]
    Io {
        /// The file.
        path: PathBuf,
        /// Why.
        source: std::io::Error,
    },
}

impl From<Status> for UpgradeError {
    fn from(status: Status) -> Self {
        Self::Control(format!(
            "{}: {}",
            status.code().description(),
            status.message()
        ))
    }
}

/// What the command does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Installs a release.
    Install,
    /// Reports the newest release and whether the fleet allows it, and changes nothing.
    Check,
    /// Swaps the installed binary with the previous one.
    Rollback,
}

/// Whether the command restarts a running node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Restart {
    /// Drains a running node, restarts it in place, and waits until it rejoins and catches up.
    InPlace,
    /// Only replaces the binary.
    No,
}

/// The node on this machine, from its configuration file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalNode {
    /// Its ID.
    pub id: NodeId,
    /// Its data directory.
    pub data_dir: PathBuf,
}

/// What `fleetringd upgrade` was asked to do.
#[derive(Debug)]
pub struct Options {
    /// Install, check, or roll back.
    pub mode: Mode,
    /// Whether to restart a running node.
    pub restart: Restart,
    /// The release to install; the newest final release if `None`.
    pub version: Option<Release>,
    /// A mirror's directory holding the release's files, as `install.sh --base-url` takes it.
    pub base_url: Option<String>,
    /// Where the releases list is read; GitHub's API if `None`.
    pub releases_url: Option<String>,
    /// The node on this machine, if `--config` named one.
    pub node: Option<LocalNode>,
    /// The control plane, to check the fleet's window and restart the node.
    pub fleet: Option<Client>,
    /// How long each download, the drain, the rejoin, and the catch-up may take.
    pub timeout: Duration,
}

/// Runs `fleetringd upgrade` and returns its report.
///
/// # Errors
///
/// [`UpgradeError`] when it refuses or fails; nothing is changed by a refusal.
pub async fn run(options: Options) -> Result<Outcome, UpgradeError> {
    let exe = std::env::current_exe().map_err(|source| UpgradeError::Io {
        path: PathBuf::from("fleetringd"),
        source,
    })?;
    let installed = Installed::at(&exe)?;
    match options.mode {
        Mode::Check => check(&options).await,
        Mode::Install => install(&options, &installed).await,
        // A new binary that doesn't run can't roll itself back, so the previous one can do it:
        // `fleetringd.previous upgrade --rollback` swaps the pair it belongs to.
        Mode::Rollback => roll_back(&options, &installed.kept_for().unwrap_or(installed)).await,
    }
}

/// What the command did, and whether it counts as a success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// What it prints.
    pub report: String,
    /// Whether a check found a newer release the fleet doesn't allow.
    pub blocked: bool,
}

impl Outcome {
    fn done(report: String) -> Self {
        Self {
            report,
            blocked: false,
        }
    }
}

/// The release this binary is.
fn this_release() -> Release {
    crate::software::release()
}

async fn target_release(options: &Options, source: &Source) -> Result<Release, UpgradeError> {
    match &options.version {
        Some(version) => Ok(version.clone()),
        None if source.is_mirror() => Err(UpgradeError::Unsupported {
            reason: "--base-url needs --version",
        }),
        None => source.newest().await,
    }
}

fn source(options: &Options) -> Result<Source, UpgradeError> {
    Source::new(
        options.releases_url.clone(),
        options.base_url.clone(),
        options.timeout,
    )
}

/// Every member the fleet's window refuses `release` beside: the first one found, in ID order.
async fn check_fleet(
    fleet: &Client,
    node: Option<&NodeId>,
    release: &Release,
) -> Result<(), UpgradeError> {
    let view = fleet.status().await?;
    for (member, _) in view.members() {
        // The node being upgraded leaves its own release behind.
        if Some(member.id()) == node {
            continue;
        }
        let theirs = member.running();
        if !release.runs_beside(theirs.release()) {
            return Err(UpgradeError::Forbidden {
                release: release.clone(),
                member: member.id().clone(),
                member_release: theirs.release().clone(),
            });
        }
    }
    Ok(())
}

async fn check(options: &Options) -> Result<Outcome, UpgradeError> {
    let source = source(options)?;
    let installed = this_release();
    let available = target_release(options, &source).await?;
    let mut report = format!("installed: {installed}\navailable: {available}\n");
    let newer = available > installed;
    if !newer && options.version.is_none() {
        report.push_str("fleetringd is up to date\n");
        return Ok(Outcome::done(report));
    }
    let Some(fleet) = &options.fleet else {
        report.push_str("fleet: not checked; pass --config or --control to check it\n");
        return Ok(Outcome::done(report));
    };
    let node = options.node.as_ref().map(|node| &node.id);
    match check_fleet(fleet, node, &available).await {
        Ok(()) => {
            let _ = writeln!(report, "fleet: allows {available}");
            Ok(Outcome::done(report))
        }
        Err(UpgradeError::Forbidden {
            member,
            member_release,
            ..
        }) => {
            let _ = writeln!(
                report,
                "fleet: doesn't allow {available}: node '{member}' runs {member_release}, and a \
                 fleet runs at most two adjacent release lines at once"
            );
            Ok(Outcome {
                report,
                blocked: newer,
            })
        }
        Err(other) => Err(other),
    }
}

async fn install(options: &Options, installed: &Installed) -> Result<Outcome, UpgradeError> {
    if cfg!(windows) {
        return Err(UpgradeError::Unsupported {
            reason: "fleetringd upgrade can't replace a binary on Windows; download the .zip \
                     from https://github.com/fleetring/fleetring/releases",
        });
    }
    let source = source(options)?;
    let current = this_release();
    let release = target_release(options, &source).await?;
    if options.version.is_none() && release <= current {
        return Ok(Outcome::done(format!(
            "fleetringd {current} is up to date\n"
        )));
    }
    let running = running_daemon(options, installed)?;
    if let Some(fleet) = &options.fleet {
        check_fleet(fleet, options.node.as_ref().map(|node| &node.id), &release).await?;
    }
    installed.check_writable()?;
    // Verification needs cosign; find it before downloading anything.
    let verifier = Cosign::find()?;

    let scratch = tempfile::tempdir().map_err(|source| UpgradeError::Io {
        path: std::env::temp_dir(),
        source,
    })?;
    let target = crate::software::TARGET;
    let archive_file = archive_name(&release, target);
    let sums = source.download(&release, SUMS, scratch.path()).await?;
    let bundle = source.download(&release, BUNDLE, scratch.path()).await?;
    let archive = source
        .download(&release, &archive_file, scratch.path())
        .await?;
    verifier.verify(&sums, &bundle)?;
    check_sum(&sums, &archive_file, &archive)?;
    let binary = scratch.path().join("fleetringd");
    extract(&archive, &release, target, &binary)?;
    check_binary(&binary, &release)?;
    installed.replace_with(&binary)?;

    let mut report = format!(
        "installed fleetringd {release} at {} (verified the signature and the checksum); the \
         previous binary, {current}, is kept at {}\n",
        installed.path().display(),
        installed.previous().display()
    );
    report.push_str(&restart(options, running, &release).await?);
    Ok(Outcome::done(report))
}

async fn roll_back(options: &Options, installed: &Installed) -> Result<Outcome, UpgradeError> {
    if cfg!(windows) {
        return Err(UpgradeError::Unsupported {
            reason: "fleetringd upgrade can't replace a binary on Windows",
        });
    }
    let previous = installed.previous();
    if !previous.is_file() {
        return Err(UpgradeError::NoPrevious { path: previous });
    }
    let release = version_of(&previous)?;
    let running = running_daemon(options, installed)?;
    if let Some(fleet) = &options.fleet {
        check_fleet(fleet, options.node.as_ref().map(|node| &node.id), &release).await?;
    }
    let current = this_release();
    installed.roll_back()?;
    let mut report = format!(
        "rolled back to fleetringd {release} at {}; {current} is kept at {}\n",
        installed.path().display(),
        previous.display()
    );
    report.push_str(&restart(options, running, &release).await?);
    Ok(Outcome::done(report))
}

/// The daemon running on this machine for `options.node`, if any, as its process file
/// describes it, or `Some(None)` if one runs that wrote no process file, as 0.3.0 doesn't.
///
/// # Errors
///
/// [`UpgradeError::OtherBinary`] if the daemon runs a binary other than `installed`.
fn running_daemon(
    options: &Options,
    installed: &Installed,
) -> Result<Option<Option<ProcessFile>>, UpgradeError> {
    let Some(node) = &options.node else {
        return Ok(None);
    };
    if !process::is_running(&node.data_dir)? {
        return Ok(None);
    }
    let file = ProcessFile::read(&node.data_dir).map_err(|source| UpgradeError::Io {
        path: node.data_dir.join(PROCESS_FILE),
        source,
    })?;
    if let Some(file) = &file {
        let daemon = Installed::at(&file.exe)
            .map_or_else(|_| file.exe.clone(), |daemon| daemon.path().to_owned());
        if daemon != installed.path() {
            return Err(UpgradeError::OtherBinary {
                daemon,
                this: installed.path().to_owned(),
            });
        }
    }
    Ok(Some(file))
}

/// Restarts the running node, if asked to and there is one, and says what happened.
async fn restart(
    options: &Options,
    running: Option<Option<ProcessFile>>,
    release: &Release,
) -> Result<String, UpgradeError> {
    let (Some(node), Some(daemon)) = (&options.node, running) else {
        return Ok(match &options.node {
            Some(node) => format!(
                "node {} isn't running; start it to run {release}\n",
                node.id
            ),
            None => format!("restart the daemon to run {release}\n"),
        });
    };
    let Some(daemon) = daemon else {
        return Ok(format!(
            "node {} runs a release that can't restart in place; restart it with its \
             supervisor, such as `systemctl restart fleetringd`, or stop and start it: it drains \
             before it stops\n",
            node.id
        ));
    };
    if options.restart == Restart::No {
        return Ok(format!(
            "node {} still runs {}; restart it to run {release}: send it SIGHUP, or restart it \
             with its supervisor\n",
            node.id, daemon.version
        ));
    }
    let Some(fleet) = &options.fleet else {
        return Err(UpgradeError::Restart {
            reason: "restarting a node needs the control plane; pass --config with a control \
                     address, or --no-restart"
                .to_owned(),
        });
    };
    let mut report = String::new();
    let moved = drain(fleet, &node.id, options.timeout).await?;
    let _ = writeln!(report, "drained {}: {moved}", node.id);
    process::restart(daemon.pid)?;
    wait_for_restart(&node.data_dir, &daemon, options.timeout).await?;
    let _ = writeln!(
        report,
        "restarted {} in place (pid {})",
        node.id, daemon.pid
    );
    wait_for_rejoin(fleet, &node.id, release, options.timeout).await?;
    let _ = writeln!(report, "{} rejoined the fleet on {release}", node.id);
    let caught_up = catch_up(fleet, &node.id, options.timeout).await?;
    let _ = writeln!(report, "{} caught up: {caught_up}", node.id);
    Ok(report)
}

/// Drains `node` and waits, within `limit`, until it is primary for nothing. A database it
/// can't hand over doesn't fail the upgrade: the node's own stop hands it over or lets it fail
/// over, as a stop always does.
async fn drain(fleet: &Client, node: &NodeId, limit: Duration) -> Result<String, UpgradeError> {
    let primaries = fleet.drain_node(node).await?;
    if primaries.is_empty() {
        return Ok("it was primary for nothing".to_owned());
    }
    let waited = tokio::time::timeout(limit, async {
        loop {
            let databases = fleet.list_databases().await?;
            if !databases
                .iter()
                .any(|location| location.primary.as_ref() == Some(node))
            {
                return Ok::<_, UpgradeError>(());
            }
            tokio::time::sleep(POLL).await;
        }
    })
    .await;
    match waited {
        Ok(result) => {
            result?;
            Ok(format!(
                "handed over {} database{}",
                primaries.len(),
                if primaries.len() == 1 { "" } else { "s" }
            ))
        }
        Err(_) => Ok(format!(
            "some databases weren't handed over within {}s; they fail over when it stops",
            limit.as_secs()
        )),
    }
}

/// Waits until the daemon `before` describes has restarted: the process file in `data_dir`
/// counts one more restart. The restarted daemon writes it once it has started.
async fn wait_for_restart(
    data_dir: &std::path::Path,
    before: &ProcessFile,
    limit: Duration,
) -> Result<(), UpgradeError> {
    let restarted = tokio::time::timeout(limit, async {
        loop {
            if let Ok(Some(now)) = ProcessFile::read(data_dir)
                && now.restarts > before.restarts
            {
                return;
            }
            tokio::time::sleep(POLL).await;
        }
    })
    .await;
    restarted.map_err(|_| UpgradeError::Restart {
        reason: format!(
            "the daemon (pid {}) didn't restart within {}s; its log says why",
            before.pid,
            limit.as_secs()
        ),
    })
}

/// Waits until the control plane lists `node` as live on `release`.
async fn wait_for_rejoin(
    fleet: &Client,
    node: &NodeId,
    release: &Release,
    limit: Duration,
) -> Result<(), UpgradeError> {
    let rejoined = tokio::time::timeout(limit, async {
        loop {
            // The control plane may be the node itself, restarting: keep asking.
            if let Ok(view) = fleet.status().await
                && view.members().any(|(member, liveness)| {
                    member.id() == node
                        && liveness == Liveness::Live
                        // A control node too old to record software (0.3.0) reports none; then
                        // liveness after the restart is all it can confirm.
                        && member.software().is_none_or(|software| software.release() == release)
                })
            {
                return;
            }
            tokio::time::sleep(POLL).await;
        }
    })
    .await;
    rejoined.map_err(|_| UpgradeError::Restart {
        reason: format!(
            "{node} didn't rejoin the fleet on {release} within {}s; its log says why",
            limit.as_secs()
        ),
    })
}

/// Waits until each of `node`'s replicas has reported the position its primary reported when
/// the wait began, and says how many there were.
async fn catch_up(fleet: &Client, node: &NodeId, limit: Duration) -> Result<String, UpgradeError> {
    // Each database `node` follows, with the primary's position when the wait begins.
    let mut targets: Vec<(fleetring_control::DatabaseName, Position)> = Vec::new();
    for location in fleet.list_databases().await? {
        let follows = location
            .replicas
            .iter()
            .any(|replica| &replica.node == node)
            && location.primary.as_ref() != Some(node);
        let primary = location
            .primary_replica()
            .and_then(|primary| primary.position.clone());
        if let (true, Some(position)) = (follows, primary) {
            targets.push((location.database, position));
        }
    }
    let count = targets.len();
    let caught_up = tokio::time::timeout(limit, async {
        while !targets.is_empty() {
            let mut behind = Vec::with_capacity(targets.len());
            for (database, wanted) in targets {
                let reached = match fleet.locate(&database).await {
                    Ok(location) => location.replicas.iter().any(|replica| {
                        &replica.node == node
                            && replica.position.as_ref().is_some_and(|at| *at >= wanted)
                    }),
                    // Dropped meanwhile: nothing to catch up on.
                    Err(status) if status.code() == tonic::Code::NotFound => true,
                    Err(_) => false,
                };
                if !reached {
                    behind.push((database, wanted));
                }
            }
            targets = behind;
            if !targets.is_empty() {
                tokio::time::sleep(POLL).await;
            }
        }
    })
    .await;
    caught_up.map_err(|_| UpgradeError::Restart {
        reason: format!(
            "{node} rejoined but didn't catch up on every database within {}s",
            limit.as_secs()
        ),
    })?;
    Ok(format!(
        "{count} database{} at its primary's position",
        if count == 1 { "" } else { "s" }
    ))
}
