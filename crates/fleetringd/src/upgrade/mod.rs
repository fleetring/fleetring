//! `fleetring upgrade`: a verified self-upgrade, a rollback, and a restart in place
//! (ADR-0038).
//!
//! 1. **Find the release:** the newest final release, or the one `--version` names.
//! 2. **Check the fleet:** with a control plane to ask, refuse a release that would put the fleet
//!    outside its window of two adjacent release lines, before downloading anything.
//! 3. **Download and verify:** the archive, `SHA256SUMS`, and its Sigstore bundle; the signature
//!    with cosign against the release workflow's identity, then the archive's checksum, then the
//!    extracted binary's `--version`. Any failure is a refusal; nothing unverified is installed.
//! 4. **Swap:** the new binary replaces the installed one atomically, which is kept as
//!    `<name>.previous` for `--rollback`. A release before 0.7 can't run `serve`, so installing
//!    one first checks how the node was started (ADR-0043, point 3.5).
//! 5. **Restart:** with `--config` naming a running node, drain it, send it `SIGHUP` so it
//!    restarts in place, and wait until it has rejoined on the new release and caught up.

mod install;
mod process;
mod release;
mod verify;

use std::fmt::Write as _;
use std::io::{BufRead as _, IsTerminal as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{Liveness, Release};
use tonic::Status;

pub use process::{PROCESS_FILE, ProcessFile, RESTARTS_VAR};

use crate::MembershipView;
use crate::data::Client;
use crate::engine::Position;
use crate::upgrade::install::{Installed, Reported, check_binary, extract, software_of};
use crate::upgrade::release::{BUNDLE, SUMS, Source, archive_name};
use crate::upgrade::verify::{Cosign, check_sum};

/// How often the restart's waits look again.
const POLL: Duration = Duration::from_millis(200);

/// Why `fleetring upgrade` refused or failed.
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
    #[error("{url} lists no fleetring release; pass --version")]
    NoRelease {
        /// The releases list.
        url: String,
    },
    /// cosign isn't on `PATH`, so the release's signature can't be verified.
    #[error(
        "cosign isn't installed, so the release's signature can't be verified, and fleetring \
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
        "the fleet doesn't allow fleetring {release}: node '{member}' runs {member_release}, \
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
    /// The release lacks a fleet feature the fleet has enabled, so the fleet would refuse the
    /// node, or a control node on it couldn't read the control log.
    #[error(
        "the fleet doesn't allow fleetring {release}: it lacks the fleet feature '{feature}' \
         this fleet uses, so the fleet would refuse the node\n  hint: install a release that \
         supports it; a fleet never gives up a feature it has used"
    )]
    MissingFeature {
        /// The release asked for.
        release: Release,
        /// The enabled feature it lacks.
        feature: String,
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
    /// The release can't run the node's command line: the node was started with `serve`, which a
    /// release before 0.7 doesn't know (ADR-0043, point 3.5).
    #[error(
        "fleetring {release} doesn't know `serve`, which this node was started with\n  hint: \
         start the node with `fleetringd --config <file>` instead, restart it, then roll back \
         again"
    )]
    StartedWithServe {
        /// The release that would be installed.
        release: Release,
    },
    /// The release doesn't know `serve`, and no running node shows how it was started: there is
    /// no terminal to ask on, and `--yes` wasn't given.
    #[error(
        "fleetring {release} doesn't know `serve`, and no running node shows whether it was \
         started with it\n  hint: pass --config naming the node, or --yes if no node on this \
         machine starts with `fleetring serve`"
    )]
    StartCommandUnknown {
        /// The release that would be installed.
        release: Release,
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
        "{reason}\n  hint: to go back, run `fleetring upgrade --rollback` with the same --config"
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

/// How `upgrade` learns whether a node it can't see runs `serve`, before it installs a release
/// that doesn't know it (ADR-0043, point 3.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartCommand {
    /// `--yes`: no node on this machine starts with `serve`.
    Confirmed,
    /// Ask on the terminal.
    Ask,
    /// There is no terminal to ask on: refuse.
    Unknown,
}

/// The node on this machine, from its configuration file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalNode {
    /// Its ID.
    pub id: NodeId,
    /// Its data directory.
    pub data_dir: PathBuf,
}

/// What `fleetring upgrade` was asked to do.
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
    /// Whether a node that isn't running may have been started with `serve`.
    pub start_command: StartCommand,
}

/// Runs `fleetring upgrade` and returns its report.
///
/// # Errors
///
/// [`UpgradeError`] when it refuses or fails; nothing is changed by a refusal.
pub async fn run(options: Options) -> Result<Outcome, UpgradeError> {
    let exe = std::env::current_exe().map_err(|source| UpgradeError::Io {
        path: PathBuf::from("fleetring"),
        source,
    })?;
    let installed = Installed::at(&exe)?;
    match options.mode {
        Mode::Check => check(&options).await,
        Mode::Install => install(&options, &installed).await,
        // A new binary that doesn't run can't roll itself back, so the previous one can do it:
        // `<name>.previous upgrade --rollback` swaps the pair it belongs to.
        Mode::Rollback => roll_back(&options, &installed.kept_for().unwrap_or(installed)).await,
    }
}

/// What the command did, and whether it counts as a success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// What it prints.
    pub report: String,
    /// Whether a check found that the fleet doesn't allow the release.
    pub blocked: bool,
}

impl Outcome {
    fn done(report: String) -> Self {
        Self {
            report,
            blocked: false,
        }
    }

    fn blocked(report: String) -> Self {
        Self {
            report,
            blocked: true,
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

/// What the fleet allows: its members' releases and the fleet features it has enabled, from the
/// control plane, if there is one to ask.
async fn fleet_view(options: &Options) -> Result<Option<MembershipView>, UpgradeError> {
    match &options.fleet {
        Some(fleet) => Ok(Some(fleet.status().await?)),
        None => Ok(None),
    }
}

/// Checks that `release` runs beside every member but `node`, the one being upgraded, which
/// leaves its own release behind.
///
/// # Errors
///
/// [`UpgradeError::Forbidden`] naming the first member, in ID order, it can't run beside.
fn check_window(
    view: &MembershipView,
    node: Option<&NodeId>,
    release: &Release,
) -> Result<(), UpgradeError> {
    for (member, _) in view.members() {
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

/// Checks that `software` supports every fleet feature the fleet has enabled: the fleet refuses
/// a node, or a control node can't read the log, otherwise.
///
/// # Errors
///
/// [`UpgradeError::MissingFeature`] naming the first feature it lacks.
fn check_features(view: &MembershipView, software: &Reported) -> Result<(), UpgradeError> {
    match view
        .features()
        .iter()
        .find(|feature| !software.features.contains(*feature))
    {
        Some(feature) => Err(UpgradeError::MissingFeature {
            release: software.release.clone(),
            feature: feature.clone(),
        }),
        None => Ok(()),
    }
}

async fn check(options: &Options) -> Result<Outcome, UpgradeError> {
    let source = source(options)?;
    let installed = this_release();
    let available = target_release(options, &source).await?;
    let mut report = format!("installed: {installed}\navailable: {available}\n");
    if available <= installed && options.version.is_none() {
        report.push_str("fleetring is up to date\n");
        return Ok(Outcome::done(report));
    }
    let Some(view) = fleet_view(options).await? else {
        report.push_str("fleet: not checked; pass --config or --control to check it\n");
        return Ok(Outcome::done(report));
    };
    let node = options.node.as_ref().map(|node| &node.id);
    let mut verdict = check_window(&view, node, &available);
    if verdict.is_ok() && !view.features().is_empty() {
        // What a release supports is known only to its binary, so the check fetches it, and
        // verifies it as an upgrade would, into a scratch directory.
        let software = if available == installed {
            Reported::this_binary()
        } else {
            let scratch = scratch()?;
            software_of(&fetch(&source, &available, scratch.path()).await?)?
        };
        verdict = check_features(&view, &software);
    }
    match verdict {
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
            Ok(Outcome::blocked(report))
        }
        Err(UpgradeError::MissingFeature { feature, .. }) => {
            let _ = writeln!(
                report,
                "fleet: doesn't allow {available}: it lacks the fleet feature '{feature}' this \
                 fleet uses"
            );
            Ok(Outcome::blocked(report))
        }
        Err(other) => Err(other),
    }
}

fn scratch() -> Result<tempfile::TempDir, UpgradeError> {
    tempfile::tempdir().map_err(|source| UpgradeError::Io {
        path: std::env::temp_dir(),
        source,
    })
}

/// Downloads `release` into `scratch`, verifies its signature and checksum, and extracts its
/// binary, which says it is `release`. Returns the binary's path.
async fn fetch(
    source: &Source,
    release: &Release,
    scratch: &Path,
) -> Result<PathBuf, UpgradeError> {
    // Verification needs cosign; find it before downloading anything.
    let verifier = Cosign::find()?;
    let target = crate::software::TARGET;
    let archive_file = archive_name(release, target);
    let sums = source.download(release, SUMS, scratch).await?;
    let bundle = source.download(release, BUNDLE, scratch).await?;
    let archive = source.download(release, &archive_file, scratch).await?;
    verifier.verify(&sums, &bundle)?;
    check_sum(&sums, &archive_file, &archive)?;
    let binary = scratch.join("fleetring");
    extract(&archive, release, target, &binary)?;
    check_binary(&binary, release)?;
    Ok(binary)
}

async fn install(options: &Options, installed: &Installed) -> Result<Outcome, UpgradeError> {
    if cfg!(windows) {
        return Err(UpgradeError::Unsupported {
            reason: "fleetring upgrade can't replace a binary on Windows; download the .zip \
                     from https://github.com/fleetring/fleetring/releases",
        });
    }
    let source = source(options)?;
    let current = this_release();
    let release = target_release(options, &source).await?;
    if options.version.is_none() && release <= current {
        return Ok(Outcome::done(format!(
            "fleetring {current} is up to date\n"
        )));
    }
    let running = running_daemon(options, installed)?;
    check_start_command(options, running.as_ref(), &release)?;
    let view = fleet_view(options).await?;
    if let Some(view) = &view {
        check_window(view, options.node.as_ref().map(|node| &node.id), &release)?;
    }
    installed.check_writable()?;
    let scratch = scratch()?;
    let binary = fetch(&source, &release, scratch.path()).await?;
    if let Some(view) = &view {
        check_features(view, &software_of(&binary)?)?;
    }
    installed.replace_with(&binary)?;

    let mut report = format!(
        "installed fleetring {release} at {} (verified the signature and the checksum); the \
         previous binary, {current}, is kept at {}\n",
        installed.path().display(),
        installed.previous().display()
    );
    report.push_str(&complete_names(installed));
    report.push_str(&restart(options, installed, running, &release).await?);
    Ok(Outcome::done(report))
}

async fn roll_back(options: &Options, installed: &Installed) -> Result<Outcome, UpgradeError> {
    if cfg!(windows) {
        return Err(UpgradeError::Unsupported {
            reason: "fleetring upgrade can't replace a binary on Windows",
        });
    }
    let previous = installed.previous();
    if !previous.is_file() {
        return Err(UpgradeError::NoPrevious { path: previous });
    }
    let software = software_of(&previous)?;
    let release = software.release.clone();
    let running = running_daemon(options, installed)?;
    check_start_command(options, running.as_ref(), &release)?;
    if let Some(view) = fleet_view(options).await? {
        check_window(&view, options.node.as_ref().map(|node| &node.id), &release)?;
        check_features(&view, &software)?;
    }
    let current = this_release();
    installed.roll_back()?;
    let mut report = format!(
        "rolled back to fleetring {release} at {}; {current} is kept at {}\n",
        installed.path().display(),
        previous.display()
    );
    report.push_str(&complete_names(installed));
    report.push_str(&restart(options, installed, running, &release).await?);
    Ok(Outcome::done(report))
}

/// The first release line that knows `serve` (ADR-0043, point 1.1).
const SERVE_LINE: (u64, u64) = (0, 7);

/// Whether `release` knows `serve`: it is 0.7 or later, or on this build's own line, since this
/// build knows it even before its release sets its version.
fn knows_serve(release: &Release) -> bool {
    release.line() >= SERVE_LINE || release.line() >= this_release().line()
}

/// Refuses to install `release` if it can't run the node's command line: a release before 0.7
/// doesn't know `serve` (ADR-0043, point 3.5). `running` is [`running_daemon`]'s answer. A
/// running node's process file says how it was started; without one, `options.start_command`
/// says whether to ask, to trust `--yes`, or to refuse. It never only warns.
///
/// # Errors
///
/// [`UpgradeError::StartedWithServe`] if the node was started with `serve`, or the answer on the
/// terminal says one was, and [`UpgradeError::StartCommandUnknown`] if it can't tell and can't
/// ask.
fn check_start_command(
    options: &Options,
    running: Option<&Option<ProcessFile>>,
    release: &Release,
) -> Result<(), UpgradeError> {
    if knows_serve(release) {
        return Ok(());
    }
    let refused = || UpgradeError::StartedWithServe {
        release: release.clone(),
    };
    match running {
        Some(Some(daemon)) if daemon.started_with_serve() => Err(refused()),
        // Started without `serve`, or by a release before 0.4 that writes no process file and
        // doesn't know `serve` either.
        Some(_) => Ok(()),
        None => match options.start_command {
            StartCommand::Confirmed => Ok(()),
            StartCommand::Unknown => Err(UpgradeError::StartCommandUnknown {
                release: release.clone(),
            }),
            StartCommand::Ask => {
                if ask_whether_serve(release)? {
                    Err(refused())
                } else {
                    Ok(())
                }
            }
        },
    }
}

/// Asks on the terminal whether any node on this machine starts with `fleetring serve`, and
/// returns the answer: yes unless it is `n` or `no`, or empty, the default.
fn ask_whether_serve(release: &Release) -> Result<bool, UpgradeError> {
    let unknown = |_| UpgradeError::StartCommandUnknown {
        release: release.clone(),
    };
    let stdin = std::io::stdin();
    if !stdin.is_terminal() {
        return Err(UpgradeError::StartCommandUnknown {
            release: release.clone(),
        });
    }
    let mut stderr = std::io::stderr();
    write!(
        stderr,
        "does any node on this machine start with 'fleetring serve'? fleetring {release} can't \
         run it [y/N] "
    )
    .and_then(|()| stderr.flush())
    .map_err(unknown)?;
    let mut answer = String::new();
    stdin.lock().read_line(&mut answer).map_err(unknown)?;
    let answer = answer.trim();
    Ok(!(answer.is_empty()
        || answer.eq_ignore_ascii_case("n")
        || answer.eq_ignore_ascii_case("no")))
}

/// Puts the `fleetring` name beside a binary installed as `fleetringd` (ADR-0043, point 3.4), and
/// says so. The swap has happened by now, so a link that can't be made only adds a line.
fn complete_names(installed: &Installed) -> String {
    match installed.complete_names() {
        Ok(Some(link)) => format!("added fleetring beside fleetringd at {}\n", link.display()),
        Ok(None) => String::new(),
        Err(error) => format!(
            "can't add fleetring beside {}: {error}; add it with `ln -s fleetringd fleetring` in \
             that directory\n",
            installed.path().display()
        ),
    }
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
    installed: &Installed,
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
    // A node that runs the control role alone isn't a member: it has no primaries to drain and
    // no replicas to catch up, and it is back once the control plane answers again.
    let member = fleet
        .status()
        .await?
        .members()
        .any(|(member, _)| member.id() == &node.id);
    if member {
        let moved = drain(fleet, &node.id, options.timeout).await?;
        let _ = writeln!(report, "drained {}: {moved}", node.id);
    }
    // The download and the drain took a while: the daemon may have stopped, or its supervisor
    // may have restarted it, meanwhile. Signal the daemon that runs now, if one does.
    let Some(Some(now)) = running_daemon(options, installed)? else {
        return Err(UpgradeError::Restart {
            reason: format!(
                "node {} stopped while it was upgraded; start it to run {release}",
                node.id
            ),
        });
    };
    if now != daemon && now.version == release.to_string() {
        let _ = writeln!(
            report,
            "{} was restarted meanwhile, on {release} (pid {})",
            node.id, now.pid
        );
    } else {
        process::restart(now.pid)?;
        wait_for_restart(&node.data_dir, &now, options.timeout).await?;
        let _ = writeln!(report, "restarted {} in place (pid {})", node.id, now.pid);
    }
    if !member {
        wait_for_control(fleet, options.timeout).await?;
        let _ = writeln!(report, "the control plane answers again on {release}");
        return Ok(report);
    }
    wait_for_rejoin(fleet, &node.id, release, options.timeout).await?;
    let _ = writeln!(report, "{} rejoined the fleet on {release}", node.id);
    let caught_up = catch_up(fleet, &node.id, options.timeout).await?;
    let _ = writeln!(report, "{} caught up: {caught_up}", node.id);
    Ok(report)
}

/// Waits until the control plane answers, as after a control-only node restarted.
async fn wait_for_control(fleet: &Client, limit: Duration) -> Result<(), UpgradeError> {
    let answered = tokio::time::timeout(limit, async {
        while fleet.status().await.is_err() {
            tokio::time::sleep(POLL).await;
        }
    })
    .await;
    answered.map_err(|_| UpgradeError::Restart {
        reason: format!(
            "the control plane didn't answer within {}s of the restart; its log says why",
            limit.as_secs()
        ),
    })
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

#[cfg(all(test, unix))]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    /// A scratch install: the installed binary, a previous binary that says it is a given
    /// release, and a data directory.
    struct Scratch {
        dir: tempfile::TempDir,
        installed: Installed,
    }

    impl Scratch {
        fn new(previous: &str) -> TestResult<Self> {
            let dir = tempfile::tempdir()?;
            let bin = dir.path().join("bin");
            fs::create_dir_all(&bin)?;
            let path = bin.join("fleetring");
            fs::write(&path, b"this build")?;
            let kept = bin.join("fleetring.previous");
            fs::write(&kept, format!("#!/bin/sh\necho 'fleetringd {previous}'\n"))?;
            fs::set_permissions(&kept, fs::Permissions::from_mode(0o755))?;
            fs::create_dir_all(dir.path().join("data"))?;
            let installed = Installed::at(&path)?;
            Ok(Self { dir, installed })
        }

        fn data_dir(&self) -> PathBuf {
            self.dir.path().join("data")
        }

        /// A daemon of the installed binary running on the data directory, started with
        /// `args`; it runs while the returned lock is held.
        fn run_daemon(&self, args: &[&str]) -> TestResult<crate::DataDir> {
            let lock = crate::DataDir::lock(self.data_dir())?;
            ProcessFile {
                pid: std::process::id(),
                exe: self.installed.path().to_owned(),
                version: "0.7.0".to_owned(),
                restarts: 0,
                args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            }
            .write(&self.data_dir())?;
            Ok(lock)
        }

        fn options(&self, mode: Mode, restart: Restart, version: Option<&str>) -> Options {
            Options {
                mode,
                restart,
                version: version.and_then(|version| version.parse().ok()),
                base_url: Some("http://127.0.0.1:1".to_owned()),
                releases_url: None,
                node: Some(LocalNode {
                    id: "node-1".parse().expect("a node ID"),
                    data_dir: self.data_dir(),
                }),
                fleet: None,
                timeout: Duration::from_secs(1),
                start_command: StartCommand::Unknown,
            }
        }

        /// Whether neither binary has moved.
        fn unchanged(&self) -> bool {
            fs::read(self.installed.path()).ok().as_deref() == Some(&b"this build"[..])
                && fs::read_to_string(self.installed.previous())
                    .is_ok_and(|kept| kept.contains("fleetringd"))
        }
    }

    // The releases before the rename here are 0.5: a build before its own release still says it
    // is the previous one, 0.6, and its own line counts as knowing `serve`.

    #[tokio::test]
    async fn a_rollback_past_the_rename_is_refused_for_a_node_started_with_serve() -> TestResult {
        for restart in [Restart::InPlace, Restart::No] {
            for args in [
                &["serve", "--config", "x"][..],
                &["--log-format", "json", "serve", "--config", "x"],
            ] {
                let scratch = Scratch::new("0.5.0")?;
                let _running = scratch.run_daemon(args)?;
                let mut options = scratch.options(Mode::Rollback, restart, None);
                // Even `--yes` doesn't override what the process file shows.
                options.start_command = StartCommand::Confirmed;
                let refused = roll_back(&options, &scratch.installed).await;
                assert!(
                    matches!(&refused, Err(UpgradeError::StartedWithServe { release })
                        if release.to_string() == "0.5.0"),
                    "{restart:?} {args:?}: {refused:?}"
                );
                assert!(scratch.unchanged(), "{restart:?} {args:?}: swapped");
            }
        }
        // A node started the 0.6 way rolls back.
        let scratch = Scratch::new("0.5.0")?;
        let _running = scratch.run_daemon(&["--config", "x"])?;
        let options = scratch.options(Mode::Rollback, Restart::No, None);
        let outcome = roll_back(&options, &scratch.installed).await?;
        assert!(
            outcome.report.starts_with("rolled back to fleetring 0.5.0"),
            "{}",
            outcome.report
        );
        assert!(!scratch.unchanged());
        // A previous release that knows `serve` rolls back whatever the node runs.
        let scratch = Scratch::new("0.7.1")?;
        let _running = scratch.run_daemon(&["serve", "--config", "x"])?;
        let options = scratch.options(Mode::Rollback, Restart::No, None);
        roll_back(&options, &scratch.installed).await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_downgrade_past_the_rename_without_a_node_needs_yes() -> TestResult {
        for with_config in [false, true] {
            let scratch = Scratch::new("0.7.0")?;
            let mut options = scratch.options(Mode::Install, Restart::No, Some("0.5.0"));
            if !with_config {
                options.node = None;
            }
            // No --config, or a node that isn't running: nothing shows how it starts.
            let refused = install(&options, &scratch.installed).await;
            assert!(
                matches!(&refused, Err(UpgradeError::StartCommandUnknown { release })
                    if release.to_string() == "0.5.0"),
                "{refused:?}"
            );
            assert!(scratch.unchanged());
            // With --yes it goes on to the download, which this mirror refuses: the swap itself
            // is the upgrade suite's to test.
            options.start_command = StartCommand::Confirmed;
            let fetched = install(&options, &scratch.installed).await;
            assert!(
                matches!(
                    &fetched,
                    Err(UpgradeError::Network { .. }
                        | UpgradeError::CosignMissing { .. }
                        | UpgradeError::CosignTooOld { .. })
                ),
                "{fetched:?}"
            );
            assert!(scratch.unchanged());
        }
        // A release that knows `serve` needs no confirmation.
        let scratch = Scratch::new("0.7.0")?;
        let mut options = scratch.options(Mode::Install, Restart::No, Some("0.7.0"));
        options.node = None;
        let fetched = install(&options, &scratch.installed).await;
        assert!(
            !matches!(&fetched, Err(UpgradeError::StartCommandUnknown { .. })),
            "{fetched:?}"
        );
        Ok(())
    }
}
