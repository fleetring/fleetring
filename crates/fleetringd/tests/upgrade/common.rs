//! What the upgrade tests share: fake releases served from a local directory, a stand-in
//! `cosign` that checks the arguments it is given, fleets of real daemon processes, and a writer
//! whose history is checked at the end.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, LeaseEpoch, Liveness, Release};
use fleetringd::data::Client;
use fleetringd::engine::{Statement, Value};
use fleetringd::software::TARGET;

use crate::support::Tls;

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn Error>>;

/// The identity the release workflow signs with, as `RELEASING.md` names it.
pub(crate) const RELEASE_IDENTITY: &str = "https://github.com/fleetring/fleetring/.github/workflows/release.yml@refs/tags/fleetringd-v9.9.9";

/// The issuer of that identity.
pub(crate) const ISSUER: &str = "https://token.actions.githubusercontent.com";

/// This build's release.
pub(crate) fn current() -> Release {
    fleetringd::software::release()
}

/// The release `minors` lines after this build's, `.0`.
pub(crate) fn lines_ahead(minors: u64) -> Release {
    let (major, minor) = current().line();
    Release::new(major, minor + minors, 0)
}

/// The release one line before this build's, `.0`: within the fleet's window, and older.
pub(crate) fn line_behind() -> Release {
    let (major, minor) = current().line();
    Release::new(major, minor.saturating_sub(1), 0)
}

/// The last line before the rename, `.0`: it doesn't know `serve` (ADR-0043, point 1.1), whatever
/// this build's line is.
pub(crate) fn before_the_rename() -> Release {
    Release::new(0, 6, 0)
}

/// The release the binary at `bin` says it is, from `--version`: `fleetring <version>`, or
/// `fleetringd <version>` from a release before 0.7 or a binary named `fleetringd`.
pub(crate) fn release_of(bin: &Path) -> TestResult<Release> {
    let printed = crate::support::output_copied(Command::new(bin).arg("--version"))?;
    let text = String::from_utf8_lossy(&printed.stdout);
    let text = text.trim();
    Ok(text
        .strip_prefix("fleetring ")
        .or_else(|| text.strip_prefix("fleetringd "))
        .ok_or_else(|| format!("{} --version printed '{text}'", bin.display()))?
        .parse()?)
}

/// Runs `fleetring node <args>` against `fleet`'s control node as the test principal.
pub(crate) fn node_command(fleet: &ProcessFleet, args: &[&str]) -> TestResult<Output> {
    Ok(fleet
        .command(Path::new(env!("CARGO_BIN_EXE_fleetring")), "/usr/bin:/bin")
        .arg("node")
        .args(args)
        .args(["--control", &fleet.control])
        .output()?)
}

/// How long a test waits for a fleet to reach a state before it fails.
pub(crate) const PATIENCE: Duration = Duration::from_secs(120);

/// How often a wait looks again.
const POLL: Duration = Duration::from_millis(100);

/// The most times in a row the writer resends one write the primary refused before committing
/// anything, before the test fails: the bound on how long a leave and rejoin keeps a database
/// from taking writes. Each resend waits [`POLL`] first, so 30 is at least three seconds. Twelve
/// rolling runs under load, 4 at a time beside 4 CPU hogs, needed at most one resend in a row,
/// and the restart in place none; a regression that keeps writes out for seconds fails here
/// rather than pass as resends.
pub(crate) const MAX_REFUSALS_IN_A_ROW: u64 = 30;

/// Calls `ready` until it returns `Some`, giving up with `what` after about [`PATIENCE`] of
/// polls.
pub(crate) fn wait_for<T>(what: &str, mut ready: impl FnMut() -> Option<T>) -> TestResult<T> {
    let polls = PATIENCE.as_millis() / POLL.as_millis();
    for _ in 0..polls {
        if let Some(value) = ready() {
            return Ok(value);
        }
        std::thread::sleep(POLL);
    }
    Err(format!("gave up waiting until {what}").into())
}

/// The bytes of a SHA-256, in lowercase hex.
pub(crate) fn sha256(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// How a fake release's files are wrong, if they are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Defect {
    /// Nothing: the files verify.
    None,
    /// The bundle's signature covers other bytes than `SHA256SUMS`.
    Signature,
    /// The bundle was signed by an identity other than the release workflow's.
    Identity,
    /// `SHA256SUMS` lists another checksum for the archive, and is signed correctly.
    Checksum,
}

/// Which names a fake release's archive holds the binary under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArchiveLayout {
    /// From 0.7, as the release workflow packs it: `fleetring`, and a regular copy named
    /// `fleetringd` that a 0.6 upgrader extracts (ADR-0043, point 3.1).
    Both,
    /// Before 0.7: `fleetringd` alone.
    Before07,
}

/// Writes a release of `binary`, as `release`, for this build's target into `dir`: the archive,
/// laid out as `layout` says, `SHA256SUMS`, and a bundle the fake cosign accepts unless `defect`
/// says otherwise.
pub(crate) fn write_release(
    dir: &Path,
    release: &Release,
    binary: &[u8],
    defect: Defect,
    layout: ArchiveLayout,
) -> TestResult {
    fs::create_dir_all(dir)?;
    let name = format!("fleetringd-{release}-{TARGET}");
    let archive_name = format!("{name}.tar.gz");
    let mut archive = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::fast(),
    ));
    let names: &[&str] = match layout {
        ArchiveLayout::Both => &["fleetring", "fleetringd"],
        ArchiveLayout::Before07 => &["fleetringd"],
    };
    for file in names {
        let mut header = tar::Header::new_gnu();
        header.set_size(u64::try_from(binary.len())?);
        header.set_mode(0o755);
        header.set_cksum();
        archive.append_data(&mut header, format!("{name}/{file}"), binary)?;
    }
    let archive = archive.into_inner()?.finish()?;
    fs::write(dir.join(&archive_name), &archive)?;
    let listed = if defect == Defect::Checksum {
        sha256(b"something else")
    } else {
        sha256(&archive)
    };
    let sums = format!(
        "{listed}  {archive_name}\n{}  install.sh\n",
        sha256(b"#!/bin/sh\n")
    );
    fs::write(dir.join("SHA256SUMS"), &sums)?;
    let signed = if defect == Defect::Signature {
        sha256(b"other bytes")
    } else {
        sha256(sums.as_bytes())
    };
    let identity = if defect == Defect::Identity {
        "https://github.com/someone-else/fleetring/.github/workflows/release.yml@refs/tags/fleetringd-v9.9.9"
    } else {
        RELEASE_IDENTITY
    };
    fs::write(
        dir.join("SHA256SUMS.sigstore.json"),
        format!("{identity}\n{ISSUER}\n{signed}\n"),
    )?;
    Ok(())
}

/// A shell script that answers `--version` as `release` and nothing else, as a release before
/// 0.7 does: a binary that is recognisably not the one it replaced.
pub(crate) fn fake_binary(release: &Release) -> Vec<u8> {
    format!("#!/bin/sh\necho \"fleetringd {release}\"\n").into_bytes()
}

/// A stand-in for a next release that refuses a configuration this build accepts, as a release
/// that tightens a rule does: a script that runs this build for everything, except that its
/// `serve --check` refuses a heartbeat interval of 200 ms, which every [`ProcessFleet`] node's
/// configuration sets. It names the field as this build's check does, and exits 1.
pub(crate) fn refusing_release() -> Vec<u8> {
    let script = r#"#!/bin/sh
check=no
config=
previous=
for arg in "$@"; do
    case "$arg" in
    --check) check=yes ;;
    --config=*) config="${arg#--config=}" ;;
    esac
    if [ "$previous" = "--config" ]; then
        config="$arg"
    fi
    previous="$arg"
done
if [ "$check" = yes ] && grep -q '^heartbeat_interval_ms = 200$' "$config"; then
    echo "fleetring: $config: field membership.heartbeat_interval_ms: 200 ms is under this release's minimum, 500 ms" >&2
    exit 1
fi
exec 'THIS_BUILD' "$@"
"#;
    script
        .replace("THIS_BUILD", env!("CARGO_BIN_EXE_fleetring"))
        .into_bytes()
}

/// Writes a stand-in `cosign` into `dir`, reporting `version`. `verify-blob` accepts a bundle
/// only if its identity matches the `--certificate-identity-regexp` it is given, its issuer is
/// the `--certificate-oidc-issuer`, and it signs the SHA-256 of the blob, so the arguments
/// `fleetringd upgrade` passes are checked as cosign would check them. Every call is logged to
/// `dir/calls`.
pub(crate) fn fake_cosign(dir: &Path, version: &str) -> TestResult {
    use std::os::unix::fs::PermissionsExt as _;
    fs::create_dir_all(dir)?;
    let calls = dir.join("calls");
    let script = format!(
        r#"#!/bin/sh
echo "$*" >> '{calls}'
case "$1" in
version)
    printf '  ______\nGitVersion:    v{version}\nGitCommit:     0000000\n'
    exit 0
    ;;
verify-blob)
    blob="$2"
    shift 2
    bundle=""
    regexp=""
    issuer=""
    while [ "$#" -gt 0 ]; do
        case "$1" in
        --bundle) bundle="$2"; shift 2 ;;
        --certificate-identity-regexp) regexp="$2"; shift 2 ;;
        --certificate-oidc-issuer) issuer="$2"; shift 2 ;;
        *) echo "Error: unknown flag $1" >&2; exit 2 ;;
        esac
    done
    identity=$(sed -n 1p "$bundle")
    signer=$(sed -n 2p "$bundle")
    signed=$(sed -n 3p "$bundle")
    if ! printf '%s\n' "$identity" | grep -Eq -- "$regexp"; then
        echo "Error: none of the expected identities matched what was in the certificate, got subjects [$identity]" >&2
        exit 1
    fi
    if [ "$signer" != "$issuer" ]; then
        echo "Error: expected issuer $issuer, got $signer" >&2
        exit 1
    fi
    actual=$(shasum -a 256 "$blob" 2>/dev/null || sha256sum "$blob")
    if [ "${{actual%% *}}" != "$signed" ]; then
        echo "Error: invalid signature when validating ASN.1 encoded signature" >&2
        exit 1
    fi
    echo "Verified OK" >&2
    exit 0
    ;;
esac
echo "Error: unexpected command $1" >&2
exit 2
"#,
        calls = calls.display()
    );
    let path = dir.join("cosign");
    fs::write(&path, script)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755))?;
    Ok(())
}

/// `PATH` with `dir` first, and the system's directories after it.
pub(crate) fn path_with(dir: &Path) -> String {
    format!("{}:/usr/bin:/bin:/usr/sbin:/sbin", dir.display())
}

/// Serves the files under a directory over HTTP on a loopback port, and records each path asked
/// for. `/releases.json` is the releases list; anything else is a file under the directory.
pub(crate) struct ReleaseServer {
    address: SocketAddr,
    requested: Arc<Mutex<Vec<String>>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ReleaseServer {
    pub(crate) fn serve(root: &Path) -> TestResult<Self> {
        use http_body_util::Full;
        use hyper::body::Bytes;
        use hyper::server::conn::http1;
        use hyper::service::service_fn;
        use hyper::{Response, StatusCode};
        use hyper_util::rt::TokioIo;

        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let requested = Arc::new(Mutex::new(Vec::new()));
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let root = root.to_owned();
        let log = Arc::clone(&requested);
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime");
            runtime.block_on(async move {
                let listener =
                    tokio::net::TcpListener::from_std(listener).expect("a tokio listener");
                let accept = async {
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            continue;
                        };
                        let root = root.clone();
                        let log = Arc::clone(&log);
                        tokio::spawn(async move {
                            let service = service_fn(move |request: hyper::Request<_>| {
                                let path = request.uri().path().trim_start_matches('/').to_owned();
                                log.lock().expect("lock poisoned").push(path.clone());
                                let file = root.join(&path);
                                let response = match fs::read(&file) {
                                    Ok(bytes) if !path.contains("..") => {
                                        Response::new(Full::new(Bytes::from(bytes)))
                                    }
                                    _ => {
                                        let mut response = Response::new(Full::new(Bytes::new()));
                                        *response.status_mut() = StatusCode::NOT_FOUND;
                                        response
                                    }
                                };
                                async move { Ok::<_, std::convert::Infallible>(response) }
                            });
                            let _served = http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service)
                                .await;
                        });
                    }
                };
                tokio::select! {
                    () = accept => {}
                    _ = stopped => {}
                }
            });
        });
        Ok(Self {
            address,
            requested,
            stop: Some(stop),
            thread: Some(thread),
        })
    }

    /// The URL of `path` under the served directory.
    pub(crate) fn url(&self, path: &str) -> String {
        format!("http://{}/{path}", self.address)
    }

    /// Every path asked for so far.
    pub(crate) fn requested(&self) -> Vec<String> {
        self.requested.lock().expect("lock poisoned").clone()
    }
}

impl Drop for ReleaseServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Writes the releases list GitHub's API would return for `tags`, as `(tag, prerelease)`.
pub(crate) fn write_releases_list(path: &Path, tags: &[(&str, bool)]) -> TestResult {
    let entries: Vec<String> = tags
        .iter()
        .map(|(tag, prerelease)| {
            format!(
                r#"{{"tag_name":"{tag}","draft":false,"prerelease":{prerelease},"name":"{tag}"}}"#
            )
        })
        .collect();
    fs::write(path, format!("[{}]", entries.join(",")))?;
    Ok(())
}

/// A free loopback port, as near as a test can tell: one the system just handed out.
pub(crate) fn free_port() -> TestResult<u16> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

/// One daemon process of a [`ProcessFleet`].
pub(crate) struct NodeProcess {
    pub(crate) id: String,
    pub(crate) config: PathBuf,
    pub(crate) data_dir: PathBuf,
    pub(crate) bin: PathBuf,
    pub(crate) log: PathBuf,
    child: Option<Child>,
}

impl NodeProcess {
    /// The process ID, while the test's own child runs.
    pub(crate) fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }
}

/// How a fleet's nodes are installed and started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Launch {
    /// As 0.6 installs and starts a node, and a host upgraded from it keeps doing: the binary is
    /// `bin/fleetringd`, run as `fleetringd --config <file>`.
    Before07,
    /// As 0.7 does: `bin/fleetring`, run as `fleetring serve --config <file>`.
    Serve,
}

impl Launch {
    /// The binary's file name.
    fn file_name(self) -> &'static str {
        match self {
            Self::Before07 => "fleetringd",
            Self::Serve => "fleetring",
        }
    }
}

/// A fleet of real daemon processes: `node-1` runs both roles and the others data only, each from
/// its own copy of a binary, over TLS from one test authority.
pub(crate) struct ProcessFleet {
    pub(crate) tls: Tls,
    pub(crate) control: String,
    pub(crate) nodes: Vec<NodeProcess>,
    launch: Launch,
}

impl ProcessFleet {
    /// Starts `count` nodes, each running a copy of `binary` installed and started as 0.6 does
    /// ([`Launch::Before07`]), and waits until every one is a live member.
    pub(crate) fn start(root: &Path, binary: &Path, count: usize) -> TestResult<Self> {
        Self::start_with(root, binary, count, "both")
    }

    /// Starts a fleet as [`ProcessFleet::start`] does, with each node installed and started as
    /// 0.7 does ([`Launch::Serve`]).
    pub(crate) fn start_serving(root: &Path, binary: &Path, count: usize) -> TestResult<Self> {
        Self::start_as(root, binary, count, "both", true, Launch::Serve)
    }

    /// Starts a fleet of the release `binary` is, as [`ProcessFleet::start`] does. A release
    /// before 0.4.0 has no admin listener and refuses an `[admin]` table, so its nodes get none
    /// until [`ProcessFleet::enable_admin`] moves them to this build.
    pub(crate) fn start_release(root: &Path, binary: &Path, count: usize) -> TestResult<Self> {
        let release = release_of(binary)?;
        let admin = release >= Release::new(0, 4, 0);
        Self::start_as(root, binary, count, "both", admin, Launch::Before07)
    }

    /// Starts a fleet as [`ProcessFleet::start`] does, with `node-1` in `first_role`: `both`, or
    /// `control` for a control node that is no member.
    pub(crate) fn start_with(
        root: &Path,
        binary: &Path,
        count: usize,
        first_role: &str,
    ) -> TestResult<Self> {
        Self::start_as(root, binary, count, first_role, true, Launch::Before07)
    }

    /// Starts a fleet as [`ProcessFleet::start_with`] does, giving each node an admin listener
    /// on a port of its own if `admin`, installed and started as `launch` says.
    fn start_as(
        root: &Path,
        binary: &Path,
        count: usize,
        first_role: &str,
        admin: bool,
        launch: Launch,
    ) -> TestResult<Self> {
        let tls = Tls::create(root)?;
        let control = format!("127.0.0.1:{}", free_port()?);
        let mut nodes = Vec::with_capacity(count);
        for index in 1..=count {
            let id = format!("node-{index}");
            let dir = root.join(&id);
            let bin = dir.join("bin").join(launch.file_name());
            fs::create_dir_all(bin.parent().ok_or("no parent")?)?;
            fs::copy(binary, &bin)?;
            let data_dir = dir.join("data");
            let listen = if index == 1 {
                control.clone()
            } else {
                format!("127.0.0.1:{}", free_port()?)
            };
            let role = if index == 1 {
                format!("role = \"{first_role}\"\n")
            } else {
                format!("role = \"data\"\n\n[control]\naddress = \"{control}\"\n")
            };
            let text = format!(
                "data_dir = {data_dir:?}\nlisten = \"{listen}\"\n{role}\n[node]\nid = \"{id}\"\n\n\
                 [membership]\nheartbeat_interval_ms = 200\n{}",
                if admin {
                    tls.node_table(&id)?
                } else {
                    tls.node_tls_table(&id)?
                }
            );
            let config = dir.join("node.toml");
            fs::write(&config, text)?;
            nodes.push(NodeProcess {
                id,
                config,
                data_dir,
                bin,
                log: dir.join("node.log"),
                child: None,
            });
        }
        let mut fleet = Self {
            tls,
            control,
            nodes,
            launch,
        };
        for index in 0..count {
            fleet.spawn(index)?;
        }
        Ok(fleet)
    }

    /// Gives node `index` an admin listener on a port of its own, as its configuration may say
    /// once it runs this build, unless it has one: without it, every node on this machine would
    /// take the default port.
    pub(crate) fn enable_admin(&self, index: usize) -> TestResult {
        let config = &self.nodes[index].config;
        let text = fs::read_to_string(config)?;
        if !text.contains("[admin]") {
            fs::write(config, format!("{text}{}", crate::support::ADMIN))?;
        }
        Ok(())
    }

    /// Starts node `index` from its binary, appending its log to its log file.
    pub(crate) fn spawn(&mut self, index: usize) -> TestResult {
        let node = &mut self.nodes[index];
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&node.log)?;
        let mut command = Command::new(&node.bin);
        crate::support::isolate(&mut command);
        if self.launch == Launch::Serve {
            command.arg("serve");
        }
        // The node's binary was copied as the fleet started.
        let child = crate::support::spawn_copied(
            command
                .arg("--config")
                .arg(&node.config)
                .env("RUST_LOG", "info")
                .stdout(Stdio::null())
                .stderr(log),
        )?;
        node.child = Some(child);
        Ok(())
    }

    /// Stops node `index` with SIGTERM, as a supervisor does, and waits for it to exit.
    pub(crate) fn stop(&mut self, index: usize) -> TestResult {
        let node = &mut self.nodes[index];
        let Some(mut child) = node.child.take() else {
            return Ok(());
        };
        let killed = Command::new("kill")
            .arg("-TERM")
            .arg(child.id().to_string())
            .status()?;
        assert!(killed.success(), "kill -TERM {}", child.id());
        let status = child.wait()?;
        assert!(status.success(), "{} exited with {status}", node.id);
        Ok(())
    }

    /// Runs `bin` with `args` as the test principal, with `path` as `PATH`.
    pub(crate) fn command(&self, bin: &Path, path: &str) -> Command {
        let mut command = Command::new(bin);
        crate::support::isolate(&mut command);
        command
            .env("FLEETRING_TLS_DIR", self.tls.dir())
            .env("PATH", path)
            .env_remove("FLEETRING_BASE_URL")
            .env_remove("FLEETRING_RELEASES_URL");
        command
    }

    /// A client of the fleet, as the test principal.
    pub(crate) fn client(&self) -> TestResult<Client> {
        Ok(Client::new(
            self.control.clone(),
            self.tls.client_network("tester")?,
        ))
    }

    /// Every node's log, for a failure message.
    pub(crate) fn logs(&self) -> String {
        let mut text = String::new();
        for node in &self.nodes {
            let _ = writeln!(
                text,
                "---- {} ----\n{}",
                node.id,
                fs::read_to_string(&node.log).unwrap_or_default()
            );
        }
        text
    }
}

impl Drop for ProcessFleet {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(mut child) = node.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

/// Waits until every node in `ids` is a live member on `release`, as the control plane sees it.
pub(crate) async fn wait_for_members(
    client: &Client,
    ids: &[&str],
    release: Option<&Release>,
) -> TestResult {
    let ids: BTreeSet<NodeId> = ids.iter().map(|id| id.parse()).collect::<Result<_, _>>()?;
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        if let Ok(view) = client.status().await {
            let ready: BTreeSet<NodeId> = view
                .members()
                .filter(|(member, liveness)| {
                    *liveness == Liveness::Live
                        && release.is_none_or(|release| member.running().release() == release)
                })
                .map(|(member, _)| member.id().clone())
                .collect();
            if ids.is_subset(&ready) {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() > deadline {
            return Err(format!("members {ids:?} never became live on {release:?}").into());
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Waits until every node in `ids` has reported its software as `release` to the control plane:
/// stricter than [`wait_for_members`], since a member that reported nothing runs 0.3.0 too.
pub(crate) async fn wait_for_reported(
    client: &Client,
    ids: &[&str],
    release: &Release,
) -> TestResult {
    for _ in 0..PATIENCE.as_millis() / POLL.as_millis() {
        if let Ok(view) = client.status().await {
            let reported = ids.iter().all(|id| {
                view.members().any(|(member, _)| {
                    member.id().as_str() == *id
                        && member
                            .software()
                            .is_some_and(|software| software.release() == release)
                })
            });
            if reported {
                return Ok(());
            }
        }
        tokio::time::sleep(POLL).await;
    }
    Err(format!("members {ids:?} never reported {release}").into())
}

/// What the writer saw: each write it attempted, whether it was acknowledged, and how many writes
/// needed their one retry.
#[derive(Debug, Default)]
pub(crate) struct History {
    pub(crate) attempted: BTreeSet<i64>,
    pub(crate) acknowledged: BTreeSet<i64>,
    /// Writes that failed even after their retry, with the last error.
    pub(crate) failed: Vec<(i64, String)>,
    pub(crate) retried: u64,
    /// Reads that returned less than every write acknowledged before they began.
    pub(crate) stale_reads: Vec<String>,
    pub(crate) reads: u64,
    /// Sends of a write the primary refused before committing anything, which a client resends.
    pub(crate) refused: u64,
    /// The most resends one write needed before the primary took it.
    pub(crate) longest_refusal: u64,
}

/// Creates the three-replica database `history` with its table, retrying until the fleet has
/// placed it and given it a primary.
pub(crate) async fn create_history_database(client: &Client) -> TestResult<DatabaseName> {
    let name: DatabaseName = "history".parse()?;
    let factor = ReplicationFactor::new(3).ok_or("zero")?;
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        let created = match client.create_database(&name, factor).await {
            Ok(_) => true,
            Err(status) => status.code() == tonic::Code::AlreadyExists,
        };
        if created
            && client
                .execute(
                    &name,
                    Statement::new("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)"),
                )
                .await
                .is_ok()
        {
            return Ok(name);
        }
        if tokio::time::Instant::now() > deadline {
            return Err("the database never got a primary".into());
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Writes one row per step, with increasing IDs, until `stop` is set, and reads the latest ID
/// every few writes. A write that fails is retried once, after the database has a primary again,
/// as FR-038's drain allows a client to; it is idempotent, so a write that did land the first
/// time isn't applied twice.
pub(crate) async fn write_until(
    client: Client,
    name: DatabaseName,
    stop: Arc<AtomicBool>,
) -> History {
    let mut history = History::default();
    let mut id: i64 = 0;
    // The newest epoch a write was acknowledged at.
    let mut epoch = LeaseEpoch::ZERO;
    while !stop.load(Ordering::SeqCst) {
        id += 1;
        history.attempted.insert(id);
        match send(&client, &name, id, &mut history).await {
            Ok(written) => {
                epoch = epoch.max(written.epoch);
                history.acknowledged.insert(id);
            }
            Err(status) => {
                history.retried += 1;
                // A drain refuses writes until its hand-over completes at the next epoch; any
                // other failure, such as the control node restarting, only needs a primary.
                let after = status
                    .message()
                    .contains("moving to a new primary")
                    .then_some(epoch);
                wait_for_primary(&client, &name, after).await;
                match send(&client, &name, id, &mut history).await {
                    Ok(written) => {
                        epoch = epoch.max(written.epoch);
                        history.acknowledged.insert(id);
                    }
                    Err(status) => history.failed.push((id, status.to_string())),
                }
            }
        }
        if id % 10 == 0 {
            let floor = history.acknowledged.last().copied().unwrap_or(0);
            if let Ok(rows) = client
                .query(&name, Statement::new("SELECT max(id) FROM t"))
                .await
            {
                history.reads += 1;
                let seen = rows
                    .rows
                    .first()
                    .and_then(|row| row.first())
                    .and_then(|value| match value {
                        Value::Integer(seen) => Some(*seen),
                        _ => None,
                    })
                    .unwrap_or(0);
                if seen < floor {
                    history
                        .stale_reads
                        .push(format!("read max id {seen} after {floor} was acknowledged"));
                }
            }
        }
        // A steady trickle, so the run is long on writes and short on CPU.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    history
}

/// Writes row `id`, sending it again while the primary refuses it before committing anything,
/// as when a membership change leaves it briefly short of reachable replicas: such a refusal is
/// safe to resend, and doesn't count against a write's one retry. Each is counted in
/// [`History::refused`].
async fn send(
    client: &Client,
    name: &DatabaseName,
    id: i64,
    history: &mut History,
) -> Result<fleetringd::data::Written, tonic::Status> {
    let write = || {
        client.execute(
            name,
            Statement::with_params(
                "INSERT OR IGNORE INTO t (id) VALUES (?)",
                vec![Value::Integer(id)],
            ),
        )
    };
    let polls = PATIENCE.as_millis() / POLL.as_millis();
    let mut outcome = write().await;
    let mut refused: u64 = 0;
    while let Err(status) = &outcome
        && status.message().ends_with("nothing committed")
        && u128::from(refused) < polls
    {
        refused += 1;
        history.refused += 1;
        tokio::time::sleep(POLL).await;
        outcome = write().await;
    }
    history.longest_refusal = history.longest_refusal.max(refused);
    outcome
}

/// Waits, at most [`PATIENCE`], until the database has a primary, at an epoch later than `after`
/// if given.
async fn wait_for_primary(client: &Client, name: &DatabaseName, after: Option<LeaseEpoch>) {
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while tokio::time::Instant::now() < deadline {
        if let Ok(location) = client.locate(name).await
            && location.primary.is_some()
            && after.is_none_or(|epoch| location.epoch > epoch)
        {
            return;
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Checks the history against the rows the database holds at the end: no write failed beyond
/// its one retry, no read was stale, every acknowledged write is there, and nothing is there
/// that wasn't written.
pub(crate) async fn check_history(
    client: &Client,
    name: &DatabaseName,
    history: &History,
) -> TestResult {
    assert!(
        history.failed.is_empty(),
        "writes failed after their retry: {:?} ({} clean refusals resent, at most {} in a row)",
        history.failed,
        history.refused,
        history.longest_refusal
    );
    assert!(
        history.longest_refusal <= MAX_REFUSALS_IN_A_ROW,
        "a write was refused {} times in a row before the primary took it, more than \
         {MAX_REFUSALS_IN_A_ROW} ({} clean refusals resent in all)",
        history.longest_refusal,
        history.refused
    );
    assert!(history.stale_reads.is_empty(), "{:?}", history.stale_reads);
    let rows = client
        .query(name, Statement::new("SELECT id FROM t"))
        .await?;
    let stored: BTreeSet<i64> = rows
        .rows
        .iter()
        .filter_map(|row| match row.first() {
            Some(Value::Integer(id)) => Some(*id),
            _ => None,
        })
        .collect();
    let lost: Vec<_> = history.acknowledged.difference(&stored).collect();
    assert!(lost.is_empty(), "acknowledged writes lost: {lost:?}");
    let phantom: Vec<_> = stored.difference(&history.attempted).collect();
    assert!(phantom.is_empty(), "rows nobody wrote: {phantom:?}");
    Ok(())
}

/// The output's stdout and stderr, for a failure message.
pub(crate) fn printed(output: &Output) -> String {
    format!(
        "status {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Writes `text` to stderr, so a long test shows where it is.
pub(crate) fn progress(text: &str) {
    let _ = writeln!(std::io::stderr(), "upgrade test: {text}");
}
