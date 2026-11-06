//! `fleetring dev`'s flags, its background mode, and its subcommands: `dev -d`, `dev stop`, `dev
//! status` and `dev logs` (ADR-0043, point 5).

use std::error::Error;
use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, Seek as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Subcommand};
use fleetringd::dev::{self, DevContextLine, DevError, DevStatus, Prepared};

use crate::exit::INTERRUPTED;
use crate::format::{self, OutputArgs, OutputFormat};
use crate::progress::{MachineOutput, Progress};
use crate::stop::{self, StopTarget};
use crate::{context_cmd, parse_ttl, program, spinner_surroundings};

/// How long `dev -d` waits for the node to be ready unless `--timeout` says otherwise.
pub(crate) const DETACH_TIMEOUT: Duration = Duration::from_secs(60);

/// How often `dev logs -f` looks for new lines.
const FOLLOW_POLL: Duration = Duration::from_millis(200);

/// `fleetring dev`'s flags (ADR-0039, ADR-0043 point 5).
#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub(crate) struct DevArgs {
    #[command(subcommand)]
    pub(crate) command: Option<DevCommand>,
    /// The directory that holds the authority, the certificates, the configuration, and the
    /// databases; `~/.fleetring` by default.
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    /// The loopback port the node serves on, 7400 by default; the admin listener takes the next
    /// one. Only the first run chooses it: later runs read it from `<dir>/fleetringd.toml`, and
    /// refuse a different one.
    #[arg(long)]
    pub(crate) port: Option<u16>,
    /// Runs the dev node in the background and returns once it is ready. Its output goes to
    /// `<dir>/fleetring.log`.
    #[arg(short = 'd', long)]
    pub(crate) detach: bool,
    /// How long `-d` waits for the node to be ready: `<n>m` or `<n>s`; 60s by default.
    #[arg(long, value_parser = parse_ttl, requires = "detach")]
    pub(crate) timeout: Option<Duration>,
}

/// `fleetring dev`'s subcommands, for the node `dev -d` started.
#[derive(Debug, Subcommand)]
pub(crate) enum DevCommand {
    /// Stops the dev node: drains it, and waits until it has exited.
    Stop {
        /// The dev directory; `~/.fleetring` by default.
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// How long to wait for the node to drain and exit: `<n>m` or `<n>s`; 2m by default.
        #[arg(long, value_parser = parse_ttl)]
        timeout: Option<Duration>,
    },
    /// Says whether the dev node runs, with its PID, address, release, log and context. Exits 0
    /// either way.
    Status {
        /// The dev directory; `~/.fleetring` by default.
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Prints the log `dev -d` writes. It rotates only when the node starts, so during one long
    /// run it grows without bound; `fleetring dev stop && fleetring dev -d` rotates it.
    Logs {
        /// The dev directory; `~/.fleetring` by default.
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Keeps printing new lines as the node writes them, until Ctrl-C.
        #[arg(short = 'f', long)]
        follow: bool,
    },
}

/// Whether `dev` runs the node in this process or in the background.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Detach {
    /// In this process, until Ctrl-C.
    Foreground,
    /// In the background, `dev -d`.
    Background,
}

/// Whether `dev logs` stops at the end of the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Follow {
    /// It prints the log and exits.
    Once,
    /// It keeps printing new lines until Ctrl-C.
    Follow,
}

impl DevArgs {
    /// Whether these flags run a node in this process: plain `dev`, without `-d` or a
    /// subcommand.
    pub(crate) fn runs_in_foreground(&self) -> bool {
        self.command.is_none() && self.detach() == Detach::Foreground
    }

    fn detach(&self) -> Detach {
        if self.detach {
            Detach::Background
        } else {
            Detach::Foreground
        }
    }
}

/// Runs `dev -d`, `dev stop`, `dev status` or `dev logs`.
pub(crate) async fn run(args: DevArgs) -> Result<ExitCode, Box<dyn Error>> {
    match args.command {
        Some(DevCommand::Stop { data_dir, timeout }) => stop_dev(data_dir, timeout).await,
        Some(DevCommand::Status { data_dir, output }) => status(data_dir, output.format()),
        Some(DevCommand::Logs { data_dir, follow }) => {
            let follow = if follow { Follow::Follow } else { Follow::Once };
            logs(&dir(data_dir)?, follow).await
        }
        None => detach(&args).await,
    }
}

/// The dev directory: `data_dir`, else `~/.fleetring`.
fn dir(data_dir: Option<PathBuf>) -> Result<PathBuf, DevError> {
    data_dir.map_or_else(dev::default_dir, Ok)
}

/// `dev -d`: prepares the directory and writes the context in the foreground, so every refusal
/// is immediate, then starts the node in the background and waits until it is ready.
async fn detach(args: &DevArgs) -> Result<ExitCode, Box<dyn Error>> {
    debug_assert_eq!(args.detach(), Detach::Background, "plain dev runs in main");
    if cfg!(not(unix)) {
        return Err(DevError::Unsupported.into());
    }
    let prepared = crate::prepare_dev(args)?;
    let context = context_cmd::write_dev_context(&prepared);
    let exe = std::env::current_exe()?;
    let arg0 = std::env::args_os()
        .next()
        .unwrap_or_else(|| exe.clone().into_os_string());
    let progress = Progress::start(
        "starting the dev node\u{2026}",
        &spinner_surroundings(MachineOutput::No),
    );
    let detached = dev::detach(
        &prepared,
        &exe,
        &arg0,
        args.port,
        args.timeout.unwrap_or(DETACH_TIMEOUT),
    )
    .await?;
    let headline = match detached {
        dev::Detached::Started { pid } => {
            progress.finish_ok("dev node ready");
            format!("dev node ready (pid {pid})")
        }
        dev::Detached::AlreadyRunning { pid } => {
            progress.finish_ok("dev node already running");
            format!("dev node already running (pid {pid})")
        }
    };
    print!("{}", ready_text(&headline, &prepared, &context));
    Ok(ExitCode::SUCCESS)
}

/// What `dev -d` prints on stdout once the node is ready.
fn ready_text(headline: &str, prepared: &Prepared, context: &DevContextLine) -> String {
    let mut text = format!("{headline}\n");
    let _ = writeln!(text, "  address    {}", prepared.config().listen);
    let _ = writeln!(text, "  data dir   {}", prepared.dir().display());
    let _ = writeln!(
        text,
        "  log        {}",
        prepared.dir().join(dev::LOG_FILE).display()
    );
    let _ = writeln!(text, "  context    {}", context.describe());
    if matches!(context, DevContextLine::NotWritten { .. }) {
        let _ = writeln!(text, "  env file   {}", prepared.env_file().display());
    }
    text.push_str("Stop it with `fleetring dev stop`.\n");
    text
}

/// `dev stop`: `stop` for the dev directory's node.
async fn stop_dev(
    data_dir: Option<PathBuf>,
    timeout: Option<Duration>,
) -> Result<ExitCode, Box<dyn Error>> {
    let data_dir = stop::data_dir_of(&StopTarget::DataDir(dir(data_dir)?))?;
    let progress = Progress::start(
        "stopping the dev node\u{2026}",
        &spinner_surroundings(MachineOutput::No),
    );
    let stopped = stop::stop(&data_dir, timeout.unwrap_or(stop::STOP_TIMEOUT), &progress).await?;
    progress.finish_ok("dev node stopped");
    println!("stopped the dev node (pid {})", stopped.pid);
    Ok(ExitCode::SUCCESS)
}

/// `dev status`: a read command, so it exits 0 whether or not the node runs.
fn status(data_dir: Option<PathBuf>, format: OutputFormat) -> Result<ExitCode, Box<dyn Error>> {
    let dir = dir(data_dir)?;
    let status = dev::status(&dir)?;
    crate::emit(&format::render(format, &status, |status| {
        Ok(status_text(status, &context_cmd::dev_context_status(&dir)))
    })?)?;
    Ok(ExitCode::SUCCESS)
}

/// `dev status`'s text: `running` with what the node says about itself, or `not running`.
fn status_text(status: &DevStatus, context: &str) -> String {
    let mut text = match status.pid {
        Some(pid) if status.running => format!("running (pid {pid})\n"),
        _ => "not running\n".to_owned(),
    };
    if let Some(address) = status.address {
        let _ = writeln!(text, "  address    {address}");
    }
    if let Some(release) = &status.release {
        let _ = writeln!(text, "  release    {release}");
    }
    let _ = writeln!(text, "  data dir   {}", status.data_dir.display());
    if let Some(log) = &status.log {
        let _ = writeln!(text, "  log        {}", log.display());
    }
    let _ = writeln!(text, "  context    {context}");
    text
}

/// `dev logs [-f]`: prints the log, and with `-f` keeps printing what is appended, starting over
/// if the file shrinks (a rotation), until Ctrl-C.
async fn logs(dir: &Path, follow: Follow) -> Result<ExitCode, Box<dyn Error>> {
    let path = dir.join(dev::LOG_FILE);
    let mut position = match File::open(&path) {
        Ok(mut file) => copy_out(&mut file)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            eprintln!(
                "{}: no log at {}: only `fleetring dev -d` writes one",
                program(),
                path.display()
            );
            return Ok(ExitCode::FAILURE);
        }
        Err(error) => return Err(error.into()),
    };
    if follow == Follow::Once {
        return Ok(ExitCode::SUCCESS);
    }
    let interrupted = tokio::signal::ctrl_c();
    tokio::pin!(interrupted);
    loop {
        tokio::select! {
            _ = &mut interrupted => return Ok(ExitCode::from(INTERRUPTED)),
            () = tokio::time::sleep(FOLLOW_POLL) => {}
        }
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if file.metadata()?.len() < position {
            position = 0;
        }
        file.seek(io::SeekFrom::Start(position))?;
        position += copy_out(&mut file)?;
    }
}

/// Copies the rest of `file` to stdout and returns how many bytes it copied. A reader that has
/// gone away, such as `head`, isn't an error.
fn copy_out(file: &mut File) -> io::Result<u64> {
    let mut stdout = io::stdout().lock();
    match io::copy(file, &mut stdout).and_then(|copied| stdout.flush().map(|()| copied)) {
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(0),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;

    use super::*;
    use crate::{Cli, Command};

    #[test]
    fn dev_status_json_pins_its_fields() -> Result<(), Box<dyn std::error::Error>> {
        let status = fleetringd::dev::status(&tempfile::tempdir()?.path().join("dev"))?;
        let json = format::render(OutputFormat::Jsonl, &status, |_| Ok(String::new()))?;
        let value: serde_json::Value = serde_json::from_str(&json)?;
        let keys: Vec<&str> = value
            .as_object()
            .ok_or("an object")?
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            ["address", "data_dir", "log", "pid", "release", "running"]
        );
        assert_eq!(value["running"], false);
        Ok(())
    }

    fn dev_args(words: &[&str]) -> Result<DevArgs, clap::Error> {
        let cli = Cli::try_parse_from([&["fleetring", "dev"][..], words].concat())?;
        let Command::Dev(args) = cli.command else {
            panic!("not dev: {:?}", cli.command);
        };
        Ok(args)
    }

    #[test]
    fn dev_runs_in_the_foreground_unless_detached_or_given_a_subcommand() {
        assert!(dev_args(&[]).expect("dev").runs_in_foreground());
        let detached = dev_args(&["-d", "--timeout", "30s"]).expect("dev -d");
        assert!(!detached.runs_in_foreground());
        assert_eq!(detached.timeout, Some(Duration::from_secs(30)));
        assert!(!dev_args(&["stop"]).expect("dev stop").runs_in_foreground());
        let logs = dev_args(&["logs", "-f", "--data-dir", "d"]).expect("dev logs");
        assert!(matches!(
            logs.command,
            Some(DevCommand::Logs { follow: true, .. })
        ));
        // `--timeout` belongs to `-d`, and the node's flags to no subcommand.
        assert!(dev_args(&["--timeout", "30s"]).is_err());
        assert!(dev_args(&["--port", "7410", "stop"]).is_err());
    }

    #[test]
    fn the_status_says_running_with_what_the_node_says_or_not_running() {
        let running = DevStatus {
            running: true,
            pid: Some(4242),
            address: Some("127.0.0.1:7400".parse().expect("an address")),
            release: Some("0.7.0".to_owned()),
            data_dir: PathBuf::from("/home/me/.fleetring"),
            log: Some(PathBuf::from("/home/me/.fleetring/fleetring.log")),
        };
        assert_eq!(
            status_text(&running, "dev (current)"),
            "running (pid 4242)\n  address    127.0.0.1:7400\n  release    0.7.0\n  \
             data dir   /home/me/.fleetring\n  log        /home/me/.fleetring/fleetring.log\n  \
             context    dev (current)\n"
        );
        let stopped = DevStatus {
            running: false,
            pid: None,
            address: None,
            release: None,
            log: None,
            ..running
        };
        assert!(status_text(&stopped, "dev (current)").starts_with("not running\n"));
    }
}
