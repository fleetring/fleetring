//! The `fleetringd` binary.

use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use fleetring::NodeId;
use fleetringd::{Config, Daemon, Role};
use tracing_subscriber::EnvFilter;

/// Places, replicates, and fails over SQLite databases across machines you own.
#[derive(Debug, Parser)]
#[command(version)]
struct Cli {
    /// The TOML configuration file.
    #[arg(long)]
    config: PathBuf,
    /// Overrides `node.id`.
    #[arg(long)]
    node_id: Option<NodeId>,
    /// Overrides `data_dir`.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Overrides `listen`.
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// Overrides `control.address`.
    #[arg(long)]
    control: Option<SocketAddr>,
    /// Overrides `role`: `both`, `data`, or `control`.
    #[arg(long, value_parser = parse_role)]
    role: Option<Role>,
}

impl Cli {
    fn apply(self, mut config: Config) -> Config {
        if let Some(id) = self.node_id {
            config.node.id = id;
        }
        if let Some(data_dir) = self.data_dir {
            config.data_dir = data_dir;
        }
        if let Some(listen) = self.listen {
            config.listen = listen;
        }
        if self.control.is_some() {
            config.control.address = self.control;
        }
        if let Some(role) = self.role {
            config.role = role;
        }
        config
    }
}

fn parse_role(text: &str) -> Result<Role, String> {
    match text {
        "both" => Ok(Role::Both),
        "data" => Ok(Role::Data),
        "control" => Ok(Role::Control),
        other => Err(format!(
            "unknown role '{other}': expected both, data, or control"
        )),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let config = match Config::load(&cli.config) {
        Ok(config) => cli.apply(config),
        Err(error) => {
            report(&error);
            return ExitCode::FAILURE;
        }
    };
    let daemon = match Daemon::start(config).await {
        Ok(daemon) => daemon,
        Err(error) => {
            report(&error);
            return ExitCode::FAILURE;
        }
    };
    daemon.run_until(shutdown_signal()).await;
    ExitCode::SUCCESS
}

/// Prints an error and its causes, one per line.
fn report(error: &dyn Error) {
    eprintln!("fleetringd: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        eprintln!("  caused by: {cause}");
        source = cause.source();
    }
}

/// Completes on Ctrl-C or, on Unix, SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "can't listen for Ctrl-C");
        }
    };
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    () = ctrl_c => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::error!(%error, "can't listen for SIGTERM");
                ctrl_c.await;
            }
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}
