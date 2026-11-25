//! `fleetring-chaos`: fault injection on real daemons (ADR-0036).

use std::io::Read as _;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use fleetring_chaos::docker::Docker;
use fleetring_chaos::duration::parse_duration;
use fleetring_chaos::node::DEFAULT_IMAGE;
use fleetring_chaos::run::{RunOptions, remove_fleet, run};
use fleetring_chaos::schedule::{Scenario, Schedule};
use fleetring_chaos::{Build, Mode, Teardown};

/// Runs five real fleetringd processes in Docker, injects faults, and judges the history
#[derive(Debug, Parser)]
#[command(name = "fleetring-chaos", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Runs a scenario against five containers and checks its history (Linux with Docker)
    ///
    /// Exits 0 when every check passes, 1 when one finds a violation, and 2 when the harness
    /// itself failed.
    Run {
        /// The fault kinds to inject
        #[arg(long, value_enum)]
        scenario: Scenario,
        /// The seed for the fault schedule and the workload; random if unset, and printed first
        #[arg(long)]
        seed: Option<u64>,
        /// How long the fault phase runs: `<n>s`, `<n>m`, or `<n>h`
        #[arg(long, value_parser = parse_duration, default_value = "600s")]
        time_limit: Duration,
        /// Creates `acks = local` databases but checks them as `quorum`: the run must fail
        #[arg(long)]
        self_test: bool,
        /// The node image
        #[arg(long, default_value = DEFAULT_IMAGE)]
        image: String,
        /// Uses `--image` as it is instead of building it from the workspace
        #[arg(long)]
        skip_build: bool,
        /// Where run directories go
        #[arg(long, default_value = "target/chaos")]
        out: PathBuf,
        /// Leaves the containers and the network after the run
        #[arg(long)]
        keep: bool,
    },
    /// Prints the fault schedule a seed gives, without running it
    Schedule {
        /// The fault kinds to draw from
        #[arg(long, value_enum)]
        scenario: Scenario,
        /// The seed
        #[arg(long)]
        seed: u64,
        /// How long the fault phase runs: `<n>s`, `<n>m`, or `<n>h`
        #[arg(long, value_parser = parse_duration, default_value = "600s")]
        time_limit: Duration,
        /// The self-test's schedule: primary isolations only
        #[arg(long)]
        self_test: bool,
    },
    /// Removes containers and the network a run left behind
    Clean,
}

/// Maps the presence flag `--self-test` to a [`Mode`].
fn mode(self_test: bool) -> Mode {
    if self_test {
        Mode::SelfTest
    } else {
        Mode::Normal
    }
}

/// A seed from the kernel: 8 bytes of `/dev/urandom`, little-endian.
fn random_seed() -> std::io::Result<u64> {
    let mut bytes = [0; 8];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

/// Removes the five containers and the network, whichever exist.
fn clean() {
    remove_fleet(&Docker);
    println!("removed the fleetring-chaos containers and network, if there were any");
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Run {
            scenario,
            seed,
            time_limit,
            self_test,
            image,
            skip_build,
            out,
            keep,
        } => {
            if std::env::consts::OS != "linux" {
                eprintln!(
                    "fleetring-chaos: fleetring-chaos run needs Linux with Docker; this host runs {}",
                    std::env::consts::OS
                );
                return ExitCode::from(2);
            }
            let seed = match seed.map_or_else(random_seed, Ok) {
                Ok(seed) => seed,
                Err(error) => {
                    eprintln!("fleetring-chaos: can't read a seed from /dev/urandom: {error}");
                    return ExitCode::from(2);
                }
            };
            eprintln!("fleetring-chaos: {scenario} seed {seed}");
            let options = RunOptions {
                scenario,
                seed,
                time_limit,
                mode: mode(self_test),
                image,
                build: if skip_build {
                    Build::Skip
                } else {
                    Build::Image
                },
                out,
                teardown: if keep {
                    Teardown::Keep
                } else {
                    Teardown::Remove
                },
            };
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!("fleetring-chaos: can't start the async runtime: {error}");
                    return ExitCode::from(2);
                }
            };
            ExitCode::from(runtime.block_on(run(options)))
        }
        Command::Schedule {
            scenario,
            seed,
            time_limit,
            self_test,
        } => {
            let schedule = Schedule::generate(scenario, seed, time_limit, mode(self_test));
            for planned in &schedule.faults {
                println!("{planned}");
            }
            ExitCode::SUCCESS
        }
        Command::Clean => {
            clean();
            ExitCode::SUCCESS
        }
    }
}
