//! `fleetring-chaos`: fault injection on real daemons (ADR-0036).

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use fleetring_chaos::docker::{Docker, network_rm_args, rm_args};
use fleetring_chaos::duration::parse_duration;
use fleetring_chaos::node::{DEFAULT_IMAGE, Node};
use fleetring_chaos::schedule::Scenario;
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

/// Removes the five containers and the network, whichever exist.
fn clean() {
    let docker = Docker;
    let containers: Vec<String> = Node::ALL.into_iter().map(Node::container).collect();
    docker.run_ignoring_errors(&rm_args(&containers));
    docker.run_ignoring_errors(&network_rm_args());
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
            let _ = (scenario, seed, time_limit, mode(self_test), image, out);
            let _ = if skip_build {
                Build::Skip
            } else {
                Build::Image
            };
            let _ = if keep {
                Teardown::Keep
            } else {
                Teardown::Remove
            };
            todo!("FR-065 step 8")
        }
        Command::Schedule {
            scenario,
            seed,
            time_limit,
            self_test,
        } => {
            let _ = (scenario, seed, time_limit, mode(self_test));
            todo!("FR-065 step 5")
        }
        Command::Clean => {
            clean();
            ExitCode::SUCCESS
        }
    }
}
