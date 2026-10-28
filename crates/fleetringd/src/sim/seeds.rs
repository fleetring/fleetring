//! Seeds: a fixed list for every CI build, an override to rerun one, and the rerun command.
//!
//! A simulation test runs its scenario once per seed with [`run`]. When a seed fails, [`run`]
//! prints the one command that reruns that seed alone, then fails the test:
//!
//! ```text
//! FLEETRING_SIM_SEED=1234 cargo test -p fleetringd --features simulation --test sim -- --exact membership::rings_converge
//! ```
//!
//! Setting [`SEED_VAR`] runs only that seed. The scheduled sweep sets it to random seeds.

use std::fmt;
use std::time::Duration;

/// The environment variable that picks the one seed to run.
pub const SEED_VAR: &str = "FLEETRING_SIM_SEED";

/// The seeds every CI build runs, unless [`SEED_VAR`] picks one.
pub const FIXED: [u64; 4] = [1, 24, 29, 2_026];

/// Returns the seeds to run: the one in [`SEED_VAR`] if it is set, otherwise `fixed`.
///
/// # Panics
///
/// If [`SEED_VAR`] is set but isn't an unsigned 64-bit integer.
#[must_use]
pub fn seeds(fixed: &[u64]) -> Vec<u64> {
    match std::env::var(SEED_VAR) {
        Ok(text) => {
            let seed = text
                .trim()
                .parse()
                .unwrap_or_else(|error| panic!("{SEED_VAR}='{text}' is not a seed: {error}"));
            vec![seed]
        }
        Err(_) => fixed.to_vec(),
    }
}

/// The command that reruns `test`, in the test binary `binary`, with `seed` alone.
///
/// In a build with `--cfg tokio_unstable` the command sets it too, so the rerun replays the
/// failing run exactly.
#[must_use]
pub fn rerun_command(binary: &str, test: &str, seed: u64) -> String {
    let rustflags = if cfg!(tokio_unstable) {
        "RUSTFLAGS=\"--cfg tokio_unstable\" "
    } else {
        ""
    };
    format!(
        "{rustflags}{SEED_VAR}={seed} cargo test -p fleetringd --features simulation --test {binary} -- --exact {test}"
    )
}

/// Runs `scenario` once for each of [`seeds`]`(fixed)`, in the test binary named `binary`.
///
/// The test's name comes from the test thread, which the test harness names after the test.
///
/// # Panics
///
/// If `scenario` fails or panics for a seed, after printing the seed and its rerun command.
pub fn run<E: fmt::Display>(
    binary: &str,
    fixed: &[u64],
    mut scenario: impl FnMut(u64) -> Result<(), E>,
) {
    let thread = std::thread::current();
    let test = thread.name().unwrap_or("<test name>").to_owned();
    for seed in seeds(fixed) {
        let guard = RerunOnPanic {
            command: rerun_command(binary, &test, seed),
            seed,
        };
        if let Err(error) = scenario(seed) {
            panic!("seed {seed} failed: {error}");
        }
        drop(guard);
    }
}

/// Prints the rerun command if the scenario panics while it lives.
#[derive(Debug)]
struct RerunOnPanic {
    command: String,
    seed: u64,
}

impl Drop for RerunOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "\nsimulation seed {} failed; rerun it alone with:\n  {}\n",
                self.seed, self.command
            );
        }
    }
}

/// Converts a simulated duration to whole milliseconds, saturating.
pub(crate) fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rerun_command_names_the_seed_and_the_exact_test() {
        let command = rerun_command("sim", "cluster::three_nodes", 42);
        assert!(
            command.ends_with(
                "FLEETRING_SIM_SEED=42 cargo test -p fleetringd --features simulation --test sim -- --exact cluster::three_nodes"
            ),
            "{command}"
        );
        assert_eq!(
            command.contains("tokio_unstable"),
            cfg!(tokio_unstable),
            "{command}"
        );
    }
}
