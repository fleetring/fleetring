//! `fleetring completions <shell>`: shell completion (`docs/cli.md`, section 9).
//!
//! The script a shell loads is small: on each Tab it runs this binary again with
//! [`COMPLETE_VAR`] set, and the binary answers from its own command tree, before it parses
//! anything. Commands, flags and fixed values complete from the tree; context names and agent
//! IDs come from this machine's files, never the network.

use std::error::Error;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, ValueEnum};
use clap_complete::engine::CompletionCandidate;
use clap_complete::env::{Bash, CompleteEnv, EnvCompleter, Fish, Powershell, Zsh};
use fleetring_client::context::{Contexts, select};

use crate::agent::{self, AgentId};
use crate::compat::ProgramName;
use crate::{context_cmd, help};

/// The variable the script sets when it runs the binary to complete a command line.
pub(crate) const COMPLETE_VAR: &str = "FLEETRING_COMPLETE";

/// What `fleetring completions --help` adds below the arguments: how to load the script.
pub(crate) const COMPLETIONS_HELP: &str = "\
Add the line for your shell to its startup file:

  bash, in ~/.bashrc:
    source <(fleetring completions bash)
  zsh, in ~/.zshrc, after compinit:
    source <(fleetring completions zsh)
  fish, in ~/.config/fish/config.fish:
    fleetring completions fish | source
  PowerShell, in $PROFILE:
    fleetring completions powershell | Out-String | Invoke-Expression

The script runs this binary on each Tab, so it follows upgrades.";

/// `fleetring completions`' argument.
#[derive(Debug, Args)]
pub(crate) struct CompletionsArgs {
    /// The shell to print the script for
    shell: Shell,
}

/// A shell `completions` prints a script for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum Shell {
    /// GNU Bash
    Bash,
    /// Z shell
    Zsh,
    /// The friendly interactive shell
    Fish,
    /// PowerShell, on any platform
    Powershell,
}

impl Shell {
    fn completer(self) -> &'static dyn EnvCompleter {
        match self {
            Self::Bash => &Bash,
            Self::Zsh => &Zsh,
            Self::Fish => &Fish,
            Self::Powershell => &Powershell,
        }
    }
}

/// Answers a completion request and exits, if [`COMPLETE_VAR`] is set; returns otherwise.
/// Call it first in `main`, before the arguments are parsed.
pub(crate) fn complete_if_asked(name: ProgramName) {
    CompleteEnv::with_factory(move || help::root_command(name))
        .var(COMPLETE_VAR)
        .complete();
}

/// Prints `shell`'s script, which completes the binary as `name` and runs `arg0` to do it.
pub(crate) fn print(
    args: &CompletionsArgs,
    name: ProgramName,
    arg0: Option<OsString>,
) -> Result<ExitCode, Box<dyn Error>> {
    let completer = completer(arg0, std::env::current_dir().ok());
    let script = script(args.shell, name, &completer)?;
    crate::emit(&script)?;
    Ok(ExitCode::SUCCESS)
}

/// The script for `shell`.
fn script(shell: Shell, name: ProgramName, completer: &str) -> Result<String, Box<dyn Error>> {
    let mut script = Vec::new();
    shell.completer().write_registration(
        COMPLETE_VAR,
        name.as_str(),
        name.as_str(),
        completer,
        &mut script,
    )?;
    Ok(String::from_utf8(script)?)
}

/// The program the script runs: `arg0` as typed if it is a bare name, which the shell finds on
/// its `PATH`, or else made absolute against `cwd`, so that the script works from any directory.
fn completer(arg0: Option<OsString>, cwd: Option<PathBuf>) -> String {
    let program = PathBuf::from(arg0.unwrap_or_else(|| OsString::from("fleetring")));
    let program = match cwd {
        Some(cwd) if program.components().count() > 1 => cwd.join(program),
        _ => program,
    };
    program.to_string_lossy().into_owned()
}

/// The contexts' names, from the contexts file alone.
pub(crate) fn context_names() -> Vec<CompletionCandidate> {
    let mut names: Vec<CompletionCandidate> = contexts()
        .contexts
        .keys()
        .map(|name| CompletionCandidate::new(name.as_str()))
        .collect();
    names.sort_by(|a, b| a.get_value().cmp(b.get_value()));
    names
}

/// The IDs of the agents with a directory under the selected context's agents directory: the
/// context `FLEETRING_CONTEXT` names, or else the current one, as `agent list` finds them.
pub(crate) fn agent_ids() -> Vec<CompletionCandidate> {
    let contexts = contexts();
    let variable = std::env::var("FLEETRING_CONTEXT").ok();
    let selected = select(&contexts, None, variable.as_deref())
        .ok()
        .flatten()
        .map(|(name, context, _)| (name, context));
    let Ok(dir) = agent::agents_dir(selected, &|name| std::env::var_os(name)) else {
        return Vec::new();
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut ids: Vec<String> = entries
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| entry.file_name().to_str().map(str::to_owned))
        .filter(|name| name.parse::<AgentId>().is_ok())
        .collect();
    ids.sort();
    ids.into_iter().map(CompletionCandidate::new).collect()
}

/// The contexts file, or no contexts if it can't be read: a completion never fails.
fn contexts() -> Contexts {
    context_cmd::store()
        .ok()
        .and_then(|store| store.load().ok())
        .unwrap_or_else(Contexts::empty)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_shell_gets_a_script_that_runs_the_binary() -> Result<(), Box<dyn Error>> {
        for shell in Shell::value_variants() {
            for name in [ProgramName::Fleetring, ProgramName::Fleetringd] {
                let script = script(*shell, name, "fleetring")?;
                assert!(script.contains(COMPLETE_VAR), "{shell:?}: {script}");
                assert!(script.contains(name.as_str()), "{shell:?}: {script}");
            }
        }
        Ok(())
    }

    #[test]
    fn the_script_runs_a_bare_name_from_the_path_and_a_relative_one_from_here() {
        let cwd = std::env::temp_dir();
        assert_eq!(
            completer(Some(OsString::from("fleetring")), Some(cwd.clone())),
            "fleetring"
        );
        let relative = PathBuf::from("bin").join("fleetring");
        assert_eq!(
            completer(Some(relative.clone().into_os_string()), Some(cwd.clone())),
            cwd.join(relative).to_string_lossy()
        );
        assert_eq!(completer(None, None), "fleetring");
    }
}
