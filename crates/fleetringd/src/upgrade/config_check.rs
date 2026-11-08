//! Checking a node's configuration with the release an upgrade or a rollback would install,
//! before it swaps anything (ADR-0038, amendment of 2026-11-08).
//!
//! The release's own binary runs `serve --check` on the node's command line, so the answer is the
//! release's: it is the one that will read the configuration when the node restarts. A release
//! before 0.8 has no `serve --check`, and says so in `upgrade --software`; its configuration goes
//! unchecked, with a note.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use fleetring_client::output::printable;
use fleetring_control::Release;

use crate::upgrade::install::Reported;
use crate::upgrade::{LocalNode, Options, ProcessFile, Restart, Step, UpgradeError};

/// The flag that makes `serve` check its configuration and flags and start nothing.
const CHECK_FLAG: &str = "--check";

/// The most lines of a release's answer that a report or a refusal shows.
const MAX_LINES: usize = 40;

/// A command line that asks a release to check a node's configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckCommand {
    /// The arguments after the program, ending with `--check`.
    pub(crate) args: Vec<OsString>,
    /// The directory to run it in, or `None` for this command's own.
    pub(crate) cwd: Option<PathBuf>,
}

/// The command line that checks `node`'s configuration as its restart would read it. The running
/// daemon's own arguments, with `--check` added, in the directory it started in, when its process
/// file records both and the arguments run a node from a configuration. Otherwise `serve --config
/// <file> --check` for the file `--config` named, since relative paths in arguments recorded
/// without their directory can't be resolved, and a dev node's arguments name no file.
pub(crate) fn check_command(
    node: &LocalNode,
    running: Option<&Option<ProcessFile>>,
) -> CheckCommand {
    if let Some(Some(daemon)) = running
        && let (Some(args), Some(cwd)) = (daemon.serve_args(), &daemon.cwd)
    {
        return CheckCommand {
            args: args
                .iter()
                .map(OsString::from)
                .chain([OsString::from(CHECK_FLAG)])
                .collect(),
            cwd: Some(cwd.clone()),
        };
    }
    CheckCommand {
        args: vec![
            OsString::from("serve"),
            OsString::from("--config"),
            node.config.clone().into_os_string(),
            OsString::from(CHECK_FLAG),
        ],
        cwd: None,
    }
}

/// How a release answered a check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Answer {
    /// It exited 0: the node would start.
    Accepted,
    /// It exited otherwise, with what it printed.
    Refused(String),
    /// It gave no answer, for this reason: it couldn't be run, or didn't finish in time.
    Unanswered(String),
}

/// Runs the binary at `binary` with `command`, an argument vector and no shell, for at most
/// `limit`, and returns its answer. A run that outlasts `limit` is killed.
pub(crate) async fn ask(binary: &Path, command: &CheckCommand, limit: Duration) -> Answer {
    let mut process = tokio::process::Command::new(binary);
    process
        .args(&command.args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(cwd) = &command.cwd {
        process.current_dir(cwd);
    }
    let child = match process.spawn() {
        Ok(child) => child,
        Err(error) => return Answer::Unanswered(format!("it doesn't run: {error}")),
    };
    // Dropping the unfinished wait drops the child, which kills it.
    match tokio::time::timeout(limit, child.wait_with_output()).await {
        Err(_) => Answer::Unanswered(format!(
            "it didn't finish within {}s",
            limit.as_secs().max(1)
        )),
        Ok(Err(error)) => Answer::Unanswered(format!("it couldn't be waited for: {error}")),
        Ok(Ok(output)) if output.status.success() => Answer::Accepted,
        Ok(Ok(output)) => {
            let mut said = String::from_utf8_lossy(&output.stdout).into_owned();
            said.push_str(&String::from_utf8_lossy(&output.stderr));
            if said.trim().is_empty() {
                said = format!("nothing, and exited with {}", output.status);
            }
            Answer::Refused(said)
        }
    }
}

/// What a check found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Found {
    /// The release has no `serve --check`.
    Unsupported,
    /// The release answered.
    Answered(Answer),
}

/// The report's line when no `--config` named a node, so there was nothing to check.
fn nothing_checked(release: &Release) -> String {
    format!(
        "no node's configuration was checked; pass --config to check one with fleetring \
         {release}\n"
    )
}

/// Whether the swap goes on, from what the check of `release` found for `node`: the report's line
/// if it does, or the refusal. A configuration the release doesn't accept refuses the swap unless
/// `restart` is [`Restart::No`], which installs anyway, since the fix is to edit the
/// configuration once the new binary is in place and restart the node then.
///
/// # Errors
///
/// [`UpgradeError::ConfigRefused`] if the release refused the configuration, or gave no answer,
/// and the node would be restarted.
pub(crate) fn decide(
    found: Found,
    release: &Release,
    node: &LocalNode,
    restart: Restart,
) -> Result<String, UpgradeError> {
    let (summary, said) = match found {
        Found::Unsupported => {
            return Ok(format!(
                "fleetring {release} has no `serve --check`, so node {}'s configuration wasn't \
                 checked; its release notes say what to change\n",
                node.id
            ));
        }
        Found::Answered(Answer::Accepted) => {
            return Ok(format!(
                "fleetring {release} accepts node {}'s configuration\n",
                node.id
            ));
        }
        Found::Answered(Answer::Refused(said)) => (
            format!(
                "fleetring {release} refuses node {}'s configuration, so the node wouldn't start \
                 on it",
                node.id
            ),
            format!("  fleetring {release} says:\n{}", indented(&said)),
        ),
        Found::Answered(Answer::Unanswered(reason)) => (
            format!(
                "fleetring {release} couldn't check node {}'s configuration, so the node might \
                 not start on it",
                node.id
            ),
            format!("  {}", printable(&reason)),
        ),
    };
    let config = node.config.display().to_string();
    match restart {
        Restart::InPlace => Err(UpgradeError::ConfigRefused {
            summary,
            said,
            config,
        }),
        Restart::No => Ok(format!(
            "{summary}:\n{said}\nedit the configuration until `fleetring serve --config {config} \
             --check` passes, then restart node {}\n",
            node.id
        )),
    }
}

/// Checks `options.node`'s configuration with the release at `binary`, which `software`
/// describes, and returns the report's line, or refuses as [`decide`] does. `running` is the
/// running daemon, as `upgrade` found it.
///
/// # Errors
///
/// What [`decide`] returns.
pub(crate) async fn check(
    binary: &Path,
    software: &Reported,
    options: &Options,
    running: Option<&Option<ProcessFile>>,
) -> Result<String, UpgradeError> {
    let Some(node) = &options.node else {
        return Ok(nothing_checked(&software.release));
    };
    let found = if software.config_check {
        options.steps.begin(Step::CheckConfig);
        Found::Answered(ask(binary, &check_command(node, running), options.timeout).await)
    } else {
        Found::Unsupported
    };
    decide(found, &software.release, node, options.restart)
}

/// `text`'s first [`MAX_LINES`] lines, each indented four spaces, with what a terminal would act
/// on escaped: the text is another binary's.
fn indented(text: &str) -> String {
    let lines: Vec<&str> = text.trim_end().lines().collect();
    let mut out: Vec<String> = lines
        .iter()
        .take(MAX_LINES)
        .map(|line| format!("    {}", printable(line)))
        .collect();
    if lines.len() > MAX_LINES {
        out.push(format!("    … {} more lines", lines.len() - MAX_LINES));
    }
    out.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn node(dir: &Path) -> TestResult<LocalNode> {
        Ok(LocalNode {
            id: "node-1".parse()?,
            data_dir: dir.join("data"),
            config: dir.join("node.toml"),
        })
    }

    fn daemon(args: &[&str], cwd: Option<PathBuf>) -> ProcessFile {
        ProcessFile {
            pid: 1,
            exe: PathBuf::from("fleetring"),
            version: "0.8.0".to_owned(),
            restarts: 0,
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            supervisor: None,
            started_by: None,
            cwd,
        }
    }

    fn os(words: &[&str]) -> Vec<OsString> {
        words.iter().map(OsString::from).collect()
    }

    #[test]
    fn the_running_daemons_command_line_is_checked_where_it_started() -> TestResult {
        let dir = tempfile::tempdir()?;
        let node = node(dir.path())?;
        let started = dir.path().join("started-here");
        for args in [
            &[
                "serve",
                "--config",
                "node.toml",
                "--listen",
                "127.0.0.1:7400",
            ][..],
            // 0.6's form, which the release reads as `serve` too.
            &["--log-format", "json", "--config", "node.toml"],
        ] {
            let running = Some(daemon(args, Some(started.clone())));
            let command = check_command(&node, Some(&running));
            let mut expected = os(args);
            expected.push(OsString::from("--check"));
            assert_eq!(command.args, expected, "{args:?}");
            assert_eq!(command.cwd.as_ref(), Some(&started), "{args:?}");
        }
        Ok(())
    }

    #[test]
    fn otherwise_the_file_config_names_is_checked() -> TestResult {
        let dir = tempfile::tempdir()?;
        let node = node(dir.path())?;
        let mut file_only = os(&["serve", "--config"]);
        file_only.push(node.config.clone().into_os_string());
        file_only.push(OsString::from("--check"));
        let cases = [
            // No node runs.
            None,
            // A daemon that wrote no process file, as 0.3.0.
            Some(None),
            // Arguments recorded without their directory (0.7), whose relative paths can't be
            // resolved.
            Some(Some(daemon(&["serve", "--config", "node.toml"], None))),
            // A file before 0.7, which records no arguments.
            Some(Some(daemon(&[], Some(dir.path().to_owned())))),
            // A dev node, whose arguments name no file.
            Some(Some(daemon(
                &["dev", "--data-dir", "d"],
                Some(dir.path().to_owned()),
            ))),
        ];
        for running in cases {
            let command = check_command(&node, running.as_ref());
            assert_eq!(command.args, file_only, "{running:?}");
            assert_eq!(command.cwd, None, "{running:?}");
        }
        Ok(())
    }

    fn release() -> Release {
        Release::new(0, 8, 0)
    }

    #[test]
    fn an_accepted_or_unchecked_configuration_lets_the_swap_go_on() -> TestResult {
        let dir = tempfile::tempdir()?;
        let node = node(dir.path())?;
        for restart in [Restart::InPlace, Restart::No] {
            let accepted = decide(
                Found::Answered(Answer::Accepted),
                &release(),
                &node,
                restart,
            )?;
            assert_eq!(
                accepted,
                "fleetring 0.8.0 accepts node node-1's configuration\n"
            );
            let unsupported = decide(Found::Unsupported, &release(), &node, restart)?;
            assert_eq!(
                unsupported,
                "fleetring 0.8.0 has no `serve --check`, so node node-1's configuration wasn't \
                 checked; its release notes say what to change\n"
            );
        }
        assert_eq!(
            nothing_checked(&release()),
            "no node's configuration was checked; pass --config to check one with fleetring \
             0.8.0\n"
        );
        Ok(())
    }

    #[test]
    fn a_refused_configuration_refuses_the_swap_unless_no_restart() -> TestResult {
        let dir = tempfile::tempdir()?;
        let node = node(dir.path())?;
        let config = node.config.display().to_string();
        let said = "fleetring: node.toml:3:1: field admin.port: unknown field `port`\n";
        let refused = decide(
            Found::Answered(Answer::Refused(said.to_owned())),
            &release(),
            &node,
            Restart::InPlace,
        );
        let Err(error @ UpgradeError::ConfigRefused { .. }) = refused else {
            panic!("not refused: {refused:?}");
        };
        assert_eq!(
            error.to_string(),
            format!(
                "fleetring 0.8.0 refuses node node-1's configuration, so the node wouldn't start \
                 on it; nothing was changed\n  fleetring 0.8.0 says:\n    fleetring: \
                 node.toml:3:1: field admin.port: unknown field `port`\n  hint: run the command \
                 again with --no-restart, edit the configuration until `fleetring serve --config \
                 {config} --check` passes, then restart the node"
            )
        );
        // With --no-restart the binary is swapped in, and the report says what to do first.
        let installed = decide(
            Found::Answered(Answer::Refused(said.to_owned())),
            &release(),
            &node,
            Restart::No,
        )?;
        assert!(
            installed.starts_with(
                "fleetring 0.8.0 refuses node node-1's configuration, so the node wouldn't start \
                 on it:\n  fleetring 0.8.0 says:\n    fleetring: node.toml:3:1:"
            ) && installed.ends_with(&format!(
                "edit the configuration until `fleetring serve --config {config} --check` \
                 passes, then restart node node-1\n"
            )),
            "{installed}"
        );
        Ok(())
    }

    #[test]
    fn no_answer_refuses_like_a_refusal() -> TestResult {
        let dir = tempfile::tempdir()?;
        let node = node(dir.path())?;
        let unanswered = Found::Answered(Answer::Unanswered("it didn't finish within 5s".into()));
        let refused = decide(unanswered.clone(), &release(), &node, Restart::InPlace);
        assert!(
            matches!(&refused, Err(UpgradeError::ConfigRefused { summary, said, .. })
                if summary.contains("couldn't check node node-1's configuration")
                    && said == "  it didn't finish within 5s"),
            "{refused:?}"
        );
        let installed = decide(unanswered, &release(), &node, Restart::No)?;
        assert!(installed.contains("couldn't check"), "{installed}");
        Ok(())
    }

    #[test]
    fn what_a_release_says_is_indented_escaped_and_bounded() {
        assert_eq!(indented("one\ntwo\n\n"), "    one\n    two");
        assert_eq!(indented("a\u{1b}[2Jb"), "    a\\u{001B}[2Jb");
        let long: String = (0..50).map(|line| format!("{line}\n")).collect();
        let shown = indented(&long);
        assert_eq!(shown.lines().count(), MAX_LINES + 1);
        assert!(shown.ends_with("    … 10 more lines"), "{shown}");
    }

    /// A script at `dir/name` that runs `body`.
    #[cfg(unix)]
    fn script(dir: &Path, name: &str, body: &str) -> TestResult<PathBuf> {
        use std::os::unix::fs::PermissionsExt as _;
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n"))?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;
        Ok(path)
    }

    /// Asks the script `binary` to check `command`, retrying while Linux reports the
    /// just-written script busy (ETXTBSY), as another test thread's fork can for a moment
    /// (#129).
    #[cfg(unix)]
    async fn ask_retrying(binary: &Path, command: &CheckCommand, limit: Duration) -> Answer {
        for _ in 0..20 {
            match ask(binary, command, limit).await {
                Answer::Unanswered(reason) if reason.contains("busy") => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                answer => return answer,
            }
        }
        ask(binary, command, limit).await
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_release_is_asked_with_an_argument_vector_in_the_nodes_directory() -> TestResult {
        let dir = tempfile::tempdir()?;
        let seen = dir.path().join("seen");
        let binary = script(
            dir.path(),
            "accepts",
            &format!(
                "pwd > '{seen}'\nfor arg in \"$@\"; do echo \"[$arg]\" >> '{seen}'; done",
                seen = seen.display()
            ),
        )?;
        let cwd = dir.path().join("node dir");
        std::fs::create_dir_all(&cwd)?;
        let command = CheckCommand {
            args: os(&["serve", "--config", "a b; touch pwned", "--check"]),
            cwd: Some(cwd.clone()),
        };
        let answer = ask_retrying(&binary, &command, Duration::from_secs(30)).await;
        assert_eq!(answer, Answer::Accepted);
        let seen = std::fs::read_to_string(&seen)?;
        let mut lines = seen.lines();
        let ran_in = lines.next().ok_or("no directory")?;
        assert_eq!(std::fs::canonicalize(ran_in)?, std::fs::canonicalize(&cwd)?);
        assert_eq!(
            lines.collect::<Vec<_>>(),
            ["[serve]", "[--config]", "[a b; touch pwned]", "[--check]"]
        );
        assert!(!cwd.join("pwned").exists());
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_refusal_carries_what_the_release_printed() -> TestResult {
        let dir = tempfile::tempdir()?;
        let command = CheckCommand {
            args: os(&["serve", "--check"]),
            cwd: None,
        };
        let refuses = script(
            dir.path(),
            "refuses",
            "echo 'on stdout'\necho 'fleetring: node.toml: field x: wrong' >&2\nexit 1",
        )?;
        assert_eq!(
            ask_retrying(&refuses, &command, Duration::from_secs(30)).await,
            Answer::Refused("on stdout\nfleetring: node.toml: field x: wrong\n".to_owned())
        );
        let silent = script(dir.path(), "silent", "exit 2")?;
        let Answer::Refused(said) = ask_retrying(&silent, &command, Duration::from_secs(30)).await
        else {
            panic!("a usage error is a refusal");
        };
        assert!(said.starts_with("nothing, and exited with"), "{said}");
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_release_that_hangs_or_doesnt_run_gives_no_answer() -> TestResult {
        let dir = tempfile::tempdir()?;
        let command = CheckCommand {
            args: os(&["serve", "--check"]),
            cwd: None,
        };
        let hangs = script(dir.path(), "hangs", "sleep 30")?;
        let started = tokio::time::Instant::now();
        let answer = ask_retrying(&hangs, &command, Duration::from_millis(300)).await;
        assert!(
            matches!(&answer, Answer::Unanswered(reason) if reason == "it didn't finish within 1s"),
            "{answer:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "it waited for the script"
        );
        let missing = ask(
            &dir.path().join("missing"),
            &command,
            Duration::from_secs(1),
        )
        .await;
        assert!(
            matches!(&missing, Answer::Unanswered(reason) if reason.starts_with("it doesn't run")),
            "{missing:?}"
        );
        Ok(())
    }
}
