//! FR-075: one binary, `fleetring`, that also runs as `fleetringd` until 1.0 (ADR-0043, point 1).
//! The name it runs as changes what it calls itself, and nothing else.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// This build, copied into `dir` as `file_name`, with the platform's executable suffix.
fn copy_as(dir: &Path, file_name: &str) -> Result<PathBuf, Box<dyn Error>> {
    let path = dir.join(format!("{file_name}{}", std::env::consts::EXE_SUFFIX));
    std::fs::copy(env!("CARGO_BIN_EXE_fleetring"), &path)?;
    Ok(path)
}

/// Runs `bin` with `args`, with no client variables to change what a command does.
fn run(bin: &Path, args: &[&str]) -> Result<Output, Box<dyn Error>> {
    let mut command = Command::new(bin);
    support::isolate(&mut command);
    command.args(args);
    for variable in [
        "FLEETRING_CONTROL",
        "FLEETRING_TLS_DIR",
        "FLEETRING_TOKEN",
        "FLEETRING_AS",
        "FLEETRING_CA",
        "FLEETRING_CERT",
        "FLEETRING_KEY",
        "FLEETRING_LOG_FORMAT",
        "RUST_LOG",
    ] {
        command.env_remove(variable);
    }
    Ok(command.output()?)
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// The binary as `fleetring` and as `fleetringd` in a scratch directory, each with its name.
fn both_names(dir: &Path) -> Result<[(&'static str, PathBuf); 2], Box<dyn Error>> {
    Ok([
        ("fleetring", copy_as(dir, "fleetring")?),
        ("fleetringd", copy_as(dir, "fleetringd")?),
    ])
}

#[test]
fn version_prints_the_name_it_ran_as() -> TestResult {
    let dir = tempfile::tempdir()?;
    for (name, bin) in both_names(dir.path())? {
        for flag in ["--version", "-V"] {
            let printed = run(&bin, &[flag])?;
            assert!(printed.status.success(), "{name} {flag}: {printed:?}");
            assert_eq!(
                stdout(&printed),
                format!("{name} {}\n", env!("CARGO_PKG_VERSION"))
            );
        }
    }
    Ok(())
}

/// A symbolic link named `fleetringd`, as `install.sh` and the image make it, is `fleetringd`
/// too.
#[cfg(unix)]
#[test]
fn a_fleetringd_link_is_fleetringd() -> TestResult {
    let dir = tempfile::tempdir()?;
    let real = copy_as(dir.path(), "fleetring")?;
    let link = dir.path().join("fleetringd");
    std::os::unix::fs::symlink("fleetring", &link)?;
    let printed = run(&link, &["--version"])?;
    assert_eq!(
        stdout(&printed),
        format!("fleetringd {}\n", env!("CARGO_PKG_VERSION"))
    );
    let printed = run(&real, &["--version"])?;
    assert_eq!(
        stdout(&printed),
        format!("fleetring {}\n", env!("CARGO_PKG_VERSION"))
    );
    Ok(())
}

/// 0.6 ran a node as `fleetringd --config <file>`, and a 0.6 daemon's restart in place runs the
/// new binary with those arguments: they mean `serve` under either name.
#[test]
fn both_names_run_serve_from_bare_daemon_flags() -> TestResult {
    let dir = tempfile::tempdir()?;
    let missing = dir.path().join("nonexistent.toml");
    let missing = missing.to_str().ok_or("a UTF-8 path")?;
    for (name, bin) in both_names(dir.path())? {
        for args in [
            &["--config", missing][..],
            &["--config", missing, "--node-id", "n1"],
            &["--log-format", "json", "--config", missing],
            &["serve", "--config", missing],
        ] {
            let refused = run(&bin, args)?;
            // The configuration's error, from `serve`: not a usage error.
            assert_eq!(
                refused.status.code(),
                Some(1),
                "{name} {args:?}: {refused:?}"
            );
            assert!(
                stderr(&refused).starts_with(&format!("{name}: can't read config file {missing}")),
                "{name} {args:?}: {}",
                stderr(&refused)
            );
        }
        // `serve` needs `--config`.
        let usage = run(&bin, &["serve"])?;
        assert_eq!(usage.status.code(), Some(2), "{name}: {usage:?}");
        assert!(
            stderr(&usage).contains("--config <CONFIG>"),
            "{}",
            stderr(&usage)
        );
    }
    Ok(())
}

#[test]
fn no_arguments_prints_help_and_exits_2() -> TestResult {
    let dir = tempfile::tempdir()?;
    for (name, bin) in both_names(dir.path())? {
        let help = run(&bin, &[])?;
        assert_eq!(help.status.code(), Some(2), "{name}: {help:?}");
        let text = format!("{}{}", stdout(&help), stderr(&help));
        assert!(text.contains(&format!("Usage: {name} ")), "{name}: {text}");
        assert!(text.contains("serve "), "{name}: {text}");
        assert!(
            text.contains("`fleetringd` is the same program"),
            "{name}: {text}"
        );
    }
    Ok(())
}

/// clap would take usage lines from the file name verbatim: an upgrader's scratch or kept copy
/// must still say `fleetringd`, or `fleetring`.
#[cfg(unix)]
#[test]
fn usage_never_shows_the_file_name() -> TestResult {
    let dir = tempfile::tempdir()?;
    for (file_name, name) in [
        ("fleetringd.previous", "fleetringd"),
        (".fleetringd.new", "fleetringd"),
        ("fleetring.previous", "fleetring"),
        ("fleetring", "fleetring"),
    ] {
        let bin = copy_as(dir.path(), file_name)?;
        for args in [&["--bogus"][..], &["db", "list", "--bogus"]] {
            let refused = run(&bin, args)?;
            assert_eq!(refused.status.code(), Some(2), "{file_name}: {refused:?}");
            let text = stderr(&refused);
            assert!(
                text.contains(&format!("Usage: {name} ")),
                "{file_name}: {text}"
            );
            assert!(
                !text.contains(".previous") && !text.contains(".new"),
                "{file_name}: {text}"
            );
        }
        let version = run(&bin, &["--version"])?;
        assert_eq!(
            stdout(&version),
            format!("{name} {}\n", env!("CARGO_PKG_VERSION"))
        );
    }
    Ok(())
}

#[test]
fn client_commands_are_the_same_under_both_names() -> TestResult {
    let dir = tempfile::tempdir()?;
    let outcomes = both_names(dir.path())?.map(|(name, bin)| -> Result<_, Box<dyn Error>> {
        let output = run(
            &bin,
            &[
                "db",
                "list",
                "--control",
                "127.0.0.1:1",
                "--insecure-plaintext",
            ],
        )?;
        let text = stderr(&output);
        let message = text
            .strip_prefix(&format!("{name}: "))
            .ok_or_else(|| format!("{name}'s error doesn't start with its name: {text}"))?
            .to_owned();
        Ok((output.status.code(), stdout(&output), message))
    });
    let [fleetring, fleetringd] = outcomes;
    let (fleetring, fleetringd) = (fleetring?, fleetringd?);
    // Nothing listens there: unavailable, which a retry may fix.
    assert_eq!(fleetring.0, Some(4), "{fleetring:?}");
    assert_eq!(fleetring, fleetringd);
    Ok(())
}

/// 0.6 spelled the archive commands `db archive show|verify` and `archive restore --from <db>`;
/// both fail as a usage error, with a `hint:` line naming the 0.7 spelling (ADR-0043, point 2).
#[test]
fn the_old_archive_commands_fail_with_a_hint() -> TestResult {
    let dir = tempfile::tempdir()?;
    for (name, bin) in both_names(dir.path())? {
        for (args, hint) in [
            (
                &["db", "archive", "show", "ledger", "--archive", "backups"][..],
                "hint: `db archive show|verify` is now `fleetring archive show|verify`",
            ),
            (
                &["db", "archive", "verify", "ledger", "--archive", "backups"],
                "hint: `db archive show|verify` is now `fleetring archive show|verify`",
            ),
            (
                &[
                    "archive",
                    "restore",
                    "--config",
                    "node.toml",
                    "--archive",
                    "backups",
                    "--from",
                    "ledger",
                    "--to",
                    "out",
                ],
                "hint: `--from <db>` is now the argument: `fleetring archive restore <db> --to <dir>`",
            ),
        ] {
            let refused = run(&bin, args)?;
            assert_eq!(refused.status.code(), Some(2), "{name} {args:?}");
            assert!(stdout(&refused).is_empty(), "{name} {args:?}");
            let text = stderr(&refused);
            assert!(text.trim_end().ends_with(hint), "{name} {args:?}: {text}");
        }
        // The new spellings are not refused for their names.
        for args in [
            &["archive", "show", "--help"][..],
            &["archive", "verify", "--help"],
            &["archive", "restore", "--help"],
            &["node", "list", "--help"],
        ] {
            let help = run(&bin, args)?;
            assert!(help.status.success(), "{name} {args:?}: {}", stderr(&help));
        }
    }
    Ok(())
}

/// `fleet`, the short name (ADR-0043, amendment of 2026-11-07), copied into `dir`.
fn fleet_in(dir: &Path) -> Result<PathBuf, Box<dyn Error>> {
    copy_as(dir, "fleet")
}

#[test]
fn fleet_prints_its_own_name() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = fleet_in(dir.path())?;
    for flag in ["--version", "-V"] {
        let printed = run(&fleet, &[flag])?;
        assert!(printed.status.success(), "{flag}: {printed:?}");
        assert_eq!(
            stdout(&printed),
            format!("fleet {}\n", env!("CARGO_PKG_VERSION"))
        );
    }
    let help = run(&fleet, &[])?;
    assert_eq!(help.status.code(), Some(2), "{help:?}");
    let text = format!("{}{}", stdout(&help), stderr(&help));
    assert!(text.contains("Usage: fleet "), "{text}");
    assert!(text.contains("  fleet dev -d "), "{text}");
    assert!(text.contains("permanent short name"), "{text}");
    for args in [&["--bogus"][..], &["db", "list", "--bogus"]] {
        let refused = run(&fleet, args)?;
        assert_eq!(refused.status.code(), Some(2), "{args:?}");
        assert!(stderr(&refused).contains("Usage: fleet "), "{args:?}");
    }
    Ok(())
}

/// A symbolic link named `fleet`, as `install.sh` and the image make it, is `fleet` too.
#[cfg(unix)]
#[test]
fn a_fleet_link_is_fleet() -> TestResult {
    let dir = tempfile::tempdir()?;
    copy_as(dir.path(), "fleetring")?;
    let link = dir.path().join("fleet");
    std::os::unix::fs::symlink("fleetring", &link)?;
    let printed = run(&link, &["--version"])?;
    assert_eq!(
        stdout(&printed),
        format!("fleet {}\n", env!("CARGO_PKG_VERSION"))
    );
    Ok(())
}

/// Only the exact name: `fleet-foo` and `fleet.previous` are not `fleet`.
#[test]
fn only_the_exact_name_is_fleet() -> TestResult {
    let dir = tempfile::tempdir()?;
    for file_name in ["fleet-foo", "fleet.previous"] {
        let bin = copy_as(dir.path(), file_name)?;
        let printed = run(&bin, &["--version"])?;
        assert_eq!(
            stdout(&printed),
            format!("fleetring {}\n", env!("CARGO_PKG_VERSION")),
            "{file_name}"
        );
    }
    Ok(())
}

/// Unlike `fleetringd`, `fleet` has no compatibility behaviour: 0.6's bare daemon flags are an
/// unknown argument, not `serve`.
#[test]
fn fleet_does_not_imply_serve() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = fleet_in(dir.path())?;
    for args in [
        &["--config", "x"][..],
        &["--config=x", "--node-id", "n1"],
        &["--log-format", "json", "--config", "x"],
    ] {
        let refused = run(&fleet, args)?;
        assert_eq!(refused.status.code(), Some(2), "{args:?}: {refused:?}");
        let text = stderr(&refused);
        assert!(text.contains("unexpected argument"), "{args:?}: {text}");
        assert!(text.contains("Usage: fleet "), "{args:?}: {text}");
    }
    Ok(())
}

#[test]
fn client_commands_are_the_same_under_fleet_and_fleetring() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleetring = copy_as(dir.path(), "fleetring")?;
    let fleet = fleet_in(dir.path())?;
    let outcome = |bin: &Path, name: &str| -> Result<_, Box<dyn Error>> {
        let output = run(
            bin,
            &[
                "db",
                "list",
                "--control",
                "127.0.0.1:1",
                "--insecure-plaintext",
            ],
        )?;
        let text = stderr(&output);
        let message = text
            .strip_prefix(&format!("{name}: "))
            .ok_or_else(|| format!("{name}'s error doesn't start with its name: {text}"))?
            .to_owned();
        Ok((output.status.code(), stdout(&output), message))
    };
    let fleetring = outcome(&fleetring, "fleetring")?;
    let fleet = outcome(&fleet, "fleet")?;
    // The hints name the command as it was run; the rest is the same.
    let fleet = (fleet.0, fleet.1, fleet.2.replace("`fleet ", "`fleetring "));
    // Nothing listens there: unavailable, which a retry may fix.
    assert_eq!(fleet.0, Some(4), "{fleet:?}");
    assert_eq!(fleetring, fleet);
    Ok(())
}

/// Hints that name a command say `fleet`, under `fleet`.
#[test]
fn fleet_hints_name_fleet() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleetring = copy_as(dir.path(), "fleetring")?;
    let fleet = fleet_in(dir.path())?;
    let args = ["db", "list"];
    let text = stderr(&run(&fleet, &args)?);
    assert!(text.starts_with("fleet: no control address"), "{text}");
    assert!(text.contains("`fleet dev -d`"), "{text}");
    assert!(!text.contains("`fleetring "), "{text}");
    let text = stderr(&run(&fleetring, &args)?);
    assert!(text.contains("`fleetring dev -d`"), "{text}");
    for (bin, name) in [(&fleet, "fleet"), (&fleetring, "fleetring")] {
        let refused = run(bin, &["db", "archive", "show", "ledger", "--archive", "b"])?;
        let text = stderr(&refused);
        let want = format!("is now `{name} archive show|verify`");
        assert!(text.trim_end().ends_with(&want), "{name}: {text}");
    }
    Ok(())
}

/// `fleet completions <shell>` registers `fleet`, and calls back into `fleet`.
#[test]
fn fleet_completions_register_fleet() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleetring = copy_as(dir.path(), "fleetring")?;
    let fleet = fleet_in(dir.path())?;
    let fleet_path = fleet.to_string_lossy().into_owned();
    for shell in ["bash", "zsh", "fish", "powershell"] {
        let output = run(&fleet, &["completions", shell])?;
        assert!(output.status.success(), "{shell}: {}", stderr(&output));
        let script = stdout(&output);
        assert!(script.contains("FLEETRING_COMPLETE"), "{shell}: {script}");
        assert!(script.contains(&fleet_path), "{shell}: {script}");
    }
    let bash = stdout(&run(&fleet, &["completions", "bash"])?);
    assert!(bash.contains("_clap_complete_fleet()"), "{bash}");
    assert!(!bash.contains("_clap_complete_fleetring()"), "{bash}");
    let plain = stdout(&run(&fleetring, &["completions", "bash"])?);
    assert!(plain.contains("_clap_complete_fleetring()"), "{plain}");
    Ok(())
}
