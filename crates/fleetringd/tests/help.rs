//! FR-075: help that fits one screen (ADR-0043, point 7). `fleetring -h` gives one-line
//! summaries under grouped headings, `--help` the reference, and each group's `-h` one line per
//! command.

use std::error::Error;
use std::path::Path;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// The pinned `fleetring -h` at 100 columns, which a deliberate change regenerates with
/// `UPDATE_GOLDEN=1` and a reviewer reads in the diff.
const GOLDEN: &str = "tests/fixtures/help.txt";

/// The help `args` print at `columns` columns, from stdout, which isn't a terminal here, so
/// clap takes the width from `COLUMNS`.
fn help(args: &[&str], columns: u16) -> Result<String, Box<dyn Error>> {
    let output = support::fleetring()
        .env("COLUMNS", columns.to_string())
        .env_remove("NO_COLOR")
        .env_remove("CLICOLOR_FORCE")
        .args(args)
        .output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{args:?}: {stderr}");
    Ok(String::from_utf8(output.stdout)?)
}

#[test]
fn the_top_level_short_help_fits_one_screen() -> TestResult {
    let text = help(&["-h"], 100)?;
    let lines: Vec<&str> = text.lines().collect();
    assert!(lines.len() <= 50, "{} lines:\n{text}", lines.len());
    for line in &lines {
        assert!(
            line.chars().count() <= 100,
            "wider than 100 columns: {line}"
        );
    }
    Ok(())
}

#[test]
fn the_top_level_short_help_is_pinned() -> TestResult {
    let text = help(&["-h"], 100)?;
    let golden = Path::new(env!("CARGO_MANIFEST_DIR")).join(GOLDEN);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(&golden, &text)?;
    }
    let pinned = std::fs::read_to_string(&golden)?;
    assert_eq!(
        text, pinned,
        "`fleetring -h` changed; regenerate {GOLDEN} with UPDATE_GOLDEN=1 and review the diff"
    );
    Ok(())
}

#[test]
fn the_top_level_short_help_groups_the_commands() -> TestResult {
    let text = help(&["-h"], 100)?;
    let headings = [
        "Examples:",
        "Getting started:",
        "Databases:",
        "Fleet:",
        "Security:",
        "Agents:",
        "Recovery:",
        "Options:",
    ];
    let mut from = 0;
    for heading in headings {
        let at = text[from..]
            .find(&format!("\n{heading}\n"))
            .ok_or_else(|| format!("no {heading} after byte {from}:\n{text}"))?;
        from += at + 1;
    }
    assert!(!text.contains("\nCommands:"), "{text}");
    assert!(text.contains("  fleetring dev -d "), "{text}");
    Ok(())
}

#[test]
fn long_help_lists_the_exit_codes_and_the_compatibility_name() -> TestResult {
    let text = help(&["--help"], 100)?;
    assert!(text.contains("\nExit codes:\n"), "{text}");
    for code in ["0", "1", "2", "3", "4", "130"] {
        assert!(
            text.lines()
                .any(|line| line.trim_start().starts_with(&format!("{code} "))),
            "exit code {code} isn't listed:\n{text}"
        );
    }
    let last = text.trim_end().lines().last().unwrap_or_default();
    assert!(
        last.contains("`fleetringd` is the same program, kept until 1.0"),
        "the compatibility line doesn't close the help:\n{text}"
    );
    // The long help has more examples than the short one.
    assert!(text.contains("fleetring db query notes"), "{text}");
    Ok(())
}

#[test]
fn group_help_is_one_line_per_command() -> TestResult {
    let text = help(&["db", "-h"], 100)?;
    let commands = text
        .split_once("Commands:\n")
        .and_then(|(_, rest)| rest.split_once("\n\n"))
        .map(|(list, _)| list)
        .ok_or_else(|| format!("no command list:\n{text}"))?;
    let verbs = [
        "create", "show", "edit", "exec", "query", "watch", "locate", "list", "verify", "restore",
        "drop", "help",
    ];
    let lines: Vec<&str> = commands.lines().collect();
    assert_eq!(lines.len(), verbs.len(), "{commands}");
    for (line, verb) in lines.iter().zip(verbs) {
        assert!(line.starts_with(&format!("  {verb} ")), "{line}");
    }
    Ok(())
}
