//! The top-level help: examples first, then the commands under headings (ADR-0043, point 7).
//!
//! clap gives subcommands a single heading, so the root's template leaves `{subcommands}` out,
//! and this module renders the grouped list into `before_help` (for `-h`) and
//! `before_long_help` (for `--help`) from the command tree itself. Every subcommand stays
//! visible, so completions and man pages list each one.

use std::fmt::Write as _;

use clap::CommandFactory as _;
use clap::builder::StyledStr;

use crate::Cli;
use crate::compat::ProgramName;

/// The top-level groups, in order (ADR-0043 point 7.3). A new command adds itself to one.
pub(crate) const GROUPS: &[(&str, &[&str])] = &[
    (
        "Getting started",
        &["dev", "serve", "stop", "switch", "context", "completions"],
    ),
    ("Databases", &["db"]),
    ("Fleet", &["status", "node", "orphan", "upgrade", "lease"]),
    ("Security", &["cert", "token"]),
    ("Agents", &["mcp", "agent"]),
    ("Recovery", &["archive"]),
];

/// The root's layout: no `{subcommands}`, since [`grouped`] lists them in `{before-help}`. The
/// `Options:` heading is added by [`root_template`], because `{options}` prints none.
pub(crate) const ROOT_TEMPLATE: &str =
    "{about-with-newline}\n{usage-heading} {usage}\n\n{before-help}{options}{after-help}";

/// `fleetring -h`'s examples: a command, then what it does (ADR-0043 point 7.4).
const EXAMPLES: &[(&str, &str)] = &[
    ("fleetring dev -d", "start a dev node in the background"),
    ("fleetring switch dev", "use it (dev does this for you)"),
    ("fleetring db create notes", "create a database"),
    ("fleetring switch prod", "talk to another fleet"),
];

/// What `fleetring --help` adds to [`EXAMPLES`].
const MORE_EXAMPLES: &[(&str, &str)] = &[
    ("fleetring status", "list the members and their liveness"),
    ("fleetring db list", "list the databases"),
    (
        "fleetring db show notes",
        "describe a database and its schema",
    ),
    (
        "fleetring db exec notes 'CREATE TABLE note (body TEXT)'",
        "run a write statement on the primary",
    ),
    (
        "fleetring db query notes 'SELECT count(*) FROM note' --json",
        "run a query, and print JSON",
    ),
    (
        "fleetring context add home --control 10.0.0.5:7400 --tls-dir tls",
        "name a fleet, so commands need no flags",
    ),
    (
        "fleetring agent add 42 --print-config claude",
        "set an agent up, print its MCP config",
    ),
    ("fleetring dev stop", "stop the background dev node"),
];

/// The width of an example's command column, as ADR-0043 point 7.4 lays it out. A longer
/// command puts its description on the next line.
const EXAMPLE_COLUMN: usize = 35;

/// What `fleetring -h` says below the options: the compatibility names (ADR-0043, point 1.5).
pub(crate) const COMPAT_HELP: &str = "\
`fleet` is a permanent short name for `fleetring`.\n\
`fleetringd` is the same program, kept until 1.0; `fleetringd --config <file>` runs `serve`.";

/// What `fleetring --help` says below the options: the exit codes (`docs/cli.md`, section 8),
/// then the compatibility names.
const LONG_HELP: &str = "\
Exit codes:
  0    success
  1    the command failed
  2    usage: a bad flag or argument, or a confirmation without a terminal
  3    refused: unauthenticated, or not permitted
  4    unavailable, and a retry may succeed
  130  interrupted";

/// How much [`grouped`] says about each command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Detail {
    /// The examples, and each command's summary: `-h`.
    Short,
    /// More examples, and each command's subcommands under its summary: `--help`.
    Long,
}

/// The examples and the grouped command list, from `command`'s own subcommands' `about`,
/// styled with `command`'s header and literal styles so that clap's colour choice applies.
pub(crate) fn grouped(command: &clap::Command, detail: Detail) -> StyledStr {
    let styles = command.get_styles();
    let header = styles.get_header();
    let literal = styles.get_literal();
    let mut out = String::new();
    let _ = write!(out, "{header}Examples:{header:#}");
    let more: &[(&str, &str)] = match detail {
        Detail::Short => &[],
        Detail::Long => MORE_EXAMPLES,
    };
    // `fleet` shows its own name in the examples; `fleetringd` has always shown `fleetring`.
    let program = match command.get_bin_name() {
        Some("fleet") => "fleet",
        _ => "fleetring",
    };
    for (example, what) in EXAMPLES.iter().chain(more) {
        let example = format!("{program}{}", &example["fleetring".len()..]);
        let _ = if example.len() < EXAMPLE_COLUMN {
            write!(out, "\n  {example:<EXAMPLE_COLUMN$}{what}")
        } else {
            write!(out, "\n  {example}\n  {:EXAMPLE_COLUMN$}{what}", "")
        };
    }
    let width = GROUPS
        .iter()
        .flat_map(|(_, names)| names.iter())
        .map(|name| name.len())
        .max()
        .unwrap_or(0);
    for (heading, names) in GROUPS {
        let _ = write!(out, "\n\n{header}{heading}:{header:#}");
        for name in *names {
            let Some(sub) = command.find_subcommand(name) else {
                continue;
            };
            let about = sub.get_about().map(ToString::to_string).unwrap_or_default();
            let _ = write!(out, "\n  {literal}{name:<width$}{literal:#}  {about}");
            let verbs: Vec<&str> = sub
                .get_subcommands()
                .map(clap::Command::get_name)
                .filter(|verb| *verb != "help")
                .collect();
            if detail == Detail::Long && !verbs.is_empty() {
                let _ = write!(out, "\n  {:width$}  {}", "", verbs.join(", "));
            }
        }
    }
    // `{before-help}` adds the blank line that follows.
    StyledStr::from(out)
}

/// [`ROOT_TEMPLATE`] with a styled `Options:` heading before `{options}`.
fn root_template(command: &clap::Command) -> StyledStr {
    let header = command.get_styles().get_header();
    StyledStr::from(ROOT_TEMPLATE.replace(
        "{options}",
        &format!("{header}Options:{header:#}\n{{options}}"),
    ))
}

/// `Cli::command()` as the binary named `name` prints it: the template, `before_help`,
/// `before_long_help`, the compatibility line, and the long `after_long_help` (exit codes and
/// the compatibility line).
pub(crate) fn root_command(name: ProgramName) -> clap::Command {
    // Both: `display_name` sets `--version`; usage lines come from `bin_name`, which clap would
    // otherwise take from argv[0] verbatim (`fleetringd.previous`, `.fleetringd.new`).
    let command = Cli::command()
        .display_name(name.as_str())
        .bin_name(name.as_str());
    let short = grouped(&command, Detail::Short);
    let long = grouped(&command, Detail::Long);
    let template = root_template(&command);
    command
        .help_template(template)
        .before_help(short)
        .before_long_help(long)
        .after_help(COMPAT_HELP)
        .after_long_help(format!("{LONG_HELP}\n\n{COMPAT_HELP}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The commands `GROUPS` lists, with clap's own `help` left out.
    fn subcommands() -> Vec<String> {
        Cli::command()
            .get_subcommands()
            .map(|sub| sub.get_name().to_owned())
            .filter(|name| name != "help")
            .collect()
    }

    #[test]
    fn every_subcommand_is_in_exactly_one_group() {
        for name in subcommands() {
            let groups: Vec<&str> = GROUPS
                .iter()
                .filter(|(_, names)| names.contains(&name.as_str()))
                .map(|(heading, _)| *heading)
                .collect();
            assert_eq!(groups.len(), 1, "{name} is in {groups:?}");
        }
    }

    #[test]
    fn every_group_name_is_a_subcommand() {
        let subcommands = subcommands();
        for (heading, names) in GROUPS {
            for name in *names {
                assert!(
                    subcommands.iter().any(|sub| sub == name),
                    "{heading} lists {name}, which isn't a command"
                );
            }
        }
    }

    /// Every summary that `command` and its arguments show under `-h`, with where it is.
    fn summaries(command: &clap::Command, path: &str, found: &mut Vec<(String, Option<String>)>) {
        let path = format!("{path} {}", command.get_name()).trim().to_owned();
        found.push((path.clone(), command.get_about().map(ToString::to_string)));
        for arg in command.get_arguments() {
            found.push((
                format!("{path} {}", arg.get_id()),
                arg.get_help().map(ToString::to_string),
            ));
            for value in arg.get_possible_values() {
                if let Some(help) = value.get_help() {
                    found.push((
                        format!("{path} {}={}", arg.get_id(), value.get_name()),
                        Some(help.to_string()),
                    ));
                }
            }
        }
        for sub in command.get_subcommands() {
            summaries(sub, &path, found);
        }
    }

    #[test]
    fn every_summary_is_one_line_of_at_most_80_characters() {
        let mut found = Vec::new();
        summaries(&Cli::command(), "", &mut found);
        let faults: Vec<String> = found
            .into_iter()
            .filter_map(|(path, summary)| {
                let Some(summary) = summary else {
                    return Some(format!("{path}: no summary"));
                };
                let fault = summary.contains('\n')
                    || summary.chars().count() > 80
                    || summary.ends_with('.')
                    || summary.is_empty();
                fault.then(|| format!("{path}: {summary}"))
            })
            .collect();
        assert!(
            faults.is_empty(),
            "{} faults:\n{}",
            faults.len(),
            faults.join("\n")
        );
    }

    #[test]
    fn examples_fit_80_columns() {
        for (example, what) in EXAMPLES.iter().chain(MORE_EXAMPLES) {
            let line = if example.len() < EXAMPLE_COLUMN {
                2 + EXAMPLE_COLUMN + what.len()
            } else {
                (2 + example.len()).max(2 + EXAMPLE_COLUMN + what.len())
            };
            assert!(line <= 80, "{example}: {line} columns");
        }
    }

    #[test]
    fn the_short_list_fits_80_columns_unwrapped() {
        let mut command = root_command(ProgramName::Fleetring).term_width(80);
        let help = command.render_help().to_string();
        for line in help.lines() {
            assert!(line.chars().count() <= 80, "{line}");
        }
        let root = Cli::command();
        for (_, names) in GROUPS {
            for name in *names {
                let about = root
                    .find_subcommand(name)
                    .and_then(clap::Command::get_about)
                    .map(ToString::to_string)
                    .unwrap_or_default();
                assert!(
                    help.lines()
                        .any(|line| line.starts_with(&format!("  {name} "))
                            && line.ends_with(&about)),
                    "{name}'s line wraps or is missing:\n{help}"
                );
            }
        }
    }
}
