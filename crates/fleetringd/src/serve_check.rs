//! `fleetring serve --check`: whether a node would start with a configuration and its flags,
//! without starting anything (#95).
//!
//! It runs what `serve` runs before the node starts, in the same order: [`Config::load`], the
//! flags, and [`Daemon::check`]. It reads no file the configuration names, no environment
//! variable, and no data directory, and binds nothing.

use std::error::Error;
use std::fs;
use std::ops::Range;
use std::path::Path;
use std::process::ExitCode;

use fleetring_client::output::{ConfigCheckReport, ConfigProblem};
use fleetringd::{Config, ConfigError, Daemon, NodeRole, StartError};

use crate::DaemonArgs;
use crate::format::{self, OutputFormat};
use crate::progress::MachineOutput;

/// Checks `args` as `serve` would start with them, prints the result as `format` says, and fails
/// with the problem, naming the file and the field or the line, if the node wouldn't start.
///
/// # Errors
///
/// The problem, which exits 1, or an output error.
pub(crate) fn run(args: DaemonArgs, format: OutputFormat) -> Result<ExitCode, Box<dyn Error>> {
    let path = args.config.clone();
    let (config, problem) = match Config::load(&path) {
        Err(error) => (None, Some(config_problem(&error, &path))),
        Ok(config) => {
            let config = args.apply(config);
            let problem = Daemon::check(&config)
                .err()
                .map(|error| problem_of_start(&error, &path));
            (Some(config), problem)
        }
    };
    let report = ConfigCheckReport {
        config: path.display().to_string(),
        valid: problem.is_none(),
        node: config.as_ref().map(|config| config.node.id.to_string()),
        role: config
            .as_ref()
            .map(|config| role_name(config.role).to_owned()),
        problems: problem.into_iter().collect(),
    };
    if format.machine() == MachineOutput::Yes || report.valid {
        crate::emit(&format::render(format, &report, |report| Ok(text(report)))?)?;
    }
    match report.problems.first() {
        None => Ok(ExitCode::SUCCESS),
        Some(problem) => Err(failure(&report.config, problem).into()),
    }
}

/// What text output prints for a configuration the node would start with.
fn text(report: &ConfigCheckReport) -> String {
    format!(
        "{}: valid for node {}, role {}\n",
        report.config,
        report.node.as_deref().unwrap_or("?"),
        report.role.as_deref().unwrap_or("?")
    )
}

/// The error a failed check ends with: `<file>[:<line>:<column>]: [field <field>: ]<what is
/// wrong>`, the place in the form editors and compilers use.
fn failure(config: &str, problem: &ConfigProblem) -> String {
    let place = match (problem.line, problem.column) {
        (Some(line), Some(column)) => format!(":{line}:{column}"),
        (Some(line), None) => format!(":{line}"),
        (None, _) => String::new(),
    };
    let field = problem
        .field
        .as_ref()
        .map_or_else(String::new, |field| format!("field {field}: "));
    format!("{config}{place}: {field}{}", problem.message)
}

/// `error` from [`Daemon::check`] of the file at `path` as a problem.
fn problem_of_start(error: &StartError, path: &Path) -> ConfigProblem {
    match error {
        StartError::Config(error) => config_problem(error, path),
        StartError::Engine(error) => ConfigProblem {
            field: Some("engine.page_size".to_owned()),
            line: None,
            column: None,
            message: error.to_string(),
        },
        other => ConfigProblem {
            field: None,
            line: None,
            column: None,
            message: one_line(other),
        },
    }
}

/// A [`ConfigError`] as a problem. A parse error is placed by its line and column in the file at
/// `path`, which is read again for them: the parser gives a byte offset.
fn config_problem(error: &ConfigError, path: &Path) -> ConfigProblem {
    let problem = |field: Option<&str>, message: String| ConfigProblem {
        field: field.map(str::to_owned),
        line: None,
        column: None,
        message,
    };
    match error {
        ConfigError::Read { source, .. } => problem(None, format!("can't read it: {source}")),
        ConfigError::Parse(parse) => {
            parse_error_problem(parse, fs::read_to_string(path).ok().as_deref())
        }
        ConfigError::Missing { field, reason } => {
            problem(Some(field), format!("is required: {reason}"))
        }
        ConfigError::Invalid { field, reason } => problem(Some(field), reason.clone()),
        other => problem(None, one_line(other)),
    }
}

/// A parse error of `text`, the file's contents if they could be read again, as a problem: placed
/// by line and column, and naming the dotted field it is in, if the parser places it; and naming
/// the field it lacks, if one is missing.
fn parse_error_problem(error: &toml::de::Error, text: Option<&str>) -> ConfigProblem {
    let message = error.message().trim().replace('\n', " ");
    // An empty span, as for a field missing from the top, places the error nowhere.
    let span = error.span().filter(|span| !span.is_empty());
    let place = span
        .clone()
        .zip(text)
        .and_then(|(span, text)| line_and_column(text, &span));
    let at = span
        .zip(text)
        .and_then(|(span, text)| field_at(text, &span));
    let missing = message
        .strip_prefix("missing field `")
        .and_then(|rest| rest.split_once('`'))
        .map(|(name, _)| name);
    let field = match (at, missing) {
        (Some(table), Some(name)) => Some(format!("{table}.{name}")),
        (None, Some(name)) => Some(name.to_owned()),
        (at, None) => at,
    };
    ConfigProblem {
        field,
        line: place.map(|(line, _)| line),
        column: place.map(|(_, column)| column),
        message,
    }
}

/// The dotted path of the key in `text` that `span` touches: the innermost one whose key, or
/// whose value if it isn't a table, overlaps it. A table's header counts as its key.
fn field_at(text: &str, span: &Range<usize>) -> Option<String> {
    let document = toml::de::DeTable::parse(text).ok()?;
    let mut found: Option<(usize, String)> = None;
    visit(document.get_ref(), None, span, &mut found);
    found.map(|(_, path)| path)
}

/// Walks `table`, whose path is `prefix`, for [`field_at`], keeping in `found` the key that
/// overlaps `span` with the narrowest extent, and that extent.
fn visit(
    table: &toml::de::DeTable<'_>,
    prefix: Option<&str>,
    span: &Range<usize>,
    found: &mut Option<(usize, String)>,
) {
    let overlaps = |other: &Range<usize>| other.start < span.end && span.start < other.end;
    for (key, value) in table {
        let path = match prefix {
            Some(prefix) => format!("{prefix}.{}", key.get_ref()),
            None => key.get_ref().to_string(),
        };
        let leaf = !matches!(
            value.get_ref(),
            toml::de::DeValue::Table(_) | toml::de::DeValue::Array(_)
        );
        let touched = overlaps(&key.span()) || (leaf && overlaps(&value.span()));
        let extent =
            value.span().end.max(key.span().end) - key.span().start.min(value.span().start);
        if touched
            && found
                .as_ref()
                .is_none_or(|(narrowest, _)| extent <= *narrowest)
        {
            *found = Some((extent, path.clone()));
        }
        match value.get_ref() {
            toml::de::DeValue::Table(inner) => visit(inner, Some(&path), span, found),
            toml::de::DeValue::Array(items) => {
                for item in items {
                    if let toml::de::DeValue::Table(inner) = item.get_ref() {
                        visit(inner, Some(&path), span, found);
                    }
                }
            }
            _ => {}
        }
    }
}

/// `error` and its causes on one line.
fn one_line(error: &dyn Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text.replace('\n', " ")
}

/// The line and column, each from 1, at which `span` starts in `text`; the column counts
/// characters.
fn line_and_column(text: &str, span: &Range<usize>) -> Option<(u64, u64)> {
    let before = text.get(..span.start)?;
    let line = before.matches('\n').count() + 1;
    let column = before
        .rsplit_once('\n')
        .map_or(before, |(_, last)| last)
        .chars()
        .count()
        + 1;
    Some((u64::try_from(line).ok()?, u64::try_from(column).ok()?))
}

/// The role as the configuration spells it.
fn role_name(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Both => "both",
        NodeRole::Data => "data",
        NodeRole::Control => "control",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The problem `text`'s parse error is, as the check reports it.
    fn parse_problem(text: &str) -> ConfigProblem {
        let Err(ConfigError::Parse(error)) = Config::from_toml(text) else {
            panic!("no parse error in {text:?}");
        };
        parse_error_problem(&error, Some(text))
    }

    #[test]
    fn a_parse_error_names_the_dotted_field_and_where_it_is() {
        let head = "data_dir = \"x\"\nlisten = \"127.0.0.1:1\"\n";
        for (tail, field, line) in [
            ("[node]\nid = \"n\"\n[admin]\nport = 1\n", "admin.port", 6),
            (
                "[node]\nid = \"n\"\n[replication]\nreplication_factor = 0\n",
                "replication.replication_factor",
                6,
            ),
            (
                "[node]\nid = \"n\"\n[[changes.sink]]\nname = \"a\"\nkind = \"nope\"\n",
                "changes.sink.kind",
                7,
            ),
            // A missing field names the table that lacks it, and the field.
            ("[node]\nweight = 1\n", "node.id", 3),
        ] {
            let problem = parse_problem(&format!("{head}{tail}"));
            assert_eq!(problem.field.as_deref(), Some(field), "{tail}: {problem:?}");
            assert_eq!(problem.line, Some(line), "{tail}: {problem:?}");
            assert!(!problem.message.contains('\n'), "{problem:?}");
        }
        // A field missing at the top is in no one place.
        let problem = parse_problem("data_dir = \"x\"\n[node]\nid = \"n\"\n");
        assert_eq!(problem.field.as_deref(), Some("listen"));
        assert_eq!((problem.line, problem.column), (None, None));
        assert_eq!(problem.message, "missing field `listen`");
    }

    #[test]
    fn a_span_is_placed_by_line_and_character_column() {
        let text = "a = 1\nbé = 2\n[x]\n";
        assert_eq!(line_and_column(text, &(0..1)), Some((1, 1)));
        assert_eq!(line_and_column(text, &(6..7)), Some((2, 1)));
        // `é` is two bytes and one column.
        let equals = text.find("= 2").expect("in the text");
        assert_eq!(line_and_column(text, &(equals..equals + 1)), Some((2, 4)));
        assert_eq!(line_and_column(text, &(100..101)), None);
    }

    #[test]
    fn a_failure_names_the_file_and_the_field_or_the_line() {
        let problem = |field: Option<&str>, line: Option<u64>| ConfigProblem {
            field: field.map(str::to_owned),
            line,
            column: line,
            message: "what is wrong".to_owned(),
        };
        assert_eq!(
            failure("node.toml", &problem(Some("admin.listen"), None)),
            "node.toml: field admin.listen: what is wrong"
        );
        assert_eq!(
            failure("node.toml", &problem(None, Some(3))),
            "node.toml:3:3: what is wrong"
        );
        assert_eq!(
            failure("node.toml", &problem(Some("admin.port"), Some(6))),
            "node.toml:6:6: field admin.port: what is wrong"
        );
        assert_eq!(
            failure("node.toml", &problem(None, None)),
            "node.toml: what is wrong"
        );
    }
}
