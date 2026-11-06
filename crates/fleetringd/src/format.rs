//! `-o table|json|jsonl` and `--json` (`docs/cli.md`, section 5).
//!
//! A read command prints for people by default. `-o json` prints one JSON object, indented;
//! `-o jsonl` prints the same object on one line; `--json` is short for `-o json`. A command that
//! follows a stream (`lease watch`) prints one object per line, flushed as each arrives, for
//! `json` and `jsonl` alike. The objects are [`fleetring_client::output`]'s types, whose fields
//! are stable.

use std::error::Error;

use clap::Args;
use serde::Serialize;

use crate::progress::MachineOutput;

/// How a command prints its result: `-o table|json|jsonl`, or `--json`.
#[derive(Debug, Args)]
pub(crate) struct OutputArgs {
    /// The output format: `table`, `json`, or `jsonl`
    ///
    /// `table` is for people; `json` is one object with stable snake_case fields; `jsonl` is the
    /// same object on one line. A watch prints one object per line for either.
    #[arg(short = 'o', long, value_enum, default_value_t = OutputFormat::Table)]
    output: OutputFormat,
    /// Short for `-o json`.
    #[arg(long, conflicts_with = "output")]
    json: bool,
}

impl OutputArgs {
    /// The format the flags ask for.
    pub(crate) fn format(&self) -> OutputFormat {
        if self.json {
            OutputFormat::Json
        } else {
            self.output
        }
    }
}

/// An output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum OutputFormat {
    /// For people: a table, or key and value lines for one item.
    Table,
    /// For programs: one JSON object, indented.
    Json,
    /// For programs: one JSON object per line.
    Jsonl,
}

impl OutputFormat {
    /// Whether the output is for programs, which keeps spinners off.
    pub(crate) fn machine(self) -> MachineOutput {
        match self {
            Self::Table => MachineOutput::No,
            Self::Json | Self::Jsonl => MachineOutput::Yes,
        }
    }
}

/// `value` as `format` prints it, with its line feed: indented for `json`, on one line for
/// `jsonl`, and `text()` for `table`.
///
/// # Errors
///
/// What `text` returns, or a serialization error.
pub(crate) fn render<T: Serialize>(
    format: OutputFormat,
    value: &T,
    text: impl FnOnce(&T) -> Result<String, Box<dyn Error>>,
) -> Result<String, Box<dyn Error>> {
    match format {
        OutputFormat::Table => text(value),
        OutputFormat::Json => Ok(format!("{}\n", serde_json::to_string_pretty(value)?)),
        OutputFormat::Jsonl => line(value),
    }
}

/// `value` as one line of JSON, with its line feed.
///
/// # Errors
///
/// A serialization error.
pub(crate) fn line<T: Serialize>(value: &T) -> Result<String, Box<dyn Error>> {
    Ok(format!("{}\n", serde_json::to_string(value)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_is_indented_jsonl_one_line_and_table_text() -> Result<(), Box<dyn Error>> {
        let value = serde_json::json!({ "databases": [{ "database": "notes" }] });
        let text = |_: &serde_json::Value| Ok("NAME\nnotes\n".to_owned());
        assert_eq!(render(OutputFormat::Table, &value, text)?, "NAME\nnotes\n");
        let json = render(OutputFormat::Json, &value, text)?;
        assert!(json.lines().count() > 1, "{json}");
        assert_eq!(serde_json::from_str::<serde_json::Value>(&json)?, value);
        assert_eq!(
            render(OutputFormat::Jsonl, &value, text)?,
            "{\"databases\":[{\"database\":\"notes\"}]}\n"
        );
        Ok(())
    }
}
