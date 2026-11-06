//! `fleetring db edit`: one command for every setting of a database, and the hints that point
//! the removed `db set-*` verbs at it (ADR-0032, ADR-0041).
//!
//! The metadata settings (description, labels, expiry) travel in one `UpdateDatabase` request.
//! Durability keeps its own call, so an edit that names both sends durability first, then the
//! metadata, and reports each outcome on its own line.

use std::error::Error;
use std::io::{Read, Write as _};

use clap::Args;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use fleetring_client::output::MetadataReport;
use fleetring_control::{
    Acks, DatabaseName, DescriptionChange, LabelKey, LabelValue, Labels, MetadataChange,
};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::data::{DurabilityChange, ExpiryRequest};
use fleetringd::report::{metadata_report, render_metadata};
use serde::Serialize;

use super::{
    ExpiryArgs, OutputArgs, OutputFormat, Target, description_arg, failed, parse_label_arg, shown,
    updated_failed,
};

/// What `db edit` takes: the database, the settings to change, and how to print the result.
#[derive(Debug, Args)]
pub(crate) struct EditArgs {
    /// The database's name.
    name: DatabaseName,
    #[command(flatten)]
    settings: Settings,
    /// Changes the metadata (description, labels, expiry) only if it is still at this version, as
    /// `db show` prints it; refused otherwise, so a value read first isn't overwritten by mistake.
    /// It guards the metadata update only: durability changes regardless.
    #[arg(long, value_name = "VERSION")]
    if_version: Option<u64>,
    #[command(flatten)]
    output: OutputArgs,
    #[command(flatten)]
    target: Target,
}

/// The settings `db edit` changes. A setting that isn't named stays as it is.
#[derive(Debug, Args)]
struct Settings {
    /// The description: what the database holds and how it was set up, at most 1 KiB of text;
    /// tab and line feed are allowed, other control and invisible characters aren't. `-` reads it
    /// from stdin, without its trailing line feeds. The control node records you as its author.
    #[arg(long, value_name = "TEXT|-", allow_hyphen_values = true)]
    description: Option<String>,
    /// Removes the description.
    #[arg(long, conflicts_with = "description")]
    clear_description: bool,
    /// A label to set, `key=value`, replacing the key's value if the database has it. Repeat it
    /// for several. Labels it doesn't name stay as they are.
    #[arg(long = "label", value_name = "KEY=VALUE", value_parser = parse_label_arg)]
    labels: Vec<(LabelKey, LabelValue)>,
    /// A label key to remove. Repeat it for several.
    #[arg(long = "remove-label", value_name = "KEY")]
    remove_labels: Vec<LabelKey>,
    #[command(flatten)]
    expiry: ExpiryArgs,
    /// Removes the expiry: the database stays until it is dropped.
    #[arg(long, conflicts_with_all = ["expires_in", "expires_at"])]
    clear_expiry: bool,
    /// The durability's acknowledgement mode: `quorum`, `all`, or `local`. It takes effect at the
    /// database's next epoch; the current primary keeps the settings it holds the lease under.
    #[arg(long)]
    acks: Option<Acks>,
    /// The floor on reachable replicas; the mode's write quorum if unset. Needs `--acks`.
    #[arg(long, requires = "acks")]
    min_replicas: Option<u8>,
}

/// What an edit asks the control plane to do.
#[derive(Debug)]
struct Plan {
    /// The durability change, which goes first.
    durability: Option<DurabilityRequest>,
    /// The metadata update, which goes second.
    metadata: Option<MetadataUpdate>,
}

#[derive(Debug, PartialEq, Eq)]
struct DurabilityRequest {
    acks: Acks,
    min_replicas: Option<u8>,
}

#[derive(Debug, PartialEq, Eq)]
struct MetadataUpdate {
    change: MetadataChange,
    expiry: ExpiryRequest,
}

/// The flags that change something, as the empty-edit message lists them.
const SETTING_FLAGS: &str = "--description, --clear-description, --label, --remove-label, \
                             --expires-in, --expires-at, --clear-expiry, --acks, or --min-replicas";

impl Settings {
    /// The requests the flags ask for, reading the description from `stdin` if it is `-`.
    ///
    /// # Errors
    ///
    /// A message if no setting is named, if `if_version` is given with no metadata setting to
    /// guard, or if a description or label is invalid.
    fn plan(self, if_version: Option<u64>, stdin: impl Read) -> Result<Plan, Box<dyn Error>> {
        let description = match (self.description, self.clear_description) {
            (Some(text), _) => DescriptionChange::Set(description_arg(&text, stdin)?),
            (None, true) => DescriptionChange::Clear,
            (None, false) => DescriptionChange::Keep,
        };
        let change = MetadataChange {
            description,
            set_labels: Labels::try_from_pairs(self.labels)?,
            remove_labels: self.remove_labels.into_iter().collect(),
            ..MetadataChange::default()
        };
        let expiry = if self.clear_expiry {
            ExpiryRequest::Clear
        } else {
            self.expiry
                .expiry()
                .map_or(ExpiryRequest::Keep, ExpiryRequest::from)
        };
        let metadata =
            (!change.is_empty() || !expiry.is_keep()).then_some(MetadataUpdate { change, expiry });
        let durability = self.acks.map(|acks| DurabilityRequest {
            acks,
            min_replicas: self.min_replicas,
        });
        if metadata.is_none() && durability.is_none() {
            return Err(format!("nothing to edit: pass at least one of {SETTING_FLAGS}").into());
        }
        if metadata.is_none() && if_version.is_some() {
            return Err(
                "--if-version guards the metadata update, and no metadata setting is given: \
                 pass --description, --clear-description, --label, --remove-label, \
                 --expires-in, --expires-at, or --clear-expiry"
                    .into(),
            );
        }
        Ok(Plan {
            durability,
            metadata,
        })
    }
}

/// What an edit did, as `--json` prints it.
#[derive(Debug, Serialize)]
struct EditReport {
    /// The database's name.
    database: String,
    /// The durability change, or `null` if the edit left it alone.
    durability: Option<DurabilityReport>,
    /// The metadata after the update, or `null` if the edit left it alone or the update failed.
    metadata: Option<MetadataReport>,
    /// Why the metadata update failed after the durability change went through, or `null`.
    metadata_error: Option<String>,
}

/// A durability change: the settings that take effect at the next epoch, and those in effect
/// until then.
#[derive(Debug, Serialize)]
struct DurabilityReport {
    acks: String,
    min_replicas: u8,
    current_acks: String,
    current_min_replicas: u8,
}

impl From<DurabilityChange> for DurabilityReport {
    fn from(change: DurabilityChange) -> Self {
        Self {
            acks: change.acks.to_string(),
            min_replicas: change.min_replicas,
            current_acks: change.current_acks.to_string(),
            current_min_replicas: change.current_min_replicas,
        }
    }
}

impl EditReport {
    /// The report as text output prints it: one line per outcome, then the metadata as `db show`
    /// prints it.
    fn text(&self, reader: Option<&str>) -> Result<String, Box<dyn Error>> {
        let mut out = Vec::new();
        if let Some(durability) = &self.durability {
            writeln!(
                out,
                "durability: acks={} min_replicas={} at its next epoch; now acks={} \
                 min_replicas={}",
                durability.acks,
                durability.min_replicas,
                durability.current_acks,
                durability.current_min_replicas
            )?;
        }
        if let Some(metadata) = &self.metadata {
            writeln!(out, "metadata: updated")?;
            render_metadata(Some(metadata), reader, &mut out)?;
        } else if self.metadata_error.is_some() {
            writeln!(out, "metadata: not changed")?;
        }
        Ok(String::from_utf8(out)?)
    }
}

/// Runs `db edit` and returns what it prints.
///
/// The description is read, and every flag checked, before anything is sent, so a bad flag
/// changes nothing. Durability is changed first, then the metadata.
///
/// # Errors
///
/// An empty edit, an invalid flag, or a failed call. If the durability change went through and
/// the metadata update failed, the outcome is printed and the error says that the durability
/// change was applied.
pub(crate) async fn run(args: EditArgs) -> Result<String, Box<dyn Error>> {
    let EditArgs {
        name,
        settings,
        if_version,
        output,
        target,
    } = args;
    let plan = settings.plan(if_version, std::io::stdin().lock())?;
    let client = target.client()?;
    let durability = match plan.durability {
        Some(request) => Some(
            client
                .change_durability(&name, request.acks, request.min_replicas)
                .await
                .map_err(failed)?,
        ),
        None => None,
    };
    let mut report = EditReport {
        database: name.to_string(),
        durability: durability.map(DurabilityReport::from),
        metadata: None,
        metadata_error: None,
    };
    if let Some(update) = plan.metadata {
        let updated = client
            .update_database_with(&name, &update.change, update.expiry, if_version)
            .await;
        match updated {
            Ok(view) => {
                report.metadata = Some(metadata_report(&view, SystemClock.now()));
            }
            Err(status) => {
                let error = updated_failed(status, if_version);
                if report.durability.is_none() {
                    return Err(error);
                }
                report.metadata_error = Some(error.to_string());
            }
        }
    }
    let printed = match output.format() {
        OutputFormat::Json => format!("{}\n", serde_json::to_string_pretty(&report)?),
        OutputFormat::Table => report.text(target.principal().as_deref())?,
    };
    match &report.metadata_error {
        Some(error) => {
            print!("{printed}");
            Err(format!(
                "the durability change of {} was applied, but the metadata update failed: {error}",
                shown(&name)
            )
            .into())
        }
        None => Ok(printed),
    }
}

/// The `hint:` line for a parse error about a removed `db set-*` verb, naming `db edit` and the
/// flags that replace it, or `None` for any other error.
///
/// The verbs failed like any unknown subcommand since `db edit` replaced them (ADR-0032: a clean
/// break before 1.0, with no alias).
pub(crate) fn removed_verb_hint(error: &clap::Error) -> Option<&'static str> {
    if error.kind() != ErrorKind::InvalidSubcommand {
        return None;
    }
    let Some(ContextValue::String(verb)) = error.get(ContextKind::InvalidSubcommand) else {
        return None;
    };
    match verb.as_str() {
        "set-description" => Some(
            "hint: use 'fleetring db edit <db> --description <text|->', or '--clear-description' \
             to remove it",
        ),
        "set-labels" => Some(
            "hint: use 'fleetring db edit <db> --label <key=value>' to set a label and \
             '--remove-label <key>' to remove one",
        ),
        "set-expiry" => Some(
            "hint: use 'fleetring db edit <db> --expires-in <duration>', '--expires-at <time>', \
             or '--clear-expiry'",
        ),
        "set-durability" => Some(
            "hint: use 'fleetring db edit <db> --acks <quorum|all|local>', with \
             '--min-replicas <n>' for the floor",
        ),
        _ => None,
    }
}

/// The `hint:` line for a parse error about an archive command spelled as 0.6 did, naming the
/// 0.7 spelling, or `None` for any other error (ADR-0043, point 2).
///
/// Like the removed `db set-*` verbs, the old spellings fail as any unknown command or flag does
/// (ADR-0032).
pub(crate) fn renamed_hint(error: &clap::Error) -> Option<&'static str> {
    let usage = match error.get(ContextKind::Usage) {
        Some(ContextValue::StyledStr(usage)) => usage.to_string(),
        _ => String::new(),
    };
    match error.kind() {
        // `fleetring db archive show|verify`.
        ErrorKind::InvalidSubcommand => {
            let Some(ContextValue::String(verb)) = error.get(ContextKind::InvalidSubcommand) else {
                return None;
            };
            (verb == "archive" && usage.contains(" db "))
                .then_some("hint: `db archive show|verify` is now `fleetring archive show|verify`")
        }
        // `fleetring archive restore --from <db> --to <dir>`.
        ErrorKind::UnknownArgument => {
            let Some(ContextValue::String(argument)) = error.get(ContextKind::InvalidArg) else {
                return None;
            };
            (argument.starts_with("--from") && usage.contains(" archive restore")).then_some(
                "hint: `--from <db>` is now the argument: `fleetring archive restore <db> --to <dir>`",
            )
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser as _;
    use fleetring_control::Timestamp;

    use super::*;
    use crate::{Cli, Command, DbCommand};

    const CONTROL: [&str; 2] = ["--control", "127.0.0.1:7400"];

    fn parse(flags: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from([&["fleetringd", "db", "edit", "notes"][..], flags, &CONTROL].concat())
    }

    fn edit(flags: &[&str]) -> EditArgs {
        let cli = parse(flags).expect("db edit parses");
        let Command::Db(DbCommand::Edit(args)) = cli.command else {
            panic!("not db edit: {:?}", cli.command);
        };
        args
    }

    fn plan(flags: &[&str], stdin: &str) -> Result<Plan, String> {
        let args = edit(flags);
        args.settings
            .plan(args.if_version, stdin.as_bytes())
            .map_err(|error| error.to_string())
    }

    #[test]
    fn every_setting_is_set_through_db_edit_alone() {
        let metadata =
            |flags: &[&str]| plan(flags, "").expect("a plan").metadata.expect("metadata");
        let described = metadata(&["--description", "Memory."]);
        assert!(matches!(
            described.change.description,
            DescriptionChange::Set(_)
        ));
        assert_eq!(described.expiry, ExpiryRequest::Keep);
        assert_eq!(
            metadata(&["--clear-description"]).change.description,
            DescriptionChange::Clear
        );
        let labelled = metadata(&["--label", "task=7", "--label", "agent=42"]);
        assert_eq!(labelled.change.set_labels.len(), 2);
        let unlabelled = metadata(&["--remove-label", "agent", "--remove-label", "task"]);
        assert_eq!(unlabelled.change.remove_labels.len(), 2);
        assert_eq!(
            metadata(&["--expires-in", "48h"]).expiry,
            ExpiryRequest::In(Duration::from_secs(48 * 3_600))
        );
        assert_eq!(
            metadata(&["--expires-at", "2026-11-05T10:00:00Z"]).expiry,
            ExpiryRequest::At(Timestamp::from_millis(1_793_872_800_000))
        );
        assert_eq!(metadata(&["--clear-expiry"]).expiry, ExpiryRequest::Clear);
        for flags in [
            &["--description", "x"][..],
            &["--label", "a=b"][..],
            &["--expires-in", "1h"][..],
        ] {
            assert!(plan(flags, "").expect("a plan").durability.is_none());
        }
        let durable = plan(&["--acks", "all"], "").expect("a plan");
        assert!(durable.metadata.is_none());
        assert_eq!(
            durable.durability,
            Some(DurabilityRequest {
                acks: Acks::All,
                min_replicas: None
            })
        );
        let floor = plan(&["--acks", "quorum", "--min-replicas", "2"], "").expect("a plan");
        assert_eq!(
            floor.durability,
            Some(DurabilityRequest {
                acks: Acks::Quorum,
                min_replicas: Some(2)
            })
        );
    }

    #[test]
    fn settings_combine_in_one_metadata_update_and_one_durability_change() {
        let combined = plan(
            &[
                "--description",
                "Memory.",
                "--label",
                "task=7",
                "--remove-label",
                "agent",
                "--expires-in",
                "24h",
                "--acks",
                "local",
                "--if-version",
                "3",
            ],
            "",
        )
        .expect("a plan");
        let update = combined.metadata.expect("metadata");
        assert!(matches!(
            update.change.description,
            DescriptionChange::Set(_)
        ));
        assert_eq!(update.change.set_labels.len(), 1);
        assert_eq!(update.change.remove_labels.len(), 1);
        assert_eq!(
            update.expiry,
            ExpiryRequest::In(Duration::from_secs(86_400))
        );
        assert_eq!(combined.durability.expect("durability").acks, Acks::Local);
    }

    #[test]
    fn a_description_of_a_dash_is_read_from_stdin() {
        let update = plan(&["--description", "-"], "From stdin.\n\n")
            .expect("a plan")
            .metadata
            .expect("metadata");
        let DescriptionChange::Set(text) = update.change.description else {
            panic!("not a description: {:?}", update.change.description);
        };
        assert_eq!(text.as_str(), "From stdin.");
    }

    #[test]
    fn an_empty_edit_is_refused_with_the_flags_that_change_something() {
        let error = plan(&[], "").expect_err("nothing to edit");
        assert!(
            error.starts_with("nothing to edit: pass at least one of --description"),
            "{error}"
        );
        assert!(error.contains("--acks"), "{error}");
        let error = plan(&["--if-version", "2"], "").expect_err("a version alone changes nothing");
        assert!(error.starts_with("nothing to edit"), "{error}");
        // A version next to a durability change alone guards nothing.
        let error = plan(&["--acks", "all", "--if-version", "2"], "").expect_err("guards nothing");
        assert!(
            error.contains("--if-version guards the metadata update"),
            "{error}"
        );
    }

    #[test]
    fn contradicting_or_incomplete_flags_fail_at_parse() {
        for wrong in [
            &["--description", "x", "--clear-description"][..],
            &["--expires-in", "1h", "--clear-expiry"][..],
            &["--expires-at", "2026-11-05T10:00:00Z", "--clear-expiry"][..],
            &["--expires-in", "1h", "--expires-at", "2026-11-05T10:00:00Z"][..],
            &["--min-replicas", "2"][..],
            &["--acks", "sometimes"][..],
            &["--expires-in", "soon"][..],
            &["--expires-at", "tomorrow"][..],
            &["--label", "no-equals"][..],
        ] {
            assert!(parse(wrong).is_err(), "{wrong:?}");
        }
    }

    #[test]
    fn each_removed_verb_fails_at_parse_with_a_hint_naming_db_edit() {
        for (verb, flags) in [
            ("set-description", "--description"),
            ("set-labels", "--remove-label"),
            ("set-expiry", "--clear-expiry"),
            ("set-durability", "--acks"),
        ] {
            let error = Cli::try_parse_from([
                "fleetringd",
                "db",
                verb,
                "notes",
                "--control",
                "127.0.0.1:7400",
            ])
            .expect_err("the verb is removed");
            let hint = removed_verb_hint(&error).expect("a hint");
            assert!(
                hint.starts_with("hint: use 'fleetring db edit <db> "),
                "{hint}"
            );
            assert!(hint.contains(flags), "{verb}: {hint}");
            assert_eq!(hint.lines().count(), 1, "{hint}");
        }
        let other = Cli::try_parse_from(["fleetringd", "db", "frobnicate"]).expect_err("unknown");
        assert_eq!(removed_verb_hint(&other), None);
    }

    #[test]
    fn the_old_archive_spellings_get_a_hint_naming_the_new_one() {
        for (words, hint) in [
            (
                &["fleetring", "db", "archive", "show", "notes"][..],
                "`fleetring archive show|verify`",
            ),
            (
                &[
                    "fleetring",
                    "archive",
                    "restore",
                    "--from",
                    "notes",
                    "--to",
                    "out",
                ],
                "`fleetring archive restore <db> --to <dir>`",
            ),
        ] {
            let error = Cli::try_parse_from(words).expect_err("the spelling is removed");
            let found = renamed_hint(&error).expect("a hint");
            assert!(found.starts_with("hint: "), "{found}");
            assert!(found.contains(hint), "{words:?}: {found}");
            assert_eq!(found.lines().count(), 1, "{found}");
        }
        // Other mistakes get none, and the new spellings parse.
        for words in [
            &["fleetring", "db", "frobnicate"][..],
            &["fleetring", "archive", "restore", "--bogus"],
            &["fleetring", "node", "archive"],
        ] {
            let error = Cli::try_parse_from(words).expect_err("a usage error");
            assert_eq!(renamed_hint(&error), None, "{words:?}");
        }
        for words in [
            &["fleetring", "archive", "show", "notes", "--archive", "a"][..],
            &["fleetring", "archive", "verify", "notes", "--archive", "a"],
            &[
                "fleetring",
                "archive",
                "restore",
                "notes",
                "--config",
                "c",
                "--archive",
                "a",
                "--to",
                "out",
            ],
            &["fleetring", "node", "list", "--json"],
        ] {
            Cli::try_parse_from(words).expect("the new spelling parses");
        }
    }

    fn report(metadata: Option<MetadataReport>, error: Option<&str>) -> EditReport {
        EditReport {
            database: "notes".to_owned(),
            durability: Some(DurabilityReport {
                acks: "local".to_owned(),
                min_replicas: 1,
                current_acks: "all".to_owned(),
                current_min_replicas: 3,
            }),
            metadata,
            metadata_error: error.map(str::to_owned),
        }
    }

    #[test]
    fn text_output_has_a_line_per_outcome() {
        let text = report(None, None).text(None).expect("text");
        assert_eq!(
            text,
            "durability: acks=local min_replicas=1 at its next epoch; now acks=all \
             min_replicas=3\n"
        );
        let failed = report(None, Some("stale")).text(None).expect("text");
        assert_eq!(
            failed,
            "durability: acks=local min_replicas=1 at its next epoch; now acks=all \
             min_replicas=3\nmetadata: not changed\n"
        );
    }
}
