//! `fleetring context …` and `fleetring switch` (ADR-0043, point 4), and the `dev` context dev mode
//! writes (point 5.1).

use std::error::Error;
use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, Subcommand};
use dialoguer::FuzzySelect;
use dialoguer::theme::{ColorfulTheme, SimpleTheme, Theme};
use fleetring_client::context::{
    Context, ContextError, ContextName, ContextStore, Contexts, DEV_NAME, DevContextOutcome,
    WasCurrent,
};
use fleetring_client::dirs;
use fleetringd::dev::{self, DevContextLine, Prepared};
use fleetringd::tls::{Identity, Principal};

use crate::exit::{INTERRUPTED, USAGE};
use crate::format::{self, OutputArgs, OutputFormat};
use crate::progress::{MachineOutput, Quiet, Surroundings};
use crate::{color, note, program};

/// `fleetring context`'s commands.
#[derive(Debug, Subcommand)]
pub(crate) enum ContextCommand {
    /// Lists the contexts, marking the current one.
    List {
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Shows one context: the current one unless a name is given.
    Show {
        /// The context to show.
        name: Option<ContextName>,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Prints the current context's name and nothing else, for a shell prompt
    ///
    /// Prints nothing and exits 1 if no context is current.
    Current,
    /// Adds a context: a name for a fleet's control addresses and identity
    ///
    /// The identity is its certificates and principal. The first context added becomes current.
    Add(ContextAddArgs),
    /// Makes a context current
    ///
    /// It remembers the one it replaces for `fleetring switch -`.
    Use {
        /// The context to use.
        name: ContextName,
    },
    /// Removes a context, but never the files it names
    ///
    /// It deletes only the context's entry.
    Remove {
        /// The context to remove.
        name: ContextName,
    },
}

/// `fleetring context add`'s arguments.
#[derive(Debug, Args)]
pub(crate) struct ContextAddArgs {
    /// The new context's name
    ///
    /// 1 to 63 lowercase letters, digits, '-', '_' or '.'.
    name: ContextName,
    /// A control node's address, `host:port`
    ///
    /// Repeat it for more; they are tried in order.
    #[arg(long, required = true)]
    control: Vec<String>,
    /// The fleet's name
    ///
    /// Defaults to the fleet the client certificate names.
    #[arg(long)]
    fleet: Option<String>,
    /// A directory with `ca.crt` and the client certificates `cert client` writes
    #[arg(long, conflicts_with_all = ["ca", "cert", "key"])]
    tls_dir: Option<PathBuf>,
    /// The trusted authority certificate, PEM.
    #[arg(long, requires_all = ["cert", "key"])]
    ca: Option<PathBuf>,
    /// The client certificate, PEM.
    #[arg(long)]
    cert: Option<PathBuf>,
    /// The client certificate's private key, PEM.
    #[arg(long)]
    key: Option<PathBuf>,
    /// The principal to act as
    ///
    /// With `--tls-dir`, `client.<principal>.crt` is the certificate.
    #[arg(long = "as", value_name = "PRINCIPAL")]
    principal: Option<Principal>,
    /// A file that holds a bearer token
    ///
    /// The context stores the path, never the token.
    #[arg(long)]
    token_file: Option<PathBuf>,
}

/// The store in the configuration directory.
///
/// # Errors
///
/// [`dirs::DirsError`] if there is no home directory.
pub(crate) fn store() -> Result<ContextStore, dirs::DirsError> {
    let dir = dirs::config_dir(&|name| std::env::var_os(name))?;
    Ok(ContextStore::at(&dir))
}

/// Runs a `context` command.
pub(crate) fn run(command: ContextCommand) -> Result<ExitCode, Box<dyn Error>> {
    let store = store()?;
    match command {
        ContextCommand::List { output } => {
            let contexts = store.load()?;
            crate::emit(&render_list(&contexts, output.format())?)?;
        }
        ContextCommand::Show { name, output } => {
            let contexts = store.load()?;
            let (name, context, current) = match name {
                Some(name) => {
                    let context =
                        contexts
                            .contexts
                            .get(&name)
                            .ok_or_else(|| ContextError::Unknown {
                                name: name.to_string(),
                                path: store.path().to_owned(),
                            })?;
                    let current = contexts.current.as_ref() == Some(&name);
                    (name, context, current)
                }
                None => {
                    let (name, context) = contexts.current().ok_or_else(|| {
                        format!(
                            "no context is current\n  hint: pick one with `{} switch`, or add \
                             one with `{} context add`",
                            program(),
                            program()
                        )
                    })?;
                    (name.clone(), context, true)
                }
            };
            crate::emit(&render_one(&name, context, current, output.format())?)?;
        }
        ContextCommand::Current => {
            let contexts = store.load()?;
            return Ok(match contexts.current() {
                Some((name, _)) => {
                    println!("{name}");
                    ExitCode::SUCCESS
                }
                None => ExitCode::FAILURE,
            });
        }
        ContextCommand::Add(args) => add(&store, args)?,
        ContextCommand::Use { name } => use_context(&store, &name)?,
        ContextCommand::Remove { name } => {
            let was = store.update(|contexts| contexts.remove(&name))?;
            note(format!("removed context '{name}'"));
            if was == WasCurrent::Yes {
                note(format!(
                    "no context is current now; pick one with `{} switch`",
                    program()
                ));
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// Runs `fleetring switch [<name>|-]`: `-` goes back to the previous context, a name makes that
/// context current, and no name opens a picker on a terminal.
pub(crate) fn switch(target: Option<&str>) -> Result<ExitCode, Box<dyn Error>> {
    let store = store()?;
    match target {
        Some("-") => {
            let (name, fleet) = store.update(|contexts| {
                let name = contexts.use_previous()?;
                let fleet = contexts.contexts.get(&name).and_then(|c| c.fleet.clone());
                Ok((name, fleet))
            })?;
            announce_switch(&name, fleet.as_deref());
            Ok(ExitCode::SUCCESS)
        }
        Some(text) => match text.parse::<ContextName>() {
            Ok(name) => {
                use_context(&store, &name)?;
                Ok(ExitCode::SUCCESS)
            }
            Err(error) => {
                eprintln!("{}: {error}", program());
                Ok(ExitCode::from(USAGE))
            }
        },
        None => pick(&store),
    }
}

fn use_context(store: &ContextStore, name: &ContextName) -> Result<(), Box<dyn Error>> {
    let fleet = store.update(|contexts| {
        contexts.use_context(name)?;
        Ok(contexts.contexts.get(name).and_then(|c| c.fleet.clone()))
    })?;
    announce_switch(name, fleet.as_deref());
    Ok(())
}

fn announce_switch(name: &ContextName, fleet: Option<&str>) {
    match fleet {
        Some(fleet) => note(format!("switched to context '{name}' (fleet '{fleet}')")),
        None => note(format!("switched to context '{name}'")),
    }
}

/// `switch` with no name: a picker on a terminal, else the list and a usage error.
fn pick(store: &ContextStore) -> Result<ExitCode, Box<dyn Error>> {
    let contexts = store.load()?;
    let terminal = std::io::stdin().is_terminal() && std::io::stderr().is_terminal();
    if !terminal {
        eprint!("{}", render_list(&contexts, OutputFormat::Table)?);
        eprintln!(
            "{}: switch needs a context name when there is no terminal\n  hint: pass a name: \
             `{} switch <name>`",
            program(),
            program()
        );
        return Ok(ExitCode::from(USAGE));
    }
    if contexts.contexts.is_empty() {
        return Err(format!(
            "there are no contexts to switch to\n  hint: add one with `{} context add <name> \
             --control <host:port> …`, or start a local node with `{} dev`",
            program(),
            program()
        )
        .into());
    }
    let names: Vec<&ContextName> = contexts.contexts.keys().collect();
    let items: Vec<String> = contexts
        .contexts
        .iter()
        .map(|(name, context)| {
            let marker = if contexts.current.as_ref() == Some(name) {
                "*"
            } else {
                " "
            };
            match &context.fleet {
                Some(fleet) => format!("{marker} {name} ({fleet})"),
                None => format!("{marker} {name}"),
            }
        })
        .collect();
    let default = contexts
        .current
        .as_ref()
        .and_then(|current| names.iter().position(|name| *name == current))
        .unwrap_or(0);
    let coloured = Surroundings::capture(MachineOutput::No, Quiet::No).colour(color());
    let colourful = ColorfulTheme::default();
    let plain = SimpleTheme;
    let theme: &dyn Theme = if coloured { &colourful } else { &plain };
    let chosen = FuzzySelect::with_theme(theme)
        .with_prompt("switch to context")
        .items(&items)
        .default(default)
        .interact_opt()?;
    let Some(chosen) = chosen else {
        return Ok(ExitCode::from(INTERRUPTED));
    };
    let Some(name) = names.get(chosen).map(|name| (*name).clone()) else {
        return Err("the picker returned a context that doesn't exist".into());
    };
    use_context(store, &name)?;
    Ok(ExitCode::SUCCESS)
}

/// `context list`'s output.
fn render_list(contexts: &Contexts, format: OutputFormat) -> Result<String, Box<dyn Error>> {
    let list: Vec<serde_json::Value> = contexts
        .contexts
        .iter()
        .map(|(name, context)| context_json(name, context, contexts.current.as_ref() == Some(name)))
        .collect();
    format::render(format, &serde_json::json!({ "contexts": list }), |_| {
        let mut rows = vec![["CURRENT", "NAME", "FLEET", "CONTROL", "AS"].map(str::to_owned)];
        for (name, context) in &contexts.contexts {
            let current = contexts.current.as_ref() == Some(name);
            rows.push([
                if current { "*" } else { "" }.to_owned(),
                name.to_string(),
                context.fleet.clone().unwrap_or_default(),
                context.control.join(","),
                context.principal.clone().unwrap_or_default(),
            ]);
        }
        Ok(table(&rows))
    })
}

/// `context show`'s output.
fn render_one(
    name: &ContextName,
    context: &Context,
    current: bool,
    format: OutputFormat,
) -> Result<String, Box<dyn Error>> {
    format::render(format, &context_json(name, context, current), |_| {
        let mut text = format!(
            "name: {name}\ncurrent: {}\n",
            if current { "yes" } else { "no" }
        );
        let mut line = |key: &str, value: Option<String>| {
            if let Some(value) = value {
                text.push_str(&format!("{key}: {value}\n"));
            }
        };
        let path = |path: &Option<PathBuf>| path.as_ref().map(|path| path.display().to_string());
        line("fleet", context.fleet.clone());
        line(
            "control",
            (!context.control.is_empty()).then(|| context.control.join(", ")),
        );
        line("tls dir", path(&context.tls_dir));
        line("ca", path(&context.ca));
        line("cert", path(&context.cert));
        line("key", path(&context.key));
        line("as", context.principal.clone());
        line("token file", path(&context.token_file));
        line("dev dir", path(&context.dev_dir));
        line("agents dir", path(&context.agents_dir));
        Ok(text)
    })
}

/// A context as JSON: the fields of `context list`, with absent ones `null`.
fn context_json(name: &ContextName, context: &Context, current: bool) -> serde_json::Value {
    let path = |path: &Option<PathBuf>| path.as_ref().map(|path| path.display().to_string());
    serde_json::json!({
        "name": name.as_str(),
        "current": current,
        "fleet": context.fleet,
        "control": context.control,
        "tls_dir": path(&context.tls_dir),
        "ca": path(&context.ca),
        "cert": path(&context.cert),
        "key": path(&context.key),
        "as": context.principal,
        "token_file": path(&context.token_file),
    })
}

/// Rows as columns padded to their widest cell, the last column unpadded.
fn table(rows: &[[String; 5]]) -> String {
    let mut widths = [0usize; 5];
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let mut text = String::new();
    for row in rows {
        let last = row.len() - 1;
        for (index, (cell, width)) in row.iter().zip(widths).enumerate() {
            text.push_str(cell);
            if index < last {
                text.push_str(&" ".repeat(width.saturating_sub(cell.chars().count()) + 2));
            }
        }
        let trimmed = text.trim_end().len();
        text.truncate(trimmed);
        text.push('\n');
    }
    text
}

/// `context add`.
fn add(store: &ContextStore, args: ContextAddArgs) -> Result<(), Box<dyn Error>> {
    let absolute = |path: Option<PathBuf>| -> Result<Option<PathBuf>, std::io::Error> {
        path.map(std::path::absolute).transpose()
    };
    let mut context = Context {
        fleet: args.fleet,
        control: args.control,
        tls_dir: absolute(args.tls_dir)?,
        ca: absolute(args.ca)?,
        cert: absolute(args.cert)?,
        key: absolute(args.key)?,
        principal: args.principal.map(|principal| principal.to_string()),
        token_file: absolute(args.token_file)?,
        dev_dir: None,
        agents_dir: None,
    };
    if context.fleet.is_none() {
        context.fleet = Some(fleet_of_certificate(&context)?);
    }
    let name = args.name;
    let was_current = store
        .update(|contexts| {
            contexts.add(name.clone(), context)?;
            Ok(contexts.current.as_ref() == Some(&name))
        })
        .map_err(|error| match error {
            ContextError::Exists { name } => format!(
                "context '{name}' already exists\n  hint: remove it first with `{} context \
                 remove {name}`",
                program()
            )
            .into(),
            other => Box::new(other) as Box<dyn Error>,
        })?;
    if was_current {
        note(format!("added context '{name}', now current"));
    } else {
        note(format!("added context '{name}'"));
    }
    Ok(())
}

/// The fleet the certificate `context` would use names.
fn fleet_of_certificate(context: &Context) -> Result<String, Box<dyn Error>> {
    let hint = "\n  hint: pass --fleet <name>, or point --tls-dir or --cert at a certificate";
    let dir = context.tls_dir.as_deref();
    let in_dir = |name: String| dir.map(|dir| dir.join(name));
    let principal = context.principal.as_deref();
    let cert = context.cert.clone().or_else(|| {
        in_dir(match principal {
            Some(principal) => format!("client.{principal}.crt"),
            None => "client.crt".to_owned(),
        })
    });
    let key = context.key.clone().or_else(|| {
        in_dir(match principal {
            Some(principal) => format!("client.{principal}.key"),
            None => "client.key".to_owned(),
        })
    });
    let ca = context.ca.clone().or_else(|| in_dir("ca.crt".to_owned()));
    let (Some(ca), Some(cert), Some(key)) = (ca, cert, key) else {
        return Err(format!("can't tell the fleet: no certificate to read it from{hint}").into());
    };
    let identity = Identity::load(&ca, &cert, &key).map_err(|error| {
        format!(
            "can't tell the fleet from {}: {error}{hint}",
            cert.display()
        )
    })?;
    Ok(identity.fleet().to_string())
}

/// The `dev` context for a prepared dev directory (ADR-0043, point 5.1), from the configuration
/// the node runs, so it follows an edited port.
///
/// # Errors
///
/// If the directory can't be made absolute.
fn dev_context(prepared: &Prepared) -> std::io::Result<Context> {
    let dir = std::path::absolute(prepared.dir())?;
    Ok(Context {
        fleet: Some(dev::FLEET.to_owned()),
        control: vec![prepared.config().listen.to_string()],
        tls_dir: Some(dir.join(dev::TLS_DIR)),
        principal: Some(dev::PRINCIPAL.to_owned()),
        dev_dir: Some(dir),
        ..Context::default()
    })
}

/// Writes the `dev` context and says how it went, for the banner. A failure is reported, not
/// fatal: the node still starts.
pub(crate) fn write_dev_context(prepared: &Prepared) -> DevContextLine {
    let written = (|| -> Result<DevContextOutcome, Box<dyn Error>> {
        let context = dev_context(prepared)?;
        Ok(store()?.update(|contexts| Ok(contexts.upsert_dev(context)))?)
    })();
    match written {
        Ok(DevContextOutcome::Current) => DevContextLine::Current,
        Ok(DevContextOutcome::NotCurrent { current }) => DevContextLine::NotCurrent {
            current: current.to_string(),
        },
        Ok(DevContextOutcome::Kept) => DevContextLine::Kept,
        Err(error) => DevContextLine::NotWritten {
            reason: one_line(error.as_ref()),
        },
    }
}

/// What the contexts file says about dev mode's context for the dev directory `dir`, for `dev
/// status`'s context line. It reads the file and changes nothing.
pub(crate) fn dev_context_status(dir: &Path) -> String {
    let dir = std::path::absolute(dir).unwrap_or_else(|_| dir.to_owned());
    let loaded = (|| -> Result<Contexts, Box<dyn Error>> { Ok(store()?.load()?) })();
    let contexts = match loaded {
        Ok(contexts) => contexts,
        Err(error) => return format!("unknown ({})", one_line(error.as_ref())),
    };
    let Ok(name) = DEV_NAME.parse::<ContextName>() else {
        return "unknown".to_owned();
    };
    let line = match contexts.contexts.get(&name) {
        None => return "none yet: the dev node writes it when it starts".to_owned(),
        Some(context) => match &context.dev_dir {
            None => DevContextLine::Kept,
            Some(points_at) if *points_at != dir => {
                return format!(
                    "dev (points at another dev directory, {})",
                    points_at.display()
                );
            }
            Some(_) => match contexts.current() {
                Some((current, _)) if *current == name => DevContextLine::Current,
                Some((current, _)) => DevContextLine::NotCurrent {
                    current: current.to_string(),
                },
                None => return "dev (not current: none is; run fleetring switch dev)".to_owned(),
            },
        },
    };
    line.describe()
}

/// An error and its causes on one line.
fn one_line(error: &dyn Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(text: &str) -> ContextName {
        text.parse().expect("a context name")
    }

    fn sample() -> Contexts {
        let mut contexts = Contexts::empty();
        contexts
            .add(
                name("dev"),
                Context {
                    fleet: Some("dev".to_owned()),
                    control: vec!["127.0.0.1:7400".to_owned()],
                    principal: Some("admin".to_owned()),
                    ..Context::default()
                },
            )
            .expect("add");
        contexts
            .add(
                name("home"),
                Context {
                    fleet: Some("home".to_owned()),
                    control: vec!["10.0.0.2:7400".to_owned(), "10.0.0.3:7400".to_owned()],
                    ..Context::default()
                },
            )
            .expect("add");
        contexts
    }

    #[test]
    fn the_list_marks_the_current_context_and_pads_its_columns() {
        let text = render_list(&sample(), OutputFormat::Table).expect("a table");
        let want = [
            "CURRENT  NAME  FLEET  CONTROL                      AS",
            "*        dev   dev    127.0.0.1:7400               admin",
            "         home  home   10.0.0.2:7400,10.0.0.3:7400",
        ];
        assert_eq!(text, format!("{}\n", want.join("\n")));
    }

    #[test]
    fn the_list_as_json_has_null_for_absent_fields() {
        let text = render_list(&sample(), OutputFormat::Json).expect("json");
        let value: serde_json::Value = serde_json::from_str(&text).expect("valid JSON");
        let home = &value["contexts"][1];
        assert_eq!(home["name"], "home");
        assert_eq!(home["current"], false);
        assert_eq!(home["control"][1], "10.0.0.3:7400");
        for field in ["tls_dir", "ca", "cert", "key", "as", "token_file"] {
            assert!(home[field].is_null(), "{field}: {home}");
        }
        assert_eq!(value["contexts"][0]["current"], true);
    }

    #[test]
    fn show_prints_only_the_fields_that_are_set() {
        let contexts = sample();
        let dev = &contexts.contexts[&name("dev")];
        let text = render_one(&name("dev"), dev, true, OutputFormat::Table).expect("text");
        assert_eq!(
            text,
            "name: dev\ncurrent: yes\nfleet: dev\ncontrol: 127.0.0.1:7400\nas: admin\n"
        );
    }

    #[test]
    fn the_dev_banner_lines_say_what_happened() {
        assert_eq!(DevContextLine::Current.describe(), "dev (current)");
        assert_eq!(
            DevContextLine::NotCurrent {
                current: "home".to_owned()
            }
            .describe(),
            "dev (not current: 'home' is; run fleetring switch dev)"
        );
        assert_eq!(
            DevContextLine::Kept.describe(),
            "dev (left alone: you made a context named dev; dev mode keeps it)"
        );
    }
}
