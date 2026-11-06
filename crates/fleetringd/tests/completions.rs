//! FR-056: shell completions (`docs/cli.md`, section 9). `fleetring completions <shell>` prints
//! a script that runs the binary on each Tab with `FLEETRING_COMPLETE` set; these tests run the
//! binary as zsh's script does, so they need no shell.

use std::error::Error;
use std::path::Path;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// Every top-level command.
const COMMANDS: [&str; 17] = [
    "serve",
    "dev",
    "stop",
    "context",
    "switch",
    "completions",
    "cert",
    "token",
    "db",
    "node",
    "lease",
    "orphan",
    "archive",
    "upgrade",
    "mcp",
    "agent",
    "status",
];

#[test]
fn every_shell_gets_a_script() -> TestResult {
    for shell in ["bash", "zsh", "fish", "powershell"] {
        let output = support::fleetring().args(["completions", shell]).output()?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{shell}: {stderr}");
        let script = String::from_utf8(output.stdout)?;
        assert!(script.contains("FLEETRING_COMPLETE"), "{shell}: {script}");
        assert!(script.contains("fleetring"), "{shell}: {script}");
    }
    let refused = support::fleetring()
        .args(["completions", "tcsh"])
        .output()?;
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    Ok(())
}

/// The values, flags aside, zsh's script gets for `words`, the command line, completing the last
/// word, with
/// `home` as the home directory and its configuration directory.
fn complete(home: &Path, words: &[&str]) -> Result<Vec<String>, Box<dyn Error>> {
    let config = home.join("config");
    let output = support::fleetring()
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", &config)
        .env("APPDATA", &config)
        .env("XDG_DATA_HOME", home.join("data"))
        .env("LOCALAPPDATA", home.join("data"))
        .env("FLEETRING_COMPLETE", "zsh")
        .env("_CLAP_COMPLETE_INDEX", (words.len() - 1).to_string())
        .env("_CLAP_IFS", "\n")
        .arg("--")
        .args(words)
        .output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{words:?}: {stderr}");
    let stdout = String::from_utf8(output.stdout)?;
    // zsh's values are `value:description`. Flags are offered beside an argument's values, and
    // left out here.
    Ok(stdout
        .lines()
        .map(|line| line.split(':').next().unwrap_or_default().to_owned())
        .filter(|value| !value.starts_with('-'))
        .collect())
}

#[test]
fn the_first_word_completes_to_every_command() -> TestResult {
    let home = tempfile::tempdir()?;
    let offered = complete(home.path(), &["fleetring", ""])?;
    for command in COMMANDS {
        assert!(
            offered.iter().any(|value| value == command),
            "{command} isn't offered: {offered:?}"
        );
    }
    let offered = complete(home.path(), &["fleetring", "db", "cr"])?;
    assert_eq!(offered, ["create"]);
    Ok(())
}

/// Writes a contexts file with `home` and `prod`, `prod` current, whose agents live in
/// `<home>/agents`, with the agents `42` and `7`.
fn contexts(home: &Path) -> TestResult {
    let dir = home.join("config").join("fleetring");
    std::fs::create_dir_all(&dir)?;
    let agents = home.join("agents");
    for id in ["42", "7"] {
        std::fs::create_dir_all(agents.join(id))?;
    }
    // Not an agent ID: never offered.
    std::fs::create_dir_all(agents.join("Not-An-Id"))?;
    // TOML literal strings, so a Windows path needs no escaping.
    let file = format!(
        "version = 1\ncurrent = \"prod\"\n\n\
         [contexts.home]\ncontrol = [\"127.0.0.1:1\"]\n\n\
         [contexts.prod]\ncontrol = [\"127.0.0.1:2\"]\nagents_dir = '{}'\n",
        agents.display()
    );
    std::fs::write(dir.join("contexts.toml"), file)?;
    Ok(())
}

#[test]
fn context_names_complete_from_the_contexts_file() -> TestResult {
    let home = tempfile::tempdir()?;
    // No contexts file: nothing to offer, and no error.
    assert!(complete(home.path(), &["fleetring", "switch", ""])?.is_empty());
    contexts(home.path())?;
    for words in [
        &["fleetring", "db", "list", "--context", ""][..],
        &["fleetring", "switch", ""],
        &["fleetring", "context", "use", ""],
    ] {
        assert_eq!(complete(home.path(), words)?, ["home", "prod"], "{words:?}");
    }
    assert_eq!(
        complete(home.path(), &["fleetring", "status", "--context", "h"])?,
        ["home"]
    );
    Ok(())
}

#[test]
fn agent_ids_complete_from_the_current_contexts_agents() -> TestResult {
    let home = tempfile::tempdir()?;
    contexts(home.path())?;
    for words in [
        &["fleetring", "agent", "show", ""][..],
        &["fleetring", "db", "list", "--agent", ""],
    ] {
        assert_eq!(complete(home.path(), words)?, ["42", "7"], "{words:?}");
    }
    Ok(())
}
