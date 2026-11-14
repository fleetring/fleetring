//! The questions destructive commands ask first (`docs/cli.md`, section 7; ADR-0043, point 4.6).
//!
//! Every prompt says where it acts: the fleet and the context, or the fleet and the control
//! address when no context is in use. An irreversible action asks for its name to be typed, not
//! `y`. Without a terminal a prompt refuses with a usage error (exit 2) that names `--yes`.
//!
//! The prompts are built by pure functions, and [`ask`] reads and writes through the streams it
//! is given, so the tests need no terminal.

use std::error::Error;
use std::io::{self, BufRead, IsTerminal as _, Write};

use crate::exit::Usage;

/// Where a command acts, as a prompt names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Place {
    /// The fleet's name, from the context or the certificate, if either names one.
    pub(crate) fleet: Option<String>,
    /// How the command reaches it: `context 'home'`, or `control 10.0.0.2:7400`.
    pub(crate) via: String,
}

impl Place {
    /// `on fleet 'home' (context 'home')`, or `on the fleet at control 10.0.0.2:7400`.
    pub(crate) fn on(&self) -> String {
        self.with("on")
    }

    /// `from fleet 'home' (context 'home')`, or `from the fleet at control 10.0.0.2:7400`.
    pub(crate) fn from(&self) -> String {
        self.with("from")
    }

    fn with(&self, preposition: &str) -> String {
        match &self.fleet {
            Some(fleet) => format!("{preposition} fleet '{fleet}' ({})", self.via),
            None => format!("{preposition} the fleet at {}", self.via),
        }
    }
}

/// The answer a prompt accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Expect {
    /// The thing's name typed out, for an action that can't be undone.
    Typed {
        /// What to type, in words: `the name`, `the node ID`.
        what: &'static str,
        /// The text that confirms.
        value: String,
    },
    /// `y` or `yes`, for an action that can be undone.
    Yes,
}

/// A question, and what is said when it can't be asked or is declined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Prompt {
    /// What will happen, and where.
    pub(crate) question: String,
    /// The answer that confirms.
    pub(crate) expect: Expect,
    /// What to do without a terminal: the error names `--yes`.
    pub(crate) refusal: String,
    /// The error when the answer isn't the one expected.
    pub(crate) declined: String,
}

impl Prompt {
    /// The line printed on stderr: the question, then what to answer.
    pub(crate) fn line(&self) -> String {
        match &self.expect {
            Expect::Typed { what, .. } => format!("{} type {what} to confirm: ", self.question),
            Expect::Yes => format!("{} [y/N] ", self.question),
        }
    }

    fn accepts(&self, answer: &str) -> bool {
        let answer = answer.trim();
        match &self.expect {
            Expect::Typed { value, .. } => answer == value,
            Expect::Yes => answer.eq_ignore_ascii_case("y") || answer.eq_ignore_ascii_case("yes"),
        }
    }
}

/// Whether a prompt can be asked: stdin and stderr are both terminals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Terminal {
    /// It can.
    Yes,
    /// It can't, so the command refuses.
    No,
}

/// Asks `prompt` on `output` and reads the answer from `input`.
///
/// # Errors
///
/// A [`Usage`] error without a terminal, the declined message for any other answer, and the
/// error either stream returns.
pub(crate) fn ask(
    prompt: &Prompt,
    terminal: Terminal,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<(), Box<dyn Error>> {
    if terminal == Terminal::No {
        return Err(Usage(prompt.refusal.clone()).into());
    }
    write!(output, "{}", prompt.line())?;
    output.flush()?;
    let mut answer = String::new();
    input.read_line(&mut answer)?;
    if prompt.accepts(&answer) {
        Ok(())
    } else {
        Err(prompt.declined.clone().into())
    }
}

/// Asks `prompt` on the process's terminal, or refuses without one.
///
/// # Errors
///
/// As [`ask`].
pub(crate) fn confirm(prompt: &Prompt) -> Result<(), Box<dyn Error>> {
    let stdin = io::stdin();
    let terminal = if stdin.is_terminal() && io::stderr().is_terminal() {
        Terminal::Yes
    } else {
        Terminal::No
    };
    ask(prompt, terminal, &mut stdin.lock(), &mut io::stderr())
}

/// What `db drop` asks.
pub(crate) fn drop_database(name: &str, place: &Place) -> Prompt {
    Prompt {
        question: format!(
            "drop database '{name}' {} and every replica's copy?",
            place.on()
        ),
        expect: Expect::Typed {
            what: "the name",
            value: name.to_owned(),
        },
        refusal: format!("pass --yes to drop {name} without a prompt"),
        declined: format!("not dropping {name}"),
    }
}

/// What `db drop` asks of a fork that isn't ready: the drop cancels it (ADR-0046, point 3.8).
pub(crate) fn drop_fork(name: &str, place: &Place, operation: &str) -> Prompt {
    Prompt {
        question: format!(
            "drop database '{name}' {}, cancelling the fork in progress (operation {operation}), \
             and every replica's copy?",
            place.on()
        ),
        expect: Expect::Typed {
            what: "the name",
            value: name.to_owned(),
        },
        refusal: format!("pass --yes to drop {name} without a prompt"),
        declined: format!("not dropping {name}"),
    }
}

/// What `orphan remove` asks.
pub(crate) fn remove_orphan(name: &str, node: &str, place: &Place) -> Prompt {
    Prompt {
        question: format!(
            "delete the orphan copy of database '{name}' on node {node} {}? its files go for \
             good; copy them first to keep them.",
            place.on()
        ),
        expect: Expect::Typed {
            what: "the name",
            value: name.to_owned(),
        },
        refusal: format!("pass --yes to delete the orphan copy of {name} without a prompt"),
        declined: format!("not deleting the orphan copy of {name}"),
    }
}

/// What `node remove` asks.
pub(crate) fn remove_node(id: &str, place: &Place) -> Prompt {
    Prompt {
        question: format!(
            "remove node '{id}' {}? its replicas are placed elsewhere.",
            place.from()
        ),
        expect: Expect::Typed {
            what: "the node ID",
            value: id.to_owned(),
        },
        refusal: format!("pass --yes to remove {id} without a prompt"),
        declined: format!("not removing {id}"),
    }
}

/// What `token revoke --key-id` asks: every token the key signed stops working, for good.
pub(crate) fn revoke_key(key_id: &str, place: &Place) -> Prompt {
    Prompt {
        question: format!(
            "revoke every token signing key '{key_id}' signed, {}, for good?",
            place.on()
        ),
        expect: Expect::Typed {
            what: "the key ID",
            value: key_id.to_owned(),
        },
        refusal: format!("pass --yes to revoke signing key {key_id} without a prompt"),
        declined: format!("not revoking signing key {key_id}"),
    }
}

/// What `token revoke --principal` asks: it ends tokens nobody listed, so the name is typed.
pub(crate) fn revoke_principal(principal: &str, place: &Place) -> Prompt {
    Prompt {
        question: format!(
            "revoke every token of '{principal}' and of every name below it ('{principal}.*') \
             issued until now, {}?",
            place.on()
        ),
        expect: Expect::Typed {
            what: "the principal",
            value: principal.to_owned(),
        },
        refusal: format!("pass --yes to revoke principal {principal} without a prompt"),
        declined: format!("not revoking principal {principal}"),
    }
}

/// What `agent remove` asks.
pub(crate) fn remove_agent(id: &str, principal: &str, dir: &str, place: &Place) -> Prompt {
    Prompt {
        question: format!(
            "remove agent '{id}' ({principal}) {}: revoke it and every agent below it, and delete \
             {dir}?",
            place.on()
        ),
        expect: Expect::Typed {
            what: "the ID",
            value: id.to_owned(),
        },
        refusal: format!("pass --yes to remove agent '{id}' without a prompt"),
        declined: format!("not removing agent '{id}'"),
    }
}

/// What `node drain` asks when the node holds the only replica of `databases`: a drain can be
/// undone, so `y` confirms.
pub(crate) fn drain_only_replica(id: &str, databases: &[String], place: &Place) -> Prompt {
    let names: Vec<String> = databases.iter().map(|name| format!("'{name}'")).collect();
    Prompt {
        question: format!(
            "drain node '{id}' {}? it holds the only replica of {}, unavailable until it returns.",
            place.on(),
            names.join(", ")
        ),
        expect: Expect::Yes,
        refusal: format!(
            "{id} holds the only replica of {}; pass --yes to drain it without a prompt",
            names.join(", ")
        ),
        declined: format!("not draining {id}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exit::{USAGE, code_of};

    fn home() -> Place {
        Place {
            fleet: Some("home".to_owned()),
            via: "context 'home'".to_owned(),
        }
    }

    /// Asks `prompt` with `answer` typed on a terminal; returns what it printed and the outcome.
    fn answered(prompt: &Prompt, answer: &str) -> (String, Result<(), String>) {
        let mut output = Vec::new();
        let result = ask(prompt, Terminal::Yes, &mut answer.as_bytes(), &mut output)
            .map_err(|error| error.to_string());
        (String::from_utf8(output).expect("UTF-8"), result)
    }

    #[test]
    fn drop_prompt_names_the_fleet_and_the_context() {
        assert_eq!(
            drop_database("notes", &home()).line(),
            "drop database 'notes' on fleet 'home' (context 'home') and every replica's copy? \
             type the name to confirm: "
        );
        let control = Place {
            fleet: Some("home".to_owned()),
            via: "control 10.0.0.2:7400".to_owned(),
        };
        assert_eq!(
            drop_database("notes", &control).line(),
            "drop database 'notes' on fleet 'home' (control 10.0.0.2:7400) and every replica's \
             copy? type the name to confirm: "
        );
        let unnamed = Place {
            fleet: None,
            via: "control 10.0.0.2:7400".to_owned(),
        };
        assert!(
            drop_database("notes", &unnamed)
                .line()
                .contains("on the fleet at control 10.0.0.2:7400"),
        );
    }

    #[test]
    fn every_destructive_prompt_names_where_it_acts() {
        let place = home();
        let prompts = [
            drop_database("notes", &place),
            remove_orphan("notes", "node-2", &place),
            remove_node("node-3", &place),
            revoke_key("k1", &place),
            remove_agent("42", "agent-42", "/agents/42", &place),
            drain_only_replica("node-3", &["notes".to_owned()], &place),
        ];
        for prompt in &prompts {
            let line = prompt.line();
            assert!(line.contains("fleet 'home' (context 'home')"), "{line}");
        }
        assert_eq!(
            remove_node("node-3", &place).line(),
            "remove node 'node-3' from fleet 'home' (context 'home')? its replicas are placed \
             elsewhere. type the node ID to confirm: "
        );
        assert_eq!(
            revoke_key("k1", &place).line(),
            "revoke every token signing key 'k1' signed, on fleet 'home' (context 'home'), for \
             good? type the key ID to confirm: "
        );
    }

    #[test]
    fn an_irreversible_action_needs_its_name_typed() {
        let prompt = drop_database("notes", &home());
        let (printed, result) = answered(&prompt, "notes\n");
        assert_eq!(printed, prompt.line());
        assert_eq!(result, Ok(()));
        for wrong in ["y\n", "yes\n", "note\n", "\n", ""] {
            assert_eq!(
                answered(&prompt, wrong).1,
                Err("not dropping notes".to_owned()),
                "{wrong:?}"
            );
        }
    }

    #[test]
    fn a_reversible_action_takes_yes() {
        let prompt = drain_only_replica("node-3", &["notes".to_owned()], &home());
        assert!(prompt.line().ends_with("[y/N] "), "{}", prompt.line());
        for yes in ["y\n", "Y\n", "yes\n", " YES \n"] {
            assert_eq!(answered(&prompt, yes).1, Ok(()), "{yes:?}");
        }
        for no in ["n\n", "\n", "node-3\n"] {
            assert!(answered(&prompt, no).1.is_err(), "{no:?}");
        }
    }

    #[test]
    fn without_a_terminal_a_prompt_is_a_usage_error_that_names_yes() {
        let prompt = revoke_key("k1", &home());
        let mut output = Vec::new();
        let error =
            ask(&prompt, Terminal::No, &mut "k1\n".as_bytes(), &mut output).expect_err("refuses");
        assert!(output.is_empty(), "nothing is asked");
        assert!(error.to_string().contains("--yes"), "{error}");
        assert_eq!(code_of(error.as_ref()), USAGE);
    }
}
