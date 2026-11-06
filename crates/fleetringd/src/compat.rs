//! What keeps 0.6's spellings working until 1.0 (ADR-0043, point 1): the `fleetringd` name, and
//! the top-level daemon flags that mean `serve`.

use std::ffi::{OsStr, OsString};
use std::path::Path;

/// The name the binary was run as, which it prints in `--version`, usage and errors (ADR-0043
/// point 1.3). Nothing else depends on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProgramName {
    /// `fleetring`, the binary's name.
    Fleetring,
    /// `fleetringd`, its compatibility name until 1.0.
    Fleetringd,
}

impl ProgramName {
    /// `Fleetringd` if `arg0`'s file name, less a leading dot, starts with `fleetringd`
    /// (`fleetringd`, `fleetringd.exe`, `fleetringd.previous`, an upgrader's `.fleetringd.new`);
    /// `Fleetring` otherwise, and for `None`.
    pub(crate) fn from_arg0(arg0: Option<&OsStr>) -> Self {
        let named_fleetringd = arg0
            .and_then(|arg0| Path::new(arg0).file_name())
            .is_some_and(|name| {
                let name = name.as_encoded_bytes();
                name.strip_prefix(b".")
                    .unwrap_or(name)
                    .starts_with(b"fleetringd")
            });
        if named_fleetringd {
            Self::Fleetringd
        } else {
            Self::Fleetring
        }
    }

    /// The name as the binary prints it.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Fleetring => "fleetring",
            Self::Fleetringd => "fleetringd",
        }
    }
}

/// 0.6's top-level daemon flags, which mean `serve` until 1.0 (ADR-0043 point 1.4).
pub(crate) const LEGACY_DAEMON_FLAGS: [&str; 8] = [
    "--config",
    "--node-id",
    "--data-dir",
    "--listen",
    "--advertise",
    "--control",
    "--role",
    "--insecure-plaintext",
];

/// The global flag that may come before a command, and before 0.6's daemon flags.
const LOG_FORMAT_FLAG: &str = "--log-format";

/// How the arguments reached the parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Invocation {
    /// As they were given.
    AsWritten,
    /// With `serve` inserted, because they started with one of [`LEGACY_DAEMON_FLAGS`].
    ServeImplied,
}

/// Inserts `serve` before the first argument if, after any leading `--log-format <v>` or
/// `--log-format=<v>`, that argument is one of [`LEGACY_DAEMON_FLAGS`] alone or as `--flag=value`.
/// `args[0]` is the program and is never touched.
pub(crate) fn imply_serve(mut args: Vec<OsString>) -> (Vec<OsString>, Invocation) {
    let first = first_after_log_format(&args);
    let legacy = args
        .get(first)
        .and_then(|arg| arg.to_str())
        .is_some_and(is_legacy_daemon_flag);
    if !legacy {
        return (args, Invocation::AsWritten);
    }
    args.insert(first, OsString::from("serve"));
    (args, Invocation::ServeImplied)
}

/// The index of the first argument after the program and any leading `--log-format` and its
/// value.
fn first_after_log_format(args: &[OsString]) -> usize {
    let mut index = 1;
    while let Some(arg) = args.get(index).and_then(|arg| arg.to_str()) {
        if arg == LOG_FORMAT_FLAG {
            index += 2;
        } else if arg
            .strip_prefix(LOG_FORMAT_FLAG)
            .is_some_and(|rest| rest.starts_with('='))
        {
            index += 1;
        } else {
            break;
        }
    }
    index
}

/// Whether `arg` is one of [`LEGACY_DAEMON_FLAGS`], alone or as `--flag=value`.
fn is_legacy_daemon_flag(arg: &str) -> bool {
    let flag = arg.split_once('=').map_or(arg, |(flag, _)| flag);
    LEGACY_DAEMON_FLAGS.contains(&flag)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(words: &[&str]) -> Vec<OsString> {
        words.iter().map(OsString::from).collect()
    }

    #[test]
    fn the_name_is_fleetringd_only_for_a_file_named_so() {
        for (arg0, name) in [
            ("/usr/bin/fleetringd", ProgramName::Fleetringd),
            ("fleetringd.exe", ProgramName::Fleetringd),
            ("fleetringd.previous", ProgramName::Fleetringd),
            ("/tmp/x/.fleetringd.new", ProgramName::Fleetringd),
            ("/tmp/x/.fleetring.new", ProgramName::Fleetring),
            ("fleetring", ProgramName::Fleetring),
            ("fleetring.exe", ProgramName::Fleetring),
            ("/x/fleetring.previous", ProgramName::Fleetring),
            ("/opt/fleetringd/bin/fleetring", ProgramName::Fleetring),
        ] {
            assert_eq!(
                ProgramName::from_arg0(Some(OsStr::new(arg0))),
                name,
                "{arg0}"
            );
        }
        assert_eq!(ProgramName::from_arg0(None), ProgramName::Fleetring);
        assert_eq!(ProgramName::Fleetring.as_str(), "fleetring");
        assert_eq!(ProgramName::Fleetringd.as_str(), "fleetringd");
    }

    #[test]
    fn daemon_flags_first_imply_serve() {
        for (given, implied) in [
            (
                &["p", "--config", "f"][..],
                &["p", "serve", "--config", "f"][..],
            ),
            (&["p", "--config=f"], &["p", "serve", "--config=f"]),
            (
                &["p", "--log-format", "json", "--config", "f"],
                &["p", "--log-format", "json", "serve", "--config", "f"],
            ),
            (
                &["p", "--log-format=json", "--node-id", "n"],
                &["p", "--log-format=json", "serve", "--node-id", "n"],
            ),
            (
                &["p", "--insecure-plaintext", "--config", "f"],
                &["p", "serve", "--insecure-plaintext", "--config", "f"],
            ),
            (
                &["p", "--role=data", "--config", "f"],
                &["p", "serve", "--role=data", "--config", "f"],
            ),
        ] {
            assert_eq!(
                imply_serve(args(given)),
                (args(implied), Invocation::ServeImplied),
                "{given:?}"
            );
        }
    }

    #[test]
    fn anything_else_stays_as_written() {
        for given in [
            &["p", "db", "list"][..],
            &["p"],
            &["p", "--help"],
            &["p", "-V"],
            &["p", "serve", "--config", "f"],
            &["p", "--context", "x", "db", "list"],
            &["p", "--log-format", "json"],
            &["p", "--log-format", "json", "status"],
            &["p", "--configuration", "f"],
            &["p", "dev", "--data-dir", "d"],
        ] {
            assert_eq!(
                imply_serve(args(given)),
                (args(given), Invocation::AsWritten),
                "{given:?}"
            );
        }
    }

    #[test]
    fn the_program_is_never_read_as_a_flag() {
        assert_eq!(
            imply_serve(args(&["--config"])),
            (args(&["--config"]), Invocation::AsWritten)
        );
    }
}
