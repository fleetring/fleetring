//! Exit codes, and the errors that carry them (`docs/cli.md`, sections 6 and 8).
//!
//! Every command ends with one of six codes, the same for every command:
//!
//! | Code | Meaning |
//! |---|---|
//! | 0 | success |
//! | 1 | the command failed |
//! | 2 | usage: a bad flag or argument, or a confirmation needed without a terminal |
//! | 3 | refused: `UNAUTHENTICATED` or `PERMISSION_DENIED` |
//! | 4 | unavailable, and a retry may succeed: `UNAVAILABLE`, a timeout |
//! | 130 | interrupted |
//!
//! A failed call becomes a [`Failed`], which keeps the gRPC code, so [`code_of`] can find it
//! anywhere in an error's chain; [`exit_code`] maps a status to its code in one place.

use std::error::Error;
use std::fmt;

use fleetring_client::output::printable;
use fleetringd::control::missing_database;
use tonic::{Code, Status};

/// The command failed.
pub(crate) const FAILURE: u8 = 1;
/// A usage error, as clap's own.
pub(crate) const USAGE: u8 = 2;
/// Authentication or authorization refused the request.
pub(crate) const REFUSED: u8 = 3;
/// Unavailable; a retry may succeed.
pub(crate) const UNAVAILABLE: u8 = 4;
/// Interrupted with Ctrl-C.
pub(crate) const INTERRUPTED: u8 = 130;

/// The exit code of a call that failed with `status`.
pub(crate) fn exit_code(status: &Status) -> u8 {
    code_for(status.code())
}

fn code_for(code: Code) -> u8 {
    match code {
        Code::Unauthenticated | Code::PermissionDenied => REFUSED,
        Code::Unavailable | Code::DeadlineExceeded => UNAVAILABLE,
        _ => FAILURE,
    }
}

/// The exit code of a command that failed with `error`: the first error in its chain that
/// carries one decides, and any other failure is 1.
pub(crate) fn code_of(error: &(dyn Error + 'static)) -> u8 {
    let mut next = Some(error);
    while let Some(error) = next {
        if let Some(failed) = error.downcast_ref::<Failed>() {
            return code_for(failed.code);
        }
        if let Some(status) = error.downcast_ref::<Status>() {
            return exit_code(status);
        }
        if error.is::<Usage>() {
            return USAGE;
        }
        if let Some(Silent(code)) = error.downcast_ref::<Silent>() {
            return *code;
        }
        next = error.source();
    }
    FAILURE
}

/// A failed call as one readable line, `<kind of failure>: <the server's message>`, with the
/// next step for the common failures.
#[derive(Debug)]
pub(crate) struct Failed {
    code: Code,
    message: String,
    hint: Option<String>,
}

impl Failed {
    /// `status` as the command line reports it, with the hint [`hint`] gives.
    pub(crate) fn new(status: &Status) -> Self {
        Self {
            code: status.code(),
            // The message may name a database, whose name a principal chose.
            message: printable(status.message()),
            hint: hint(status),
        }
    }

    /// This failure with `hint` in place of the usual one.
    pub(crate) fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
}

/// A failed call as one line, without a hint: `<kind of failure>: <the server's message>`, for
/// a line among others, such as one replica's answer.
pub(crate) fn describe(status: &Status) -> String {
    format!(
        "{}: {}",
        status.code().description(),
        printable(status.message())
    )
}

impl fmt::Display for Failed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.description(), self.message)?;
        match &self.hint {
            Some(hint) => write!(f, "\n  hint: {hint}"),
            None => Ok(()),
        }
    }
}

impl Error for Failed {}

/// The next step after the failures people meet most (`docs/cli.md`, section 6): an unknown
/// database, a refusal, and an unavailable fleet.
pub(crate) fn hint(status: &Status) -> Option<String> {
    match status.code() {
        Code::NotFound => missing_database(status).map(|name| {
            let name = printable(name);
            format!(
                "list databases with `fleetring db list`, or create it with `fleetring db create \
                 {name}`"
            )
        }),
        Code::PermissionDenied => Some(
            "act as a principal whose certificate or token grants it (`--as`, `--token-file`, \
             `--context`), or issue a token with that scope (`fleetring token issue`)"
                .to_owned(),
        ),
        Code::Unauthenticated => Some(
            "check the identity the command sends, with `fleetring context show`; a certificate \
             or token may have expired or been revoked"
                .to_owned(),
        ),
        // `db watch` after a position whose changes the primary no longer holds (ADR-0045).
        Code::OutOfRange if status.message().contains("no longer held") => Some(
            "watch from the oldest position held with `--from <position>`, or from now without \
             `--from`"
                .to_owned(),
        ),
        Code::Unavailable | Code::DeadlineExceeded => Some(
            "retry; the fleet may be starting or failing over. If it persists, check \
             `fleetring status`, or `fleetring dev status` for a dev node"
                .to_owned(),
        ),
        _ => None,
    }
}

/// A usage error: a flag or argument the command can't use, or a confirmation it can't ask.
#[derive(Debug)]
pub(crate) struct Usage(pub(crate) String);

impl fmt::Display for Usage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for Usage {}

/// An outcome the command has already reported, such as a restore that printed why it couldn't
/// set its expiry, or a watch ended with Ctrl-C: it only chooses the exit code.
#[derive(Debug)]
pub(crate) struct Silent(pub(crate) u8);

impl fmt::Display for Silent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the command ended with exit code {}", self.0)
    }
}

impl Error for Silent {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_grpc_code_maps_to_one_exit_code() {
        let table = [
            (Code::Ok, FAILURE),
            (Code::Cancelled, FAILURE),
            (Code::Unknown, FAILURE),
            (Code::InvalidArgument, FAILURE),
            (Code::DeadlineExceeded, UNAVAILABLE),
            (Code::NotFound, FAILURE),
            (Code::AlreadyExists, FAILURE),
            (Code::PermissionDenied, REFUSED),
            (Code::ResourceExhausted, FAILURE),
            (Code::FailedPrecondition, FAILURE),
            (Code::Aborted, FAILURE),
            (Code::OutOfRange, FAILURE),
            (Code::Unimplemented, FAILURE),
            (Code::Internal, FAILURE),
            (Code::Unavailable, UNAVAILABLE),
            (Code::DataLoss, FAILURE),
            (Code::Unauthenticated, REFUSED),
        ];
        for (code, exit) in table {
            assert_eq!(exit_code(&Status::new(code, "x")), exit, "{code:?}");
        }
    }

    #[test]
    fn the_code_is_found_anywhere_in_the_chain() {
        #[derive(Debug)]
        struct Wrapper(Box<dyn Error + Send + Sync>);
        impl fmt::Display for Wrapper {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("wrapped")
            }
        }
        impl Error for Wrapper {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                Some(self.0.as_ref())
            }
        }
        let refused = Failed::new(&Status::permission_denied("no"));
        assert_eq!(code_of(&refused), REFUSED);
        assert_eq!(code_of(&Wrapper(Box::new(refused))), REFUSED);
        assert_eq!(code_of(&Status::unavailable("down")), UNAVAILABLE);
        assert_eq!(code_of(&Usage("pass --yes".to_owned())), USAGE);
        assert_eq!(code_of(&Silent(INTERRUPTED)), INTERRUPTED);
        let plain: Box<dyn Error> = "no control address".into();
        assert_eq!(code_of(plain.as_ref()), FAILURE);
    }

    #[test]
    fn a_failure_reads_as_one_line_and_a_hint() {
        let failed = Failed::new(&Status::unavailable("the control plane is down\u{1b}[2J"));
        let text = failed.to_string();
        let mut lines = text.lines();
        assert_eq!(
            lines.next(),
            Some("The service is currently unavailable: the control plane is down\\u{001B}[2J")
        );
        assert!(
            lines
                .next()
                .is_some_and(|line| line.starts_with("  hint: retry")),
            "{text}"
        );
        assert_eq!(
            Failed::new(&Status::internal("x")).to_string(),
            "Internal error: x"
        );
        assert!(
            Failed::new(&Status::internal("x"))
                .with_hint("do this")
                .to_string()
                .ends_with("\n  hint: do this")
        );
    }

    #[test]
    fn the_common_failures_have_hints() {
        for code in [
            Code::PermissionDenied,
            Code::Unauthenticated,
            Code::Unavailable,
            Code::DeadlineExceeded,
        ] {
            assert!(hint(&Status::new(code, "x")).is_some(), "{code:?}");
        }
        assert_eq!(hint(&Status::not_found("no member 'x'")), None);
        let mut missing = Status::not_found("database 'notes' doesn't exist");
        missing.metadata_mut().insert(
            fleetringd::control::MISSING_DATABASE_KEY,
            tonic::metadata::MetadataValue::from_static("notes"),
        );
        assert_eq!(
            hint(&missing).as_deref(),
            Some(
                "list databases with `fleetring db list`, or create it with `fleetring db create \
                 notes`"
            )
        );
        assert_eq!(hint(&Status::internal("x")), None);
    }
}
