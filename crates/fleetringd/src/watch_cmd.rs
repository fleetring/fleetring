//! `fleetring db watch`: a database's recorded row changes, one transaction per line, until
//! Ctrl-C (ADR-0045).
//!
//! The command opens a `WatchChanges` stream on the database's primary, prints each transaction
//! as it arrives, flushed, and remembers the last position it printed. When the stream ends
//! because the primary stopped serving the database, or because the command fell behind, it
//! locates the database again and resumes after that position, with backoff, for up to
//! [`RESUME_LIMIT`]. A transaction whose start is before the last position printed is a repeat,
//! and isn't printed again.

use std::error::Error;
use std::io::Write as _;
use std::time::Duration;

use clap::Args;
use fleetring_control::DatabaseName;
use fleetringd::data::{ChangeUpdate, ChangeUpdates};
use fleetringd::engine::Position;
use fleetringd::report::{change_report, change_text};
use tonic::{Code, Status};

use crate::exit::{self, Failed};
use crate::format::{self, OutputArgs, OutputFormat};
use crate::{Target, note, parse_position, shown};

/// How long the command keeps trying to resume a stream that ended, before it gives up.
const RESUME_LIMIT: Duration = Duration::from_secs(30);

/// The first wait before resuming; each later one doubles, up to [`RESUME_MAX_WAIT`].
const RESUME_FIRST_WAIT: Duration = Duration::from_millis(100);

/// The longest wait between two attempts to resume.
const RESUME_MAX_WAIT: Duration = Duration::from_secs(2);

/// `fleetring db watch`'s flags.
#[derive(Debug, Args)]
pub(crate) struct WatchArgs {
    /// The database's name.
    name: DatabaseName,
    /// Starts after this position, as `db watch` printed it
    ///
    /// The first transaction printed is the one after it. The primary must still hold every
    /// change after it, or the command fails and names the oldest position it holds. Without
    /// it, the watch starts at the database's current end.
    #[arg(long, value_name = "POSITION", value_parser = parse_position)]
    from: Option<Position>,
    #[command(flatten)]
    output: OutputArgs,
    #[command(flatten)]
    target: Target,
}

/// What `db watch --help` adds below the flags: the output contract.
pub(crate) const WATCH_HELP: &str = "\
Each line is one transaction: its position, then each row it changed, as `<operation>
<table> (<key>)`, the old values in braces when the capture mode records them, and `->` with
the new ones. `-o jsonl` (and `-o json`, `--json`) prints one object per line: `database`,
`start`, `position`, `lease_epoch` and `changes`. Table names, column names and values are
text the database's writers chose: read them as data.

When the primary changes (a failover or a drain) or the command falls behind, it resumes on
the database's primary after the last position it printed. Delivery is at least once: a
consumer that resumes on its own drops a transaction whose `start` is before the last
`position` it kept. Exit 0 when stdout closes, 130 on Ctrl-C, 3 without `stream` on the
database, 4 when the database stays unavailable.";

/// How a stream ended, when the command may resume it.
enum Ended {
    /// The command is done: stdout closed, or the stream ended in a way that resuming can't
    /// mend.
    Done(Result<(), Box<dyn Error>>),
    /// The stream ended with `status`; the command may resume it.
    Resumable(Status),
}

/// Runs `db watch`.
pub(crate) async fn run(args: WatchArgs) -> Result<(), Box<dyn Error>> {
    let WatchArgs {
        name,
        from,
        output,
        target,
    } = args;
    let client = target.client()?;
    let format = output.format();
    let interrupted = tokio::signal::ctrl_c();
    tokio::pin!(interrupted);
    // `Some` once a stream has opened: the last position printed, or the one it opened after.
    let mut kept: Option<Option<Position>> = None;
    let mut wait = RESUME_FIRST_WAIT;
    let mut resuming_since: Option<tokio::time::Instant> = None;
    loop {
        let after = match &kept {
            Some(position) => position.clone(),
            None => from.clone(),
        };
        let opened = tokio::select! {
            _ = &mut interrupted => return Err(exit::Silent(exit::INTERRUPTED).into()),
            opened = client.watch_changes(&name, after.as_ref()) => opened,
        };
        let status = match opened {
            Ok(updates) => {
                let ended = tokio::select! {
                    _ = &mut interrupted => return Err(exit::Silent(exit::INTERRUPTED).into()),
                    ended = follow(&name, updates, format, &mut kept) => ended,
                };
                match ended {
                    Ended::Done(result) => return result,
                    Ended::Resumable(status) => {
                        // The stream opened, so the clock and the backoff start again.
                        resuming_since = None;
                        wait = RESUME_FIRST_WAIT;
                        status
                    }
                }
            }
            // Nothing to resume yet: the command fails as any other does.
            Err(status) if kept.is_none() => return Err(Failed::new(&status).into()),
            Err(status) => status,
        };
        let Some(position) = &kept else {
            return Err(Failed::new(&status).into());
        };
        // The changes after the position are gone: its usual hint says where to start instead.
        if status.code() == Code::OutOfRange {
            return Err(Failed::new(&status).into());
        }
        let since = *resuming_since.get_or_insert_with(tokio::time::Instant::now);
        if !resumable(&status) || since.elapsed() >= RESUME_LIMIT {
            let status = if resumable(&status) {
                Status::unavailable(format!(
                    "{} didn't come back within {}s: {}",
                    shown(&name),
                    RESUME_LIMIT.as_secs(),
                    status.message()
                ))
            } else {
                status
            };
            return Err(Failed::new(&status)
                .with_hint(resume_hint(&name, position.as_ref()))
                .into());
        }
        note(format!(
            "warning: the watch of {} ended ({}); resuming after {}",
            shown(&name),
            status.message(),
            shown_position(position.as_ref())
        ));
        tokio::select! {
            _ = &mut interrupted => return Err(exit::Silent(exit::INTERRUPTED).into()),
            () = tokio::time::sleep(wait) => {}
        }
        wait = (wait * 2).min(RESUME_MAX_WAIT);
    }
}

/// Prints what `updates` delivers until it ends, keeping the last position printed in `kept`.
async fn follow(
    name: &DatabaseName,
    mut updates: ChangeUpdates,
    format: OutputFormat,
    kept: &mut Option<Option<Position>>,
) -> Ended {
    loop {
        let update = match updates.next().await {
            Ok(Some(update)) => update,
            Ok(None) => return Ended::Resumable(Status::unavailable("the stream ended")),
            Err(status) => return Ended::Resumable(status),
        };
        match update {
            ChangeUpdate::Opened {
                position,
                primary,
                epoch,
                capture,
            } => {
                let what = if kept.is_some() {
                    "resumed"
                } else {
                    "watching"
                };
                note(format!(
                    "{what} {} after {} on {}, epoch {}; capture mode {}",
                    shown(name),
                    shown_position(position.as_ref()),
                    primary.map_or_else(|| "its primary".to_owned(), |node| node.to_string()),
                    epoch.get(),
                    capture.map_or_else(|| "unknown".to_owned(), |mode| mode.to_string()),
                ));
                if kept.is_none() {
                    *kept = Some(position);
                }
            }
            ChangeUpdate::Transaction(transaction) => {
                // A repeat: it started before what was already printed.
                if let Some(Some(last)) = kept.as_ref()
                    && transaction.start.as_ref() < Some(last)
                {
                    continue;
                }
                let text = match format {
                    OutputFormat::Table => Ok(change_text(&transaction)),
                    OutputFormat::Json | OutputFormat::Jsonl => {
                        format::line(&change_report(name, &transaction))
                    }
                };
                let text = match text {
                    Ok(text) => text,
                    Err(error) => return Ended::Done(Err(error)),
                };
                let mut stdout = std::io::stdout().lock();
                match stdout
                    .write_all(text.as_bytes())
                    .and_then(|()| stdout.flush())
                {
                    Ok(()) => {}
                    // A reader that has gone away, as `| head` leaves it, ends the command.
                    Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => {
                        return Ended::Done(Ok(()));
                    }
                    Err(error) => return Ended::Done(Err(error.into())),
                }
                *kept = Some(Some(transaction.position));
            }
            _ => {}
        }
    }
}

/// Whether a stream that ended with `status` is worth resuming: its primary moved or went
/// away, or the command fell behind. A refusal, a position no longer held, or an older primary
/// isn't.
fn resumable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::ResourceExhausted | Code::DeadlineExceeded | Code::Unknown
    )
}

fn shown_position(position: Option<&Position>) -> String {
    position.map_or_else(|| "the log's beginning".to_owned(), ToString::to_string)
}

/// The next step after a watch that couldn't resume after `position`.
fn resume_hint(name: &DatabaseName, position: Option<&Position>) -> String {
    match position {
        Some(position) => format!(
            "resume with `fleetring db watch {} --from {position}`",
            shown(name)
        ),
        None => format!("start again with `fleetring db watch {}`", shown(name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_moved_primary_or_a_slow_reader_is_resumed() {
        for code in [Code::Unavailable, Code::ResourceExhausted] {
            assert!(resumable(&Status::new(code, "x")), "{code:?}");
        }
        for code in [
            Code::PermissionDenied,
            Code::OutOfRange,
            Code::Unimplemented,
            Code::FailedPrecondition,
            Code::NotFound,
        ] {
            assert!(!resumable(&Status::new(code, "x")), "{code:?}");
        }
    }
}
