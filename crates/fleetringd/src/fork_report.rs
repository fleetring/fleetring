//! What the command line and the MCP server report about a fork (ADR-0046, `docs/cli.md`
//! section 15): the typed results built from the control plane's fork records, and the text they
//! are drawn as.
//!
//! Everything here is a pure function of a record, so every state can be rendered, and
//! snapshotted, without a fork in progress.

use std::fmt::Write as _;
use std::time::Duration;

use fleetring_client::output::{ForkFailureReport, ForkReport, ForkStatusReport, printable};
use fleetring_control::Timestamp;
use fleetring_proto::v1 as proto;

use crate::data::convert::location;
use crate::report::{placement_report, rfc3339};

/// An incarnation as 16 lowercase hex digits.
#[must_use]
pub fn incarnation_text(incarnation: u64) -> String {
    format!("{incarnation:016x}")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}

fn time(ms: u64) -> String {
    rfc3339(Timestamp::from_millis(ms))
}

/// The name of a fork state, as JSON and text print it.
#[must_use]
pub fn state_name(state: i32) -> &'static str {
    match proto::ForkState::try_from(state) {
        Ok(proto::ForkState::Seeding) => "seeding",
        Ok(proto::ForkState::Seeded) => "seeded",
        Ok(proto::ForkState::Ready) => "ready",
        Ok(proto::ForkState::Failed) => "failed",
        Ok(proto::ForkState::Cancelled) => "cancelled",
        Ok(proto::ForkState::Unspecified) | Err(_) => "unknown",
    }
}

/// Whether `state` is one an operation never leaves.
#[must_use]
pub fn is_final(state: i32) -> bool {
    matches!(
        proto::ForkState::try_from(state),
        Ok(proto::ForkState::Ready | proto::ForkState::Failed | proto::ForkState::Cancelled)
    )
}

fn reason_name(reason: i32) -> String {
    match proto::ForkFailureReason::try_from(reason) {
        Ok(proto::ForkFailureReason::SourceGone) => "source_gone",
        Ok(proto::ForkFailureReason::SourceRecreated) => "source_recreated",
        Ok(proto::ForkFailureReason::RetriesExhausted) => "retries_exhausted",
        Ok(proto::ForkFailureReason::DeadlineExceeded) => "deadline_exceeded",
        Ok(proto::ForkFailureReason::SourceTooLarge) => "source_too_large",
        Ok(proto::ForkFailureReason::InsufficientDisk) => "insufficient_disk",
        Ok(proto::ForkFailureReason::EngineUnsupported) => "engine_unsupported",
        Ok(proto::ForkFailureReason::Unspecified) | Err(_) => "unknown",
    }
    .to_owned()
}

fn cause_name(cause: i32) -> Option<String> {
    Some(
        match proto::RetryCause::try_from(cause) {
            Ok(proto::RetryCause::SeederDown) => "seeder_down",
            Ok(proto::RetryCause::AttemptTimedOut) => "attempt_timed_out",
            Ok(proto::RetryCause::TransferFailed) => "transfer_failed",
            Ok(proto::RetryCause::PinExpired) => "pin_expired",
            Ok(proto::RetryCause::HolderLost) => "holder_lost",
            Ok(proto::RetryCause::HoldersLeft) => "holders_left",
            Ok(proto::RetryCause::SeederDiskRefused) => "seeder_disk_refused",
            Ok(proto::RetryCause::Unspecified) | Err(_) => return None,
        }
        .to_owned(),
    )
}

/// The failure a record carries, if it has one.
#[must_use]
pub fn failure_report(failure: Option<&proto::ForkFailure>) -> Option<ForkFailureReport> {
    let failure = failure?;
    (failure.reason != 0).then(|| ForkFailureReport {
        reason: reason_name(failure.reason),
        last_cause: cause_name(failure.last_cause),
        attempts: failure.attempts,
    })
}

/// The report on the fork record `fork`.
#[must_use]
pub fn status_report(fork: &proto::ForkStatus) -> ForkStatusReport {
    ForkStatusReport {
        state: state_name(fork.state),
        operation_id: operation_text(&fork.operation_id),
        source: fork.source.clone(),
        source_incarnation: incarnation_text(fork.source_incarnation),
        attempt: fork.attempt,
        max_attempts: fork.max_attempts,
        seeder: fork.seeder.clone(),
        seeder_excluded: fork.seeder_excluded.clone(),
        disk_refused: fork.disk_refused.clone(),
        holders: fork.holders.clone(),
        holders_needed: fork.holders_needed,
        kept_holders: fork.kept_holders.clone(),
        bytes: fork.bytes,
        total_bytes: fork.total_bytes,
        position: fork.position.as_deref().map(hex),
        chain_hash: fork.chain_hash.as_deref().map(hex),
        failure: failure_report(fork.failure.as_ref()),
        requested_by: fork.requested_by.clone(),
        requested_at: time(fork.requested_at_ms),
        deadline_at: fork.deadline_at_ms.map(time),
        completed_at: fork.completed_at_ms.map(time),
        ready_at: fork.ready_at_ms.map(time),
        failed_at: fork.failed_at_ms.map(time),
    }
}

/// An operation ID's bytes as the 26 characters it is written in, or hex if they are not 16.
#[must_use]
pub fn operation_text(bytes: &[u8]) -> String {
    <[u8; 16]>::try_from(bytes).map_or_else(
        |_| hex(bytes),
        |bytes| fleetring_control::fork::OperationId::from_bytes(bytes).to_string(),
    )
}

/// `db fork`'s result for the operation `status`, `elapsed` after the command began.
#[must_use]
pub fn fork_report(status: &proto::ForkOperationStatus, elapsed: Duration) -> ForkReport {
    ForkReport {
        operation_id: operation_text(&status.operation_id),
        database: status.database.clone(),
        incarnation: incarnation_text(status.incarnation),
        state: state_name(status.state),
        source: status.source.clone(),
        source_incarnation: incarnation_text(status.source_incarnation),
        position: status.position.as_deref().map(hex),
        chain_hash: status.chain_hash.as_deref().map(hex),
        failure: failure_report(status.failure.as_ref()),
        elapsed_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        placement: status
            .placement
            .clone()
            .and_then(|placement| location(placement).ok())
            .map(|location| placement_report(&location)),
    }
}

fn mib(bytes: u64) -> String {
    const MIB: u64 = 1024 * 1024;
    if bytes >= MIB {
        format!("{} MiB", bytes / MIB)
    } else if bytes >= 1024 {
        format!("{} KiB", bytes / 1024)
    } else {
        format!("{bytes} B")
    }
}

fn seconds(elapsed: Duration) -> String {
    format!("{:.1} s", elapsed.as_secs_f64())
}

/// A fork's state in `db list`'s `FORK` column.
#[must_use]
pub fn column_state(fork: Option<&ForkStatusReport>) -> &'static str {
    match fork.map(|fork| fork.state) {
        Some("seeding") => "seeding",
        Some("seeded") => "opening",
        Some("ready") => "ready",
        Some("failed") => "failed",
        _ => "-",
    }
}

/// The progress line for the operation `status` of the fork `target`, as `db fork` prints it on
/// stderr, or `None` for a state with no line.
#[must_use]
pub fn progress_line(target: &str, status: &proto::ForkOperationStatus) -> Option<String> {
    let target = printable(target);
    let source = printable(&status.source);
    match proto::ForkState::try_from(status.state) {
        Ok(proto::ForkState::Seeding) => {
            let fork = status.placement.as_ref().and_then(|p| p.fork.as_ref());
            let mut line = format!(
                "seeding {target} from {source}: attempt {} of {}",
                fork.map_or(status.attempt, |fork| fork.attempt),
                fork.map_or(0, |fork| fork.max_attempts),
            );
            if let Some(fork) = fork {
                if let Some(seeder) = &fork.seeder {
                    let _ = write!(line, ", seeder {seeder}");
                }
                if let (Some(bytes), Some(total)) = (fork.bytes, fork.total_bytes) {
                    let _ = write!(line, ", {} of {}", mib(bytes), mib(total));
                }
                let _ = write!(
                    line,
                    ", holders {} of {}",
                    fork.holders.len(),
                    fork.holders_needed
                );
                if !fork.seeder_excluded.is_empty() {
                    let _ = write!(line, ", not seeders: {}", fork.seeder_excluded.join(", "));
                }
                if !fork.disk_refused.is_empty() {
                    let _ = write!(line, ", out of disk: {}", fork.disk_refused.join(", "));
                }
            }
            Some(line)
        }
        Ok(proto::ForkState::Seeded) => Some(format!("opening {target}: running the fork reset")),
        _ => None,
    }
}

/// The line `db fork` prints on stderr once the fork is ready.
#[must_use]
pub fn ready_line(report: &ForkReport) -> String {
    format!(
        "\u{2713} {} is ready: forked from {} (incarnation {}) at {} in {}",
        printable(&report.database),
        printable(&report.source),
        report.source_incarnation,
        report.position.as_deref().unwrap_or("the log's beginning"),
        seconds(Duration::from_millis(report.elapsed_ms)),
    )
}

/// `db fork`'s text result: key and value lines, then `placement` (`db locate`'s text) if there
/// is any.
#[must_use]
pub fn result_text(report: &ForkReport, placement: Option<&str>) -> String {
    let mut text = format!(
        "database: {}\nincarnation: {}\nstate: {}\nforked from: {} (incarnation {})\n\
         position: {}\nchain hash: {}\noperation: {}\nelapsed: {}\n",
        printable(&report.database),
        report.incarnation,
        report.state,
        printable(&report.source),
        report.source_incarnation,
        report.position.as_deref().unwrap_or("not yet"),
        report.chain_hash.as_deref().unwrap_or("not yet"),
        report.operation_id,
        seconds(Duration::from_millis(report.elapsed_ms)),
    );
    if let Some(failure) = &report.failure {
        let _ = writeln!(text, "failure: {}", failure_text(failure));
    }
    if let Some(placement) = placement {
        text.push_str(placement);
    }
    text
}

/// A failure in one line, in words: why it failed, the cause of its last retry, and its attempts.
#[must_use]
pub fn failure_text(failure: &ForkFailureReport) -> String {
    let mut text = match failure.reason.as_str() {
        "source_gone" => "the source was dropped",
        "source_recreated" => "the source was dropped and created again",
        "retries_exhausted" => "retries ran out",
        "deadline_exceeded" => "the deadline passed",
        "source_too_large" => "the source is too large to fork",
        "insufficient_disk" => "not enough disk on its replicas",
        "engine_unsupported" => "the source's engine can't be forked",
        other => other,
    }
    .to_owned();
    if let Some(cause) = &failure.last_cause {
        let _ = write!(text, "; last retry: {}", cause.replace('_', " "));
    }
    if failure.attempts > 0 {
        let plural = if failure.attempts == 1 { "" } else { "s" };
        let _ = write!(text, " ({} attempt{plural})", failure.attempts);
    }
    text
}

/// The "Fork" lines of `db show`, after the placement (`docs/cli.md`, section 15). `name` is the
/// database the lines are about, for the hint.
#[must_use]
pub fn section_text(name: &str, fork: &ForkStatusReport) -> String {
    let mut text = String::new();
    let name = printable(name);
    match fork.state {
        "seeding" => {
            let _ = write!(
                text,
                "fork: seeding, attempt {} of {}",
                fork.attempt, fork.max_attempts
            );
            if let Some(seeder) = &fork.seeder {
                let _ = write!(text, ", seeder {seeder}");
            }
            text.push('\n');
        }
        "seeded" => text.push_str("fork: opening: not yet usable\n"),
        "ready" => {
            let _ = writeln!(
                text,
                "fork: ready{}",
                fork.ready_at
                    .as_deref()
                    .map_or_else(String::new, |at| format!(", since {at}"))
            );
        }
        "failed" => {
            let _ = writeln!(
                text,
                "fork: failed, {}",
                fork.failure
                    .as_ref()
                    .map_or_else(|| "for an unknown reason".to_owned(), failure_text)
            );
        }
        other => {
            let _ = writeln!(text, "fork: {other}");
        }
    }
    let mut exclusions: Vec<String> = fork
        .seeder_excluded
        .iter()
        .map(|node| format!("{node} (not seeder)"))
        .collect();
    exclusions.extend(
        fork.disk_refused
            .iter()
            .map(|node| format!("{node} (disk refused)")),
    );
    if !exclusions.is_empty() {
        let _ = writeln!(text, "fork exclusions: {}", exclusions.join("; "));
    }
    if !fork.holders.is_empty() {
        let _ = write!(text, "fork holders: {}", fork.holders.join(", "));
        if fork.state == "seeding" {
            let _ = write!(
                text,
                ", {} of {} needed",
                fork.holders.len(),
                fork.holders_needed
            );
        }
        text.push('\n');
    }
    if let (Some(bytes), Some(total)) = (fork.bytes, fork.total_bytes) {
        let _ = write!(text, "fork progress: {} of {}", mib(bytes), mib(total));
        if let Some(seeder) = &fork.seeder {
            let _ = write!(text, " on {seeder}");
        }
        text.push('\n');
    }
    let _ = write!(
        text,
        "forked from: {} (incarnation {})",
        printable(&fork.source),
        fork.source_incarnation
    );
    if let Some(position) = &fork.position {
        let _ = write!(text, " at {position}");
        if let Some(hash) = &fork.chain_hash {
            let _ = write!(text, ", chain hash {hash}");
        }
    }
    text.push('\n');
    let _ = writeln!(
        text,
        "fork operation: {}, requested by {} at {}",
        fork.operation_id,
        fork.requested_by
            .as_deref()
            .map_or_else(|| "an unnamed principal".to_owned(), printable),
        fork.requested_at
    );
    match fork.state {
        "seeding" | "seeded" => {
            let _ = writeln!(
                text,
                "hint: `fleetring db drop {name}` cancels the fork in progress"
            );
        }
        "failed" => {
            let _ = writeln!(text, "hint: `fleetring db drop {name}` removes it");
        }
        _ => {}
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    const OPERATION: [u8; 16] = [0x5A; 16];

    fn record(state: proto::ForkState) -> proto::ForkStatus {
        proto::ForkStatus {
            state: state.into(),
            operation_id: OPERATION.to_vec(),
            source: "notes".to_owned(),
            source_incarnation: 0x3f9a_0c6e_21d4_b870,
            attempt: 2,
            max_attempts: 5,
            holders_needed: 2,
            requested_by: Some("agent-42".to_owned()),
            requested_at_ms: 1_794_219_300_000,
            ..proto::ForkStatus::default()
        }
    }

    fn id() -> String {
        operation_text(&OPERATION)
    }

    #[test]
    fn a_seeding_fork_shows_its_attempt_seeder_exclusions_and_progress() {
        let mut fork = record(proto::ForkState::Seeding);
        fork.seeder = Some("data-3".to_owned());
        fork.seeder_excluded = vec!["data-1".to_owned()];
        fork.disk_refused = vec!["data-4".to_owned()];
        fork.holders = vec!["data-3".to_owned()];
        fork.bytes = Some(41 * 1024 * 1024);
        fork.total_bytes = Some(120 * 1024 * 1024);
        let text = section_text("notes-exp", &status_report(&fork));
        assert_eq!(
            text,
            format!(
                "fork: seeding, attempt 2 of 5, seeder data-3\n\
                 fork exclusions: data-1 (not seeder); data-4 (disk refused)\n\
                 fork holders: data-3, 1 of 2 needed\n\
                 fork progress: 41 MiB of 120 MiB on data-3\n\
                 forked from: notes (incarnation 3f9a0c6e21d4b870)\n\
                 fork operation: {}, requested by agent-42 at 2026-11-09T10:15:00Z\n\
                 hint: `fleetring db drop notes-exp` cancels the fork in progress\n",
                id()
            )
        );
    }

    #[test]
    fn a_seeded_fork_is_not_yet_usable_and_names_its_position() {
        let mut fork = record(proto::ForkState::Seeded);
        fork.holders = vec!["data-3".to_owned(), "data-4".to_owned()];
        fork.position = Some(vec![0x01, 0xa8]);
        fork.chain_hash = Some(vec![0x9f, 0x3c]);
        let report = status_report(&fork);
        let text = section_text("notes-exp", &report);
        assert!(
            text.starts_with("fork: opening: not yet usable\n"),
            "{text}"
        );
        assert!(text.contains("fork holders: data-3, data-4\n"), "{text}");
        assert!(
            text.contains(
                "forked from: notes (incarnation 3f9a0c6e21d4b870) at 01a8, chain hash 9f3c\n"
            ),
            "{text}"
        );
        assert!(text.contains("cancels the fork in progress"), "{text}");
        assert_eq!(column_state(Some(&report)), "opening");
    }

    #[test]
    fn a_ready_fork_has_no_hint_and_a_failed_one_says_it_can_be_removed() {
        let mut ready = record(proto::ForkState::Ready);
        ready.ready_at_ms = Some(1_794_219_400_000);
        let text = section_text("notes-exp", &status_report(&ready));
        assert!(
            text.starts_with("fork: ready, since 2026-11-09T10:16:40Z\n"),
            "{text}"
        );
        assert!(!text.contains("hint:"), "{text}");

        let mut failed = record(proto::ForkState::Failed);
        failed.failure = Some(proto::ForkFailure {
            reason: proto::ForkFailureReason::RetriesExhausted.into(),
            last_cause: proto::RetryCause::SeederDown.into(),
            attempts: 5,
        });
        let report = status_report(&failed);
        let text = section_text("notes-exp", &report);
        assert!(
            text.starts_with(
                "fork: failed, retries ran out; last retry: seeder down (5 attempts)\n"
            ),
            "{text}"
        );
        assert!(
            text.ends_with("hint: `fleetring db drop notes-exp` removes it\n"),
            "{text}"
        );
        assert_eq!(column_state(Some(&report)), "failed");
        assert_eq!(column_state(None), "-");
    }

    #[test]
    fn the_progress_lines_follow_the_fork_to_opening() {
        let mut fork = record(proto::ForkState::Seeding);
        fork.seeder = Some("data-3".to_owned());
        fork.holders = vec!["data-3".to_owned()];
        fork.bytes = Some(41 * 1024 * 1024);
        fork.total_bytes = Some(120 * 1024 * 1024);
        fork.seeder_excluded = vec!["data-1".to_owned()];
        let mut status = proto::ForkOperationStatus {
            state: proto::ForkState::Seeding.into(),
            source: "notes".to_owned(),
            placement: Some(proto::Placement {
                fork: Some(fork),
                ..proto::Placement::default()
            }),
            ..proto::ForkOperationStatus::default()
        };
        assert_eq!(
            progress_line("notes-exp", &status).as_deref(),
            Some(
                "seeding notes-exp from notes: attempt 2 of 5, seeder data-3, 41 MiB of 120 MiB, \
                 holders 1 of 2, not seeders: data-1"
            )
        );
        status.state = proto::ForkState::Seeded.into();
        assert_eq!(
            progress_line("notes-exp", &status).as_deref(),
            Some("opening notes-exp: running the fork reset")
        );
        status.state = proto::ForkState::Ready.into();
        assert_eq!(progress_line("notes-exp", &status), None);
    }

    #[test]
    fn the_result_prints_ready_and_detached_forks() {
        let mut status = proto::ForkOperationStatus {
            operation_id: OPERATION.to_vec(),
            database: "notes-exp".to_owned(),
            incarnation: 0x8c41_d2e0_7b5a_9f13,
            state: proto::ForkState::Seeding.into(),
            source: "notes".to_owned(),
            source_incarnation: 0x3f9a_0c6e_21d4_b870,
            ..proto::ForkOperationStatus::default()
        };
        let detached = fork_report(&status, Duration::from_millis(300));
        let text = result_text(&detached, None);
        assert!(text.contains("state: seeding\n"), "{text}");
        assert!(
            text.contains("position: not yet\nchain hash: not yet\n"),
            "{text}"
        );

        status.state = proto::ForkState::Ready.into();
        status.position = Some(vec![0x01, 0xa8]);
        status.chain_hash = Some(vec![0x9f]);
        let ready = fork_report(&status, Duration::from_millis(12_300));
        let text = result_text(&ready, Some("primary: dev\n"));
        assert_eq!(
            text,
            format!(
                "database: notes-exp\nincarnation: 8c41d2e07b5a9f13\nstate: ready\n\
                 forked from: notes (incarnation 3f9a0c6e21d4b870)\nposition: 01a8\n\
                 chain hash: 9f\noperation: {}\nelapsed: 12.3 s\nprimary: dev\n",
                id()
            )
        );
        assert_eq!(
            ready_line(&ready),
            "\u{2713} notes-exp is ready: forked from notes (incarnation 3f9a0c6e21d4b870) at 01a8 in 12.3 s"
        );
    }

    #[test]
    fn a_cancelled_or_failed_operation_reports_its_outcome() {
        let status = proto::ForkOperationStatus {
            state: proto::ForkState::Cancelled.into(),
            failure: None,
            ..proto::ForkOperationStatus::default()
        };
        let report = fork_report(&status, Duration::ZERO);
        assert_eq!(report.state, "cancelled");
        assert_eq!(report.placement, None);
        assert!(is_final(status.state));
        assert!(!is_final(proto::ForkState::Seeded.into()));
    }

    #[test]
    fn an_unknown_failure_reason_still_reads_as_a_failure() {
        let failure = failure_report(Some(&proto::ForkFailure {
            reason: 99,
            last_cause: 0,
            attempts: 0,
        }));
        assert_eq!(
            failure.map(|failure| failure.reason),
            Some("unknown".to_owned())
        );
    }
}
