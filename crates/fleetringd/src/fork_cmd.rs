//! `fleetring db fork`: copies a database into a new one, and waits until the fork is ready
//! (ADR-0046, `docs/cli.md` section 15).
//!
//! The command makes the operation ID and prints it before it calls the control plane, so a lost
//! reply never leaves the caller without it. The call is sent again under the same ID when its
//! reply is lost ([`fork_flow::submit`]), and the command then follows the operation to `ready`:
//! a seeded fork isn't usable until its fork-reset write commits.

use std::error::Error;
use std::time::Duration;

use clap::Args;
use fleetring_client::output::ForkReport;
use fleetring_control::fork::OperationId;
use fleetring_control::{Acks, DatabaseName};
use fleetring_proto::v1 as proto;
use fleetringd::data::{Client, ForkSpec};
use fleetringd::fork_flow::{
    self, Followed, POLL_INTERVAL, ResendPolicy, SubmitError, difference, refusal_hint, resume_hint,
};
use fleetringd::fork_report::{fork_report, is_final, progress_line, ready_line, result_text};
use ring::rand::{SecureRandom, SystemRandom};
use tokio::time::Instant;
use tonic::{Code, Status};

use crate::exit::{self, Failed};
use crate::format::{self, OutputArgs, OutputFormat};
use crate::progress::{Progress, Quiet, Surroundings};
use crate::{
    MetadataArgs, Target, database_spec, emit, format_location, note, parse_ttl, quiet, shown,
};

/// How long the command waits by default.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// What `db fork --help` adds below the flags.
pub(crate) const FORK_HELP: &str = "\
Examples:
  fleetring db fork notes notes-exp                         fork notes, and wait until the fork is ready
  fleetring db fork notes notes-exp --detach                start the fork, and return
  fleetring db fork notes notes-exp --operation-id 4k7mxq…  wait for the same fork again, or retry it
  fleetring db drop notes-exp                               cancel the fork, or remove it

To cancel a fork in progress, drop its target: fleetring db drop <target>. An operation ID is
kept while its fork is pending and for 7 days after it is ready, failed, or cancelled; after
that it is unknown, and the same command starts a new fork.

Exit codes: 0 ready (or accepted, with --detach); 1 failed, cancelled, or refused; 2 usage; 3
refused scope; 4 the control plane stayed unavailable or --timeout passed; 130 Ctrl-C.";

/// `fleetring db fork`'s arguments.
#[derive(Debug, Args)]
pub(crate) struct ForkArgs {
    /// The database to copy.
    source: DatabaseName,
    /// The new database's name.
    target_name: DatabaseName,
    /// How many replicas the fork has, the primary included
    ///
    /// The fleet's default if unset, as for `db create`; never the source's.
    #[arg(long)]
    replication_factor: Option<u8>,
    /// How many replicas must store a write before it is acknowledged
    ///
    /// Once the fork is ready: `quorum`, `all`, or `local`. The fork's own copy and its fork-reset write need a
    /// majority whatever this says. The fleet's default if unset.
    #[arg(long)]
    acks: Option<Acks>,
    /// How many replicas must be reachable for a write to run
    ///
    /// The fleet's default if unset.
    #[arg(long)]
    min_replicas: Option<u8>,
    #[command(flatten)]
    metadata: MetadataArgs,
    /// Resumes the operation with this ID, or submits one under it
    ///
    /// 26 lowercase base32 characters, as the command printed. If the control plane knows the
    /// operation, the command follows it and resends nothing; the other arguments must match
    /// its request. An ID is valid for 7 days after the fork is ready, failed, or cancelled.
    #[arg(long, value_name = "ID")]
    operation_id: Option<OperationId>,
    /// Returns once the control plane has accepted the fork, which is then seeding.
    #[arg(long)]
    detach: bool,
    /// How long the command waits: `<n>h`, `<n>m`, or `<n>s`; 1h by default
    ///
    /// It limits the wait, not the fork: when it passes, the command exits 4 and the fork
    /// carries on.
    #[arg(long, value_parser = parse_ttl)]
    timeout: Option<Duration>,
    #[command(flatten)]
    output: OutputArgs,
    #[command(flatten)]
    target: Target,
}

/// A new operation ID from the operating system's randomness.
fn new_operation_id() -> Result<OperationId, Box<dyn Error>> {
    let mut bytes = [0_u8; 16];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "the operating system gave no randomness for the operation ID")?;
    Ok(OperationId::from_bytes(bytes))
}

/// The progress shown while the fork is followed: a spinner where one is allowed, and otherwise
/// one line for each change.
struct Following {
    spinner: Option<Progress>,
    lines: bool,
    last: Option<String>,
}

impl Following {
    fn new(surroundings: &Surroundings, format: OutputFormat) -> Self {
        let spinners = surroundings.spinners();
        Self {
            spinner: spinners.then(|| Progress::start("forking\u{2026}", surroundings)),
            lines: !spinners && format == OutputFormat::Table && quiet() == Quiet::No,
            last: None,
        }
    }

    fn show(&mut self, line: String) {
        if self.last.as_deref() == Some(&line) {
            return;
        }
        if let Some(spinner) = &self.spinner {
            spinner.set_message(line.clone());
        } else if self.lines {
            eprintln!("{line}");
        }
        self.last = Some(line);
    }

    fn finish(self) {
        if let Some(spinner) = self.spinner {
            spinner.finish_err();
        }
    }
}

pub(crate) async fn run(args: ForkArgs) -> Result<String, Box<dyn Error>> {
    let started = Instant::now();
    let ForkArgs {
        source,
        target_name,
        replication_factor,
        acks,
        min_replicas,
        mut metadata,
        operation_id,
        detach,
        timeout,
        output,
        target,
    } = args;
    let client = target.client()?;
    let format = output.format();
    let operation = match operation_id {
        Some(operation) => operation,
        None => new_operation_id()?,
    };
    let (id, source_text, target_text) = (
        operation.to_string(),
        source.to_string(),
        target_name.to_string(),
    );
    let mut spec = ForkSpec::new(operation);
    spec.database = database_spec(replication_factor)?;
    if let Some(acks) = acks {
        spec.database = spec.database.with_acks(acks);
    }
    if let Some(min_replicas) = min_replicas {
        spec.database = spec.database.with_min_replicas(min_replicas);
    }
    let resume = |id: &str| resume_hint(id, &shown(&source), &shown(&target_name));
    // The ID comes before the first call, so that nothing loses it (docs/cli.md, section 15).
    if operation_id.is_none() {
        note(format_args!(
            "operation {id}: wait for it again with --operation-id {id}"
        ));
    }

    // A resumed operation the control plane knows is followed, never resent.
    let mut known = None;
    if operation_id.is_some() {
        match client.fork_operation(operation).await {
            Ok(status) => {
                // A description from stdin isn't read again on a resume.
                metadata.drop_description_if_stdin();
                spec.metadata = metadata.clone().read(std::io::stdin().lock())?;
                if let Some(request) = &status.request
                    && let Some(found) = difference(request, &source_text, &target_text, &spec)
                {
                    return Err(Failed::new(&Status::already_exists(format!(
                        "operation {id} was requested with {found}"
                    )))
                    .with_hint(
                        "give the original arguments to resume it, or leave --operation-id out \
                         for a new fork",
                    )
                    .into());
                }
                known = Some(status);
            }
            Err(status) if status.code() == Code::NotFound => {}
            Err(status) => return Err(exit_failure(&status, &resume(&id))),
        }
    }
    if known.is_none() {
        spec.metadata = metadata.read(std::io::stdin().lock())?;
    }
    let first = match known {
        Some(status) => status,
        None => {
            let sent = fork_flow::submit(ResendPolicy::default(), || {
                client.fork_database(&source, &target_name, &spec)
            })
            .await;
            match sent {
                Ok(status) => status,
                Err(SubmitError::Refused(status)) => {
                    let mut failure = Failed::new(&status);
                    if let Some(hint) =
                        refusal_hint(&status, &shown(&source), &shown(&target_name), &id)
                    {
                        failure = failure.with_hint(hint);
                    }
                    return Err(failure.into());
                }
                Err(SubmitError::Unavailable { last, .. }) => {
                    return Err(Failed::new(&last).with_hint(resume(&id)).into());
                }
                Err(other) => return Err(other.into()),
            }
        }
    };

    let status = if detach {
        first
    } else {
        follow_to_the_end(
            &client,
            operation,
            first,
            timeout,
            format,
            &target_name,
            &resume(&id),
        )
        .await?
    };
    let report = fork_report(&status, started.elapsed());
    if !detach && report.state == "ready" && format == OutputFormat::Table {
        note(ready_line(&report));
    }
    let text = placement_text(&client, &target_name, &report).await;
    let rendered = format::render(format, &report, |report| {
        Ok(result_text(report, text.as_deref()))
    })?;
    match report.state {
        "failed" | "cancelled" => {
            emit(&rendered)?;
            let refused = status
                .placement
                .as_ref()
                .and_then(|placement| placement.fork.as_ref())
                .map(|fork| fork.disk_refused.clone())
                .unwrap_or_default();
            eprintln!("{}", outcome_hint(&report, &shown(&target_name), &refused));
            Err(exit::Silent(exit::FAILURE).into())
        }
        _ => Ok(rendered),
    }
}

/// `status` as a command failure, with `hint`.
fn exit_failure(status: &Status, hint: &str) -> Box<dyn Error> {
    Failed::new(status).with_hint(hint).into()
}

/// What to do about a fork that ended badly.
fn outcome_hint(report: &ForkReport, target: &str, disk_refused: &[String]) -> String {
    match report.state {
        "cancelled" => format!(
            "operation {} was cancelled before the fork was ready (dropped or expired); run \
             `fleetring db fork {} {target}` for a new fork",
            report.operation_id, report.source
        ),
        _ => {
            let mut hint = format!(
                "hint: `fleetring db drop {target}` removes the failed fork; then run \
                 `fleetring db fork {} {target}` again.",
                report.source
            );
            let out_of_disk = report
                .failure
                .as_ref()
                .is_some_and(|failure| failure.reason == "insufficient_disk");
            if out_of_disk {
                if !disk_refused.is_empty() {
                    hint.push_str(&format!(" Free disk on {}", disk_refused.join(", ")));
                    hint.push_str(", or fork with a smaller --replication-factor.");
                } else {
                    hint.push_str(" Free disk on the replicas' nodes, or fork with a smaller --replication-factor.");
                }
            }
            hint
        }
    }
}

#[cfg(test)]
mod hint_tests {
    use super::*;

    fn failed(reason: &str) -> ForkReport {
        ForkReport {
            operation_id: "op".to_owned(),
            database: "notes-try".to_owned(),
            incarnation: "0".repeat(16),
            state: "failed",
            source: "notes".to_owned(),
            source_incarnation: "0".repeat(16),
            position: None,
            chain_hash: None,
            failure: Some(fleetring_client::output::ForkFailureReport {
                reason: reason.to_owned(),
                last_cause: None,
                attempts: 1,
            }),
            elapsed_ms: 0,
            placement: None,
        }
    }

    #[test]
    fn a_failed_fork_says_to_drop_it_and_fork_again_with_no_nesting() {
        let hint = outcome_hint(&failed("retries_exhausted"), "notes-try", &[]);
        assert_eq!(
            hint,
            "hint: `fleetring db drop notes-try` removes the failed fork; then run `fleetring db \
             fork notes notes-try` again."
        );
        let disk = outcome_hint(
            &failed("insufficient_disk"),
            "notes-try",
            &["data-2".to_owned()],
        );
        assert!(
            disk.contains("Free disk on data-2, or fork with a smaller"),
            "{disk}"
        );
    }
}

/// `db locate`'s text for the fork, when it exists and is shown to people.
async fn placement_text(
    client: &Client,
    target: &DatabaseName,
    report: &ForkReport,
) -> Option<String> {
    report.placement.as_ref()?;
    client
        .locate(target)
        .await
        .ok()
        .map(|location| format_location(&location))
}

/// Follows `first` until it ends, `timeout` passes, or Ctrl-C, and returns the final status.
async fn follow_to_the_end(
    client: &Client,
    operation: OperationId,
    first: proto::ForkOperationStatus,
    timeout: Option<Duration>,
    format: OutputFormat,
    target: &DatabaseName,
    resume: &str,
) -> Result<proto::ForkOperationStatus, Box<dyn Error>> {
    let surroundings = Surroundings::capture(format.machine(), quiet());
    let mut following = Following::new(&surroundings, format);
    let name = shown(target);
    let interrupted = tokio::signal::ctrl_c();
    tokio::pin!(interrupted);
    let followed = tokio::select! {
        _ = &mut interrupted => {
            following.finish();
            eprintln!(
                "operation {}: the fork carries on. {resume}\n  to cancel it: fleetring db drop {name}",
                operation
            );
            return Err(exit::Silent(exit::INTERRUPTED).into());
        }
        followed = fork_flow::follow(
            first,
            || client.fork_operation(operation),
            POLL_INTERVAL,
            timeout.unwrap_or(DEFAULT_TIMEOUT),
            |status| {
                if let Some(line) = progress_line(&name, status) {
                    following.show(line);
                }
            },
        ) => followed,
    };
    following.finish();
    match followed {
        Ok(Followed::Finished(status)) => {
            debug_assert!(is_final(status.state));
            Ok(status)
        }
        Ok(Followed::TimedOut(_)) => Err(exit_failure(
            &Status::deadline_exceeded("--timeout passed before the fork was ready"),
            &format!("the fork carries on; {resume}"),
        )),
        Ok(_) => Err("the fork's wait ended in a way this release doesn't know".into()),
        Err(status) => Err(exit_failure(&status, resume)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_ids_are_random_and_print_as_26_characters() -> Result<(), Box<dyn Error>> {
        let (a, b) = (new_operation_id()?, new_operation_id()?);
        assert_ne!(a, b);
        assert_eq!(a.to_string().len(), 26);
        Ok(())
    }
}
