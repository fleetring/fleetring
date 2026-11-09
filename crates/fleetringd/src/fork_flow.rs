//! How a fork request is sent, resent and followed (ADR-0046, points 5 and 7).
//!
//! A client makes the operation ID before it asks, so the same request can be sent again, under
//! the same ID, after a lost reply. [`submit`] does that, at most [`MAX_RESENDS`] times within
//! [`RESEND_WINDOW`]; [`follow`] polls the operation until it ends; [`difference`] checks a
//! resumed command's arguments against the operation's recorded request. The functions take the
//! calls as closures, so they run without a daemon.

use std::fmt;
use std::future::Future;
use std::time::Duration;

use fleetring_control::Acks;
use fleetring_proto::v1 as proto;
use tokio::time::{Instant, sleep};
use tonic::{Code, Status};

use crate::data::convert::{acks_from_proto, new_metadata_to_proto};
use crate::data::{DatabaseSpec, ForkSpec};
use crate::fork_report::is_final;

/// How many times a request is sent again after the first send (ADR-0046, point 7).
pub const MAX_RESENDS: u32 = 5;

/// How long the resends may take in all, from the first send.
pub const RESEND_WINDOW: Duration = Duration::from_secs(60);

/// How long an operation's ID is kept after its outcome.
pub const RETENTION_DAYS: u32 = 7;

/// How often a followed operation is read.
pub const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// How [`submit`] resends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResendPolicy {
    /// The most sends after the first.
    pub max_resends: u32,
    /// The longest the resends take, counted from the first send.
    pub window: Duration,
    /// The wait before the first resend; each later one doubles it, up to [`ResendPolicy::window`].
    pub first_pause: Duration,
}

impl Default for ResendPolicy {
    fn default() -> Self {
        Self {
            max_resends: MAX_RESENDS,
            window: RESEND_WINDOW,
            first_pause: Duration::from_millis(250),
        }
    }
}

impl ResendPolicy {
    /// A policy that resends `max_resends` times within `window`, pausing `first_pause` first.
    #[must_use]
    pub const fn new(max_resends: u32, window: Duration, first_pause: Duration) -> Self {
        Self {
            max_resends,
            window,
            first_pause,
        }
    }
}

/// Why [`submit`] returned no operation.
#[derive(Debug)]
#[non_exhaustive]
pub enum SubmitError {
    /// The control plane refused the request; sending it again changes nothing.
    Refused(Status),
    /// Every send failed with a status that a retry may cure.
    Unavailable {
        /// How many times the request was sent.
        sends: u32,
        /// The last status.
        last: Status,
    },
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(status) => write!(f, "{}", status.message()),
            Self::Unavailable { sends, last } => {
                write!(
                    f,
                    "the control plane stayed unavailable after {sends} sends: {}",
                    last.message()
                )
            }
        }
    }
}

impl std::error::Error for SubmitError {}

/// Whether `status` says the reply, or the control plane, may be lost, so that sending the same
/// request again is safe and may help.
#[must_use]
pub fn is_lost(status: &Status) -> bool {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded => true,
        // tonic's own timeout.
        Code::Cancelled => status.message() == "Timeout expired",
        _ => false,
    }
}

/// Sends the request `call` makes, and sends it again under the same operation ID while the
/// answer is lost, as `policy` allows.
///
/// # Errors
///
/// [`SubmitError::Refused`] for a status that isn't [`is_lost`], and
/// [`SubmitError::Unavailable`] once the resends or their window are used up.
pub async fn submit<F, Fut>(
    policy: ResendPolicy,
    mut call: F,
) -> Result<proto::ForkOperationStatus, SubmitError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<proto::ForkOperationStatus, Status>>,
{
    let started = Instant::now();
    let mut pause = policy.first_pause;
    let mut sends = 0;
    loop {
        sends += 1;
        let status = match call().await {
            Ok(operation) => return Ok(operation),
            Err(status) if is_lost(&status) => status,
            Err(status) => return Err(SubmitError::Refused(status)),
        };
        if sends > policy.max_resends || started.elapsed() + pause > policy.window {
            return Err(SubmitError::Unavailable {
                sends,
                last: status,
            });
        }
        sleep(pause).await;
        pause = pause.saturating_mul(2);
    }
}

/// How [`follow`] ended.
#[derive(Debug)]
#[non_exhaustive]
pub enum Followed {
    /// The fork is ready, failed, or was cancelled.
    Finished(proto::ForkOperationStatus),
    /// The wait ran out first; the fork carries on.
    TimedOut(proto::ForkOperationStatus),
}

/// Reads the operation with `poll` every `interval`, from `first`, until it is ready, failed, or
/// cancelled, or `timeout` passes. `seen` sees every status read, `first` included. A read that
/// fails with a status [`is_lost`] says is tried again at the next interval.
///
/// # Errors
///
/// The status of a read that failed for good, such as `NOT_FOUND` for an operation the control
/// plane forgot.
pub async fn follow<F, Fut, S>(
    first: proto::ForkOperationStatus,
    mut poll: F,
    interval: Duration,
    timeout: Duration,
    mut seen: S,
) -> Result<Followed, Status>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<proto::ForkOperationStatus, Status>>,
    S: FnMut(&proto::ForkOperationStatus),
{
    let started = Instant::now();
    let mut last = first;
    loop {
        seen(&last);
        if is_final(last.state) {
            return Ok(Followed::Finished(last));
        }
        if started.elapsed() >= timeout {
            return Ok(Followed::TimedOut(last));
        }
        sleep(interval).await;
        match poll().await {
            Ok(next) => last = next,
            Err(status) if is_lost(&status) => {}
            Err(status) => return Err(status),
        }
    }
}

/// One way a resumed command's arguments differ from the operation's recorded request.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Difference {
    /// A flag or argument with another value.
    Value {
        /// The flag, or the argument's name.
        flag: &'static str,
        /// What the operation recorded.
        recorded: String,
        /// What the command gave.
        given: String,
    },
    /// A flag whose text or members differ; the values stay out of the message.
    Other {
        /// The flag.
        flag: &'static str,
    },
}

impl fmt::Display for Difference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value {
                flag,
                recorded,
                given,
            } => write!(f, "{flag} {recorded}, not {given}"),
            Self::Other { flag } => write!(f, "a different {flag}"),
        }
    }
}

fn acks_text(acks: i32) -> String {
    acks_from_proto(acks).map_or_else(|| "the default".to_owned(), |acks| acks.to_string())
}

/// The first way the command's arguments, which `spec` and the names carry, differ from the
/// `recorded` request, or `None` if they agree. A setting the command left to the fleet's default
/// isn't compared: the recorded request holds the value the control plane filled in.
#[must_use]
pub fn difference(
    recorded: &proto::ForkDatabaseRequest,
    source: &str,
    target: &str,
    spec: &ForkSpec,
) -> Option<Difference> {
    if recorded.source != source {
        return Some(Difference::Value {
            flag: "source",
            recorded: recorded.source.clone(),
            given: source.to_owned(),
        });
    }
    if recorded.database != target {
        return Some(Difference::Value {
            flag: "target",
            recorded: recorded.database.clone(),
            given: target.to_owned(),
        });
    }
    let DatabaseSpec {
        replication_factor,
        acks,
        min_replicas,
        ..
    } = spec.database;
    if let Some(factor) = replication_factor
        && u32::from(factor.get()) != recorded.replication_factor
    {
        return Some(Difference::Value {
            flag: "--replication-factor",
            recorded: recorded.replication_factor.to_string(),
            given: factor.get().to_string(),
        });
    }
    if let Some(acks) = acks
        && acks_from_proto(recorded.acks) != Some(acks)
    {
        return Some(Difference::Value {
            flag: "--acks",
            recorded: acks_text(recorded.acks),
            given: Acks::to_string(&acks),
        });
    }
    if let Some(min) = min_replicas
        && u32::from(min) != recorded.min_replicas
    {
        return Some(Difference::Value {
            flag: "--min-replicas",
            recorded: recorded.min_replicas.to_string(),
            given: min.to_string(),
        });
    }
    if let Some(incarnation) = spec.expect_source_incarnation
        && incarnation.get() != 0
    {
        // The recorded request holds the incarnation the caller required, if any.
        if recorded.expect_source_incarnation != Some(incarnation.get()) {
            return Some(Difference::Other {
                flag: "source incarnation",
            });
        }
    }
    let given = new_metadata_to_proto(&spec.metadata);
    let kept = recorded.metadata.clone().unwrap_or_default();
    if !given.description.is_empty() && given.description != kept.description {
        return Some(Difference::Other {
            flag: "--description",
        });
    }
    if !given.labels.is_empty() {
        let pairs = |labels: &[proto::Label]| {
            let mut pairs: Vec<(String, String)> = labels
                .iter()
                .map(|label| (label.key.clone(), label.value.clone()))
                .collect();
            pairs.sort();
            pairs
        };
        if pairs(&given.labels) != pairs(&kept.labels) {
            return Some(Difference::Other { flag: "--label" });
        }
    }
    if given.expiry.is_some() && given.expiry != kept.expiry {
        return Some(Difference::Other {
            flag: "--expires-in or --expires-at",
        });
    }
    None
}

/// The next step for a refusal of the fork request, or `None` for one with none. `source` and
/// `target` are the command's names; `operation` is its operation ID.
#[must_use]
pub fn refusal_hint(
    status: &Status,
    source: &str,
    target: &str,
    operation: &str,
) -> Option<String> {
    let message = status.message();
    match status.code() {
        Code::PermissionDenied => Some(
            "until 0.8's fork action ships, forking needs admin on the source, and admin on the \
             target; act as a principal that has both (`--as`, `--token-file`, `--context`)"
                .to_owned(),
        ),
        Code::AlreadyExists if message.contains("operation") => Some(format!(
            "operation {operation} was requested with other arguments; resume it with its \
             original arguments, or run the command again for a new fork"
        )),
        Code::AlreadyExists => Some(format!(
            "`fleetring db show {target}` shows what holds the name, or pick another name"
        )),
        Code::NotFound => Some(format!("`fleetring db show {source}` shows the source")),
        Code::FailedPrecondition if message.contains("can't use 'fork' yet") => Some(
            "upgrade the member the message names to a release with forks, then run the command \
             again"
                .to_owned(),
        ),
        Code::FailedPrecondition => Some(format!("`fleetring db show {source}` shows the source")),
        Code::ResourceExhausted => Some(
            "free disk on the node the message names, or fork with a smaller --replication-factor"
                .to_owned(),
        ),
        Code::Unimplemented => {
            Some("the control plane runs a release without forks; upgrade it first".to_owned())
        }
        _ => None,
    }
}

/// How to wait for `operation` again.
#[must_use]
pub fn resume_hint(operation: &str, source: &str, target: &str) -> String {
    format!(
        "resume with `fleetring db fork {source} {target} --operation-id {operation}` (kept for \
         {RETENTION_DAYS} days after the fork finishes; after that, this command starts a new \
         fork)"
    )
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use fleetring::ReplicationFactor;

    use super::*;

    fn operation(state: proto::ForkState) -> proto::ForkOperationStatus {
        proto::ForkOperationStatus {
            state: state.into(),
            ..proto::ForkOperationStatus::default()
        }
    }

    fn quick() -> ResendPolicy {
        ResendPolicy::new(5, Duration::from_secs(60), Duration::from_millis(1))
    }

    #[tokio::test]
    async fn a_lost_reply_is_sent_again_until_it_arrives() {
        let sends = Cell::new(0);
        let result = submit(quick(), || {
            sends.set(sends.get() + 1);
            let attempt = sends.get();
            async move {
                if attempt < 3 {
                    Err(Status::unavailable("gone"))
                } else {
                    Ok(operation(proto::ForkState::Seeding))
                }
            }
        })
        .await;
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(sends.get(), 3);
    }

    #[tokio::test]
    async fn the_resends_stop_after_five() {
        let sends = Cell::new(0);
        let result = submit(quick(), || {
            sends.set(sends.get() + 1);
            async { Err(Status::unavailable("gone")) }
        })
        .await;
        assert!(matches!(
            result,
            Err(SubmitError::Unavailable { sends: 6, .. })
        ));
        assert_eq!(sends.get(), 6, "the first send and five resends");
    }

    #[tokio::test]
    async fn a_refusal_is_not_sent_again() {
        let sends = Cell::new(0);
        let result = submit(quick(), || {
            sends.set(sends.get() + 1);
            async { Err(Status::already_exists("the database exists")) }
        })
        .await;
        assert!(matches!(result, Err(SubmitError::Refused(_))));
        assert_eq!(sends.get(), 1);
    }

    #[tokio::test]
    async fn the_window_bounds_the_resends() {
        let policy = ResendPolicy::new(5, Duration::from_millis(3), Duration::from_millis(2));
        let sends = Cell::new(0);
        let result = submit(policy, || {
            sends.set(sends.get() + 1);
            async { Err(Status::unavailable("gone")) }
        })
        .await;
        assert!(matches!(result, Err(SubmitError::Unavailable { .. })));
        assert!(sends.get() <= 3, "{} sends", sends.get());
    }

    #[tokio::test]
    async fn following_waits_past_seeded_for_ready() {
        let polls = Cell::new(0);
        let mut states = Vec::new();
        let followed = follow(
            operation(proto::ForkState::Seeding),
            || {
                polls.set(polls.get() + 1);
                let state = match polls.get() {
                    1 => proto::ForkState::Seeded,
                    2 => proto::ForkState::Seeded,
                    _ => proto::ForkState::Ready,
                };
                async move { Ok(operation(state)) }
            },
            Duration::from_millis(1),
            Duration::from_secs(5),
            |status| states.push(status.state),
        )
        .await;
        assert!(
            matches!(followed, Ok(Followed::Finished(op)) if op.state == proto::ForkState::Ready as i32)
        );
        assert_eq!(states.len(), 4);
    }

    #[tokio::test]
    async fn following_ends_at_failure_and_at_the_timeout() {
        let failed = follow(
            operation(proto::ForkState::Failed),
            || async { Err(Status::internal("never read")) },
            Duration::from_millis(1),
            Duration::from_secs(5),
            |_| {},
        )
        .await;
        assert!(matches!(failed, Ok(Followed::Finished(_))));
        let timed_out = follow(
            operation(proto::ForkState::Seeded),
            || async { Ok(operation(proto::ForkState::Seeded)) },
            Duration::from_millis(1),
            Duration::from_millis(10),
            |_| {},
        )
        .await;
        assert!(matches!(timed_out, Ok(Followed::TimedOut(_))));
    }

    #[tokio::test]
    async fn following_survives_a_lost_read_and_stops_at_a_forgotten_operation() {
        let polls = Cell::new(0);
        let followed = follow(
            operation(proto::ForkState::Seeding),
            || {
                polls.set(polls.get() + 1);
                let attempt = polls.get();
                async move {
                    if attempt == 1 {
                        Err(Status::unavailable("blip"))
                    } else {
                        Ok(operation(proto::ForkState::Ready))
                    }
                }
            },
            Duration::from_millis(1),
            Duration::from_secs(5),
            |_| {},
        )
        .await;
        assert!(matches!(followed, Ok(Followed::Finished(_))));
        let gone = follow(
            operation(proto::ForkState::Seeding),
            || async { Err(Status::not_found("unknown operation")) },
            Duration::from_millis(1),
            Duration::from_secs(5),
            |_| {},
        )
        .await;
        assert_eq!(gone.map(|_| ()).unwrap_err().code(), Code::NotFound);
    }

    fn recorded() -> proto::ForkDatabaseRequest {
        proto::ForkDatabaseRequest {
            database: "notes-exp".to_owned(),
            source: "notes".to_owned(),
            replication_factor: 3,
            acks: proto::Acks::Quorum.into(),
            min_replicas: 2,
            ..proto::ForkDatabaseRequest::default()
        }
    }

    #[test]
    fn a_resume_naming_what_was_recorded_agrees() {
        let spec = ForkSpec::new(fleetring_control::fork::OperationId::from_bytes([1; 16]));
        assert_eq!(difference(&recorded(), "notes", "notes-exp", &spec), None);
    }

    #[test]
    fn a_resume_naming_another_replication_factor_names_it() {
        let mut spec = ForkSpec::new(fleetring_control::fork::OperationId::from_bytes([1; 16]));
        spec.database = DatabaseSpec::new(ReplicationFactor::new(5).expect("5 is a factor"));
        let found = difference(&recorded(), "notes", "notes-exp", &spec).expect("a difference");
        assert_eq!(found.to_string(), "--replication-factor 3, not 5");
    }

    #[test]
    fn a_resume_for_other_names_or_text_is_a_difference() {
        let mut spec = ForkSpec::new(fleetring_control::fork::OperationId::from_bytes([1; 16]));
        assert_eq!(
            difference(&recorded(), "other", "notes-exp", &spec)
                .expect("source")
                .to_string(),
            "source notes, not other"
        );
        assert_eq!(
            difference(&recorded(), "notes", "other", &spec)
                .expect("target")
                .to_string(),
            "target notes-exp, not other"
        );
        spec.metadata.description = Some("hello".parse().expect("a description"));
        assert_eq!(
            difference(&recorded(), "notes", "notes-exp", &spec)
                .expect("description")
                .to_string(),
            "a different --description"
        );
    }

    #[test]
    fn refusals_have_hints() {
        let hint = |status| refusal_hint(&status, "notes", "notes-exp", "abc");
        assert!(
            hint(Status::permission_denied("no"))
                .is_some_and(|h| h.contains("admin on the source"))
        );
        assert!(
            hint(Status::already_exists("database exists"))
                .is_some_and(|h| h.contains("db show notes-exp"))
        );
        assert!(
            hint(Status::already_exists("operation id reused"))
                .is_some_and(|h| h.contains("original arguments"))
        );
        assert!(hint(Status::unimplemented("x")).is_some_and(|h| h.contains("upgrade it first")));
        assert!(hint(Status::internal("x")).is_none());
    }
}
