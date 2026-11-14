//! `fleetring agent delegate` and `fleetring agent list --fleet` (ADR-0047, points 5, 6.4 and
//! 10; `docs/cli.md`, section 16).
//!
//! `agent delegate` acts as the parent, with the parent's token, over a direct connection with
//! the root agent's certificate. It writes the operation ID into the child's directory before it
//! asks, and sends the same request again under it while the answer is lost, so a resend, or a
//! run after a crash, never records a second delegation. The child's token reaches only its own
//! file, written `0600`: never stdout, a log, or an error.

use std::error::Error;
use std::fmt::Write as _;
use std::future::Future;
use std::time::Duration;

use clap::Args;
use fleetring_client::output::{ChainLinkReport, DelegationList, DelegationReport};
use fleetring_control::Timestamp;
use fleetring_control::fork::OperationId;
use fleetring_proto::v1::{DelegateTokenRequest, DelegateTokenResponse, ListDelegationsRequest};
use fleetringd::auth::{Action, BearerToken, Scope};
use fleetringd::fork_flow::{ResendPolicy, is_lost};
use fleetringd::report::table;
use fleetringd::transport::Backoff;
use tokio::time::{Instant, sleep};
use tonic::{Code, Status};

use super::{Listed, TRUSTED_LOCAL, agent_json, situation};
use crate::agent::{
    AgentDir, AgentRecord, AgentRef, DelegatedDir, DelegatedId, Delegation, OperationFrom,
    ScopesFrom, id_of,
};
use crate::exit::{self, Coded};
use crate::format::{self, OutputArgs, OutputFormat};
use crate::{Target, format_time, note, parse_ttl};

/// What `agent delegate --help` shows below the flags (`docs/cli.md`, section 16).
pub(super) const AFTER_HELP: &str = "\
Examples:
  fleetring agent delegate 42.c7                                   a child with the parent's actions on agent-42.c7-*
  fleetring agent delegate 42.c7 --scope admin:agent-42.c7-* \\
      --scope fork:agent-42-memory                                 a child that may fork the parent's memory
  fleetring --agent 42.c7 db create agent-42.c7-scratch            act as the child
  fleetring agent remove 42.c7                                     revoke the child and every agent below it

The command writes the operation ID into the child's directory before it asks, and sends the
same request again under it, up to 5 times within 60 seconds, while the control plane is
unavailable or the reply is lost. Run it again after a crash or an exit code of 4: it finds the
operation ID and resends it, and never records a second delegation.

Exit codes: 0 on success; 1 when refused for any reason but scope; 2 for usage; 3 for a refused
scope; 4 when the control plane stays unavailable after the resends.";

/// `fleetring agent delegate`'s arguments.
#[derive(Debug, Args)]
pub(crate) struct AgentDelegateArgs {
    /// The child's ID: its parent's followed by `.<segment>`, such as `42.c7`
    ///
    /// The segment is 1 to 32 lowercase letters, digits, or '_'. The child's principal,
    /// `agent-<id>`, is at most 64 bytes, at most 8 levels below the root.
    #[arg(value_name = "CHILD_ID")]
    id: DelegatedId,
    /// A scope the child gets instead of the defaults
    ///
    /// `<action>:<pattern>`. Each must be covered whole by the parent's scopes and its `grant`
    /// patterns, and stay inside the root's databases, with any `grant` only below the child.
    /// Without it, the child gets the parent's own actions on `agent-<child-id>-*`, among
    /// `admin` and `stream`, and no `grant`. Repeat it for more.
    #[arg(long = "scope", value_name = "SCOPE")]
    scopes: Vec<Scope>,
    /// How long the child's token lasts: `<n>h`, `<n>m`, or `<n>s`
    ///
    /// From 1 minute to 24 hours. Without it, 2 hours, or less if the parent's token ends sooner, and the result says so.
    /// One past the parent's token is refused, never capped.
    #[arg(long, value_parser = parse_ttl)]
    ttl: Option<Duration>,
    /// When the child's token starts, RFC 3339
    ///
    /// At most 30 seconds ago, and at most 24 hours ahead; by default, now.
    #[arg(long, value_name = "TIME", value_parser = crate::parse_time)]
    not_before: Option<u64>,
    /// Asks under this operation ID instead of a new one
    ///
    /// 26 lowercase base32 characters, as an earlier run printed it. The same ID with another child, other scopes, another `--ttl`, or another
    /// `--not-before` is refused, naming the flag.
    #[arg(long, value_name = "ID")]
    operation_id: Option<OperationId>,
    #[command(flatten)]
    output: OutputArgs,
    #[command(flatten)]
    target: Target,
}

/// `agent delegate`: asks for the child's token as its parent, and writes the child's identity.
pub(super) async fn run(args: AgentDelegateArgs) -> Result<String, Box<dyn Error>> {
    let AgentDelegateArgs {
        id,
        scopes,
        ttl,
        not_before,
        operation_id,
        output,
        mut target,
    } = args;
    let situation = situation(&target)?;
    let tls = &target.tls;
    if tls.tls_dir.is_some()
        || tls.cert.is_some()
        || tls.key.is_some()
        || tls.principal.is_some()
        || tls.token.is_some()
        || tls.token_file.is_some()
        || tls.insecure_plaintext
    {
        return Err(exit::Usage(
            "`agent delegate` acts as the parent, with the parent's token and its root's \
             certificate; leave out --tls-dir, --cert, --key, --as, --token, --token-file and \
             --insecure-plaintext"
                .to_owned(),
        )
        .into());
    }
    for warning in scopes.iter().filter_map(Scope::separator_warning) {
        note(format!("warning: {warning}"));
    }
    let parent = id.parent();
    let parent_record = parent_record(&situation.agents_dir, &parent)?;
    let dir = DelegatedDir::new(&situation.agents_dir, id.clone());
    if dir.record()?.is_some() {
        return Err(crate::agent::AgentError::DelegatedExists {
            id: id.to_string(),
            dir: dir.dir().to_owned(),
        }
        .into());
    }
    // As the parent: the root's certificate and the parent's token.
    target.tls.agent = Some(parent.clone());
    let client = target.client_or(situation.dev_control())?;

    let pending = dir.begin(operation_id)?;
    let operation = pending.operation();
    match pending.from() {
        OperationFrom::Earlier => note(format!(
            "resending operation {operation}, which an earlier run left in {}",
            dir.dir().display()
        )),
        OperationFrom::New => note(format!(
            "operation {operation}: if this command is interrupted, run it again to resend it"
        )),
    }
    let request = DelegateTokenRequest {
        operation_id: operation.to_bytes().to_vec(),
        segment: id.segment().to_owned(),
        scopes: scopes.iter().map(ToString::to_string).collect(),
        lifetime_ms: ttl.map(|ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
        not_before_ms: not_before,
    };
    // This command resends; the client mustn't as well.
    let control = client.control().clone().with_backoff(Backoff {
        attempts: 1,
        ..Backoff::default()
    });
    let answer = send(ResendPolicy::default(), || {
        control.delegate_token(request.clone())
    })
    .await;
    let response = match answer {
        Ok(response) => response,
        Err(unanswered) => {
            // A definitive refusal recorded nothing, so the next run asks under a new operation
            // ID; anything else keeps it, to be sent again.
            if unanswered.gives_up() {
                dir.abandon(pending)?;
            }
            let refusal = Refusal {
                id: &id,
                parent: &parent,
                operation,
                dir: &dir,
                rerun: format!(
                    "`fleetring agent delegate {id} --operation-id {operation}` with the same flags"
                ),
            };
            return Err(refusal.of(unanswered).into());
        }
    };
    let DelegateTokenResponse {
        token,
        delegation_id,
        principal,
        scopes: granted,
        expires_at_ms,
        capped,
        issuer_key_id,
        ..
    } = response;
    let read_only = parent_record.read_only || !writes(&granted);
    let delegation = Delegation {
        token: BearerToken::new(token),
        principal,
        delegation_id,
        scopes: granted,
        scopes_from: if scopes.is_empty() {
            ScopesFrom::Defaults
        } else {
            ScopesFrom::Flags
        },
        read_only,
        expires_at: Timestamp::from_millis(expires_at_ms),
        issuer_key_id,
        parent: parent_record,
    };
    let record = dir.finish(pending, delegation)?;
    let listed = Listed::Delegated(dir);
    let mut json = agent_json(&listed, &record);
    json["capped"] = serde_json::Value::Bool(capped);
    let shown = serde_json::json!({ "agents": [json] });
    format::render(output.format(), &shown, |_| {
        Ok(delegated_text(&listed, &record, capped))
    })
}

/// The record of `parent`, the identity `agent delegate` acts as.
pub(super) fn parent_record(
    agents_dir: &std::path::Path,
    parent: &AgentRef,
) -> Result<AgentRecord, Box<dyn Error>> {
    match parent {
        AgentRef::Root(id) => {
            let dir = AgentDir::new(agents_dir, id.clone());
            dir.record()?.ok_or_else(|| {
                format!(
                    "the parent, agent '{id}', has no identity in {}\n  hint: a child's parent \
                     must be an identity here: create a root agent with `fleetring agent add \
                     {id}`",
                    dir.tls_dir().display()
                )
                .into()
            })
        }
        AgentRef::Delegated(id) => Ok(DelegatedDir::new(agents_dir, id.clone()).usable()?),
    }
}

/// Whether `scopes` hold an action that writes: `write`, `admin`, or `fork`. A child that holds
/// none is read-only, and `mcp --agent` offers it only the reading tools.
pub(super) fn writes(scopes: &[String]) -> bool {
    scopes.iter().any(|scope| {
        scope.parse::<Scope>().is_ok_and(|scope| {
            matches!(
                scope.action(),
                Some(Action::Write | Action::Admin | Action::Fork)
            )
        })
    })
}

/// What `agent delegate` prints for a person: key and value lines, then the trusted local
/// development line (`docs/cli.md`, section 16).
fn delegated_text(listed: &Listed, record: &AgentRecord, capped: bool) -> String {
    let mut text = String::new();
    let mut line = |key: &str, value: &str| {
        let _ = writeln!(text, "{key}: {value}");
    };
    line("agent", listed.id().as_str());
    line("principal", &record.principal);
    line("parent", record.parent.as_deref().map_or("-", id_of));
    line("root", record.root.as_deref().map_or("-", id_of));
    line("owner", &record.owner);
    line("scopes", &record.scopes.join(", "));
    let capped = if capped {
        " (capped at the parent's expiry)"
    } else {
        ""
    };
    line(
        "token expires",
        &format!("{}{capped}", record.token_expires_at),
    );
    line("delegation", &record.token_id);
    line("operation", record.operation.as_deref().unwrap_or("-"));
    text.push_str(TRUSTED_LOCAL);
    text.push('\n');
    text
}

/// Why [`send`] returned no answer.
#[derive(Debug)]
pub(super) enum Unanswered {
    /// The control plane refused the request; sending it again changes nothing.
    Refused(Status),
    /// Every send was lost or answered unavailable.
    Lost {
        /// How many times the request was sent.
        sends: u32,
        /// The last status.
        last: Status,
    },
}

impl Unanswered {
    /// Whether the operation ID is given up, so the next run asks under a new one: only after a
    /// refusal that says the request recorded nothing and can never be recorded under this ID.
    /// That is a definitive refusal, or a conflict over what the ID can't be resent with: another
    /// child, or another parent token or copy of it. A conflict over the flags keeps the ID, to
    /// be resent with the flags it was asked with; a lost answer, or a status that doesn't say
    /// whether the delegation was recorded, keeps it too.
    pub(super) fn gives_up(&self) -> bool {
        match self {
            Self::Lost { .. } => false,
            Self::Refused(status) => match status.code() {
                Code::AlreadyExists => matches!(
                    conflicting_field(status.message()),
                    Some(FIELD_SEGMENT | FIELD_CALLER_TOKEN | FIELD_CALLER_KEY)
                ),
                code => definitive(code),
            },
        }
    }
}

/// Whether a refusal with `code` says that the request recorded nothing: every refusal the
/// control plane gives a delegation, which `apply` answers before anything is logged. Any other
/// code, `UNKNOWN` or `INTERNAL` among them, may follow a delegation that was recorded.
fn definitive(code: Code) -> bool {
    matches!(
        code,
        Code::PermissionDenied
            | Code::InvalidArgument
            | Code::ResourceExhausted
            | Code::FailedPrecondition
            | Code::Unauthenticated
            | Code::NotFound
            | Code::OutOfRange
            | Code::Unimplemented
    )
}

/// The fields of a conflicting operation that a resend can't change: the child, and the copy of
/// the parent's token it was asked with (`DelegationConflict`'s field names).
const FIELD_SEGMENT: &str = "segment";
const FIELD_CALLER_TOKEN: &str = "caller token";
const FIELD_CALLER_KEY: &str = "caller token's signing key";

/// Sends the request `call` makes, and sends it again, unchanged and so under the same
/// operation ID, while the answer is lost or the control plane unavailable, as `policy` allows:
/// at most [`ResendPolicy::max_resends`] times within [`ResendPolicy::window`] (ADR-0047, point
/// 10). A name revoked less than a second ago is refused `UNAVAILABLE`, so a resend retries it
/// too.
pub(super) async fn send<T, F, Fut>(policy: ResendPolicy, mut call: F) -> Result<T, Unanswered>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Status>>,
{
    let started = Instant::now();
    let mut pause = policy.first_pause;
    let mut sends = 0;
    loop {
        sends += 1;
        let status = match call().await {
            Ok(answer) => return Ok(answer),
            Err(status) if is_lost(&status) => status,
            Err(status) => return Err(Unanswered::Refused(status)),
        };
        if sends > policy.max_resends || started.elapsed() + pause > policy.window {
            return Err(Unanswered::Lost {
                sends,
                last: status,
            });
        }
        tracing::debug!(sends, code = ?status.code(), "resending a delegation after {pause:?}");
        sleep(pause).await;
        pause = pause.saturating_mul(2);
    }
}

/// A delegation that wasn't answered, as the command reports it.
pub(super) struct Refusal<'a> {
    pub(super) id: &'a DelegatedId,
    pub(super) parent: &'a AgentRef,
    pub(super) operation: OperationId,
    pub(super) dir: &'a DelegatedDir,
    /// The command that resends the operation, as the hint names it.
    pub(super) rerun: String,
}

impl Refusal<'_> {
    /// The error `unanswered` ends the command with: the status, the next step, and the exit
    /// code `docs/cli.md` gives it in section 16.
    pub(super) fn of(&self, unanswered: Unanswered) -> Coded {
        let (status, lost) = match unanswered {
            Unanswered::Refused(status) => (status, None),
            Unanswered::Lost { sends, last } => (last, Some(sends)),
        };
        let (code, hint) = match lost {
            None if !definitive(status.code()) && status.code() != Code::AlreadyExists => (
                exit::UNAVAILABLE,
                Some(format!(
                    "the answer doesn't say whether the delegation was recorded; operation {} is \
                     kept in {}, so running {} resends it and never records a second delegation",
                    self.operation,
                    self.dir.dir().display(),
                    self.rerun,
                )),
            ),
            Some(sends) => (
                exit::UNAVAILABLE,
                Some(format!(
                    "the control plane stayed unavailable after {sends} sends; operation {} is \
                     kept in {}, so running {} resends it and never records a second delegation",
                    self.operation,
                    self.dir.dir().display(),
                    self.rerun,
                )),
            ),
            None => self.refused(&status),
        };
        let mut message = exit::describe(&status);
        if let Some(hint) = hint {
            message.push_str("\n  hint: ");
            message.push_str(&hint);
        }
        Coded { code, message }
    }

    /// The exit code and next step for `status`, a refusal.
    fn refused(&self, status: &Status) -> (u8, Option<String>) {
        let message = status.message();
        let parent = self.parent;
        let child = self.id.name();
        match status.code() {
            Code::PermissionDenied if message.starts_with("your token holds no grant") => (
                exit::REFUSED,
                Some(format!(
                    "agent {parent} needs a grant scope that covers {child}-*, such as \
                     grant:{child}-* or grant:{}.*; a root agent minted once every node reports \
                     delegation holds one",
                    agent_principal(parent)
                )),
            ),
            Code::PermissionDenied
                if message.starts_with("your token doesn't cover")
                    || message.starts_with("the child isn't a narrowing") =>
            {
                (
                    exit::REFUSED,
                    Some(format!(
                        "ask only for scopes agent {parent} holds and may grant; `fleetring agent \
                         show {parent}` lists them"
                    )),
                )
            }
            Code::PermissionDenied if message.contains("doesn't say it is one") => (
                exit::FAILURE,
                Some(format!(
                    "agent {parent}'s token was minted before every node reported delegation; \
                     run `fleetring agent renew {}`",
                    self.id.root()
                )),
            ),
            Code::Unauthenticated => (exit::REFUSED, exit::hint(status)),
            Code::AlreadyExists => (
                exit::FAILURE,
                Some(match conflicting_field(message) {
                    Some(field @ (FIELD_SEGMENT | FIELD_CALLER_TOKEN | FIELD_CALLER_KEY)) => {
                        format!(
                            "operation {} was asked for with another {}, so it can't be resent; \
                             running the command again without --operation-id asks under a new \
                             one",
                            self.operation,
                            flag_of(field).unwrap_or(field)
                        )
                    }
                    Some(field) => format!(
                        "operation {} was asked for with another {}; give the flags it was asked \
                         with, or delete {} to ask under a new operation ID",
                        self.operation,
                        flag_of(field).unwrap_or(field),
                        self.dir.operation_file().display()
                    ),
                    None => format!(
                        "operation {} was asked for with other flags; give those, or delete {} \
                         to ask under a new operation ID",
                        self.operation,
                        self.dir.operation_file().display()
                    ),
                }),
            ),
            Code::FailedPrecondition if message.contains("no delegation issuer") => (
                exit::FAILURE,
                Some(
                    "run `fleetring cert issuer --name <control node> --dir <authority dir>`, \
                     then set [control] issuer_cert and issuer_key on the control node"
                        .to_owned(),
                ),
            ),
            Code::FailedPrecondition if message.contains("can't use 'delegation' yet") => (
                exit::FAILURE,
                Some(
                    "upgrade the member the message names; every member must report the \
                     delegation feature"
                        .to_owned(),
                ),
            ),
            Code::ResourceExhausted => (
                exit::FAILURE,
                Some(
                    "revoking a delegated agent frees its slot; a rate limit reopens within a \
                     minute"
                        .to_owned(),
                ),
            ),
            Code::InvalidArgument if message.contains("lasts") || message.contains("lifetime") => {
                (exit::FAILURE, Some("change --ttl".to_owned()))
            }
            Code::InvalidArgument if message.contains("not_before") => {
                (exit::FAILURE, Some("change --not-before".to_owned()))
            }
            _ => (exit::FAILURE, None),
        }
    }
}

/// The field a conflicting operation names (`operation … was requested with a different
/// <field>`), or `None`.
fn conflicting_field(message: &str) -> Option<&str> {
    message
        .rsplit_once("was requested with a different ")
        .map(|(_, field)| field)
}

/// The flag, or what the command line calls it, of a conflicting operation's `field`.
fn flag_of(field: &str) -> Option<&'static str> {
    Some(match field {
        FIELD_SEGMENT => "child ID",
        "scopes" => "--scope",
        "lifetime" => "--ttl",
        "not_before" => "--not-before",
        FIELD_CALLER_TOKEN => "parent token, renewed since",
        FIELD_CALLER_KEY => "copy of the parent's token, signed by another issuer key",
        _ => return None,
    })
}

/// The principal of `agent`, for a hint.
fn agent_principal(agent: &AgentRef) -> String {
    agent.principal()
}

/// `agent list --fleet`: the fleet's live delegation records, never tokens (ADR-0047, point
/// 6.4), by the control node that recorded them and from when.
pub(super) async fn list_fleet(
    recorded_by: Option<String>,
    since: Option<u64>,
    format: OutputFormat,
    target: &Target,
) -> Result<String, Box<dyn Error>> {
    let situation = situation(target)?;
    let client = target.client_or(situation.dev_control())?;
    note(
        "note: each record names the control node that recorded it as the leader stamped it; \
         after a compromise, list with --since over the whole time the node may have led, not \
         only --recorded-by",
    );
    let request = ListDelegationsRequest {
        recorded_by,
        recorded_from_ms: since,
        recorded_to_ms: None,
    };
    let response = client
        .control()
        .list_delegations(request)
        .await
        .map_err(crate::failed)?;
    let delegations = response
        .delegations
        .into_iter()
        .map(|record| DelegationReport {
            principal: record.principal,
            delegation_id: record.delegation_id,
            parent: record.parent,
            chain: record
                .chain
                .into_iter()
                .map(|link| ChainLinkReport {
                    id: link.id,
                    kid: link.kid,
                })
                .collect(),
            issued_at: format_time(record.issued_at_ms),
            expires_at: format_time(record.expires_at_ms),
            recorded_by: record.recorded_by,
            recorded_at: format_time(record.recorded_at_ms),
        })
        .collect();
    let list = DelegationList { delegations };
    format::render(format, &list, |list| {
        let rows: Vec<[String; 6]> = list
            .delegations
            .iter()
            .map(|record| {
                [
                    record.principal.clone(),
                    record.parent.clone(),
                    record.delegation_id.clone(),
                    record.expires_at.clone(),
                    record.recorded_by.clone(),
                    record.recorded_at.clone(),
                ]
            })
            .collect();
        Ok(table(
            [
                "PRINCIPAL",
                "PARENT",
                "DELEGATION",
                "EXPIRES",
                "RECORDED BY",
                "RECORDED AT",
            ],
            &rows,
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> ResendPolicy {
        ResendPolicy::default()
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_answer_is_sent_again_at_most_five_times_within_a_minute() {
        let mut sends = 0;
        let started = Instant::now();
        let answer: Result<(), Unanswered> = send(policy(), || {
            sends += 1;
            async { Err(Status::unavailable("no leader")) }
        })
        .await;
        assert!(
            matches!(answer, Err(Unanswered::Lost { sends: 6, .. })),
            "{answer:?}"
        );
        assert_eq!(sends, 6, "the first send and 5 resends");
        assert!(started.elapsed() <= Duration::from_secs(60));
    }

    #[tokio::test(start_paused = true)]
    async fn a_name_revoked_a_moment_ago_is_retried_until_it_works() {
        let mut sends = 0;
        let answer = send(policy(), || {
            sends += 1;
            let attempt = sends;
            async move {
                if attempt < 3 {
                    Err(Status::unavailable(
                        "the child's name was revoked less than a second ago; retry",
                    ))
                } else {
                    Ok(attempt)
                }
            }
        })
        .await;
        assert!(matches!(answer, Ok(3)), "{answer:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_refusal_is_never_sent_again() {
        let mut sends = 0;
        let answer: Result<(), Unanswered> = send(policy(), || {
            sends += 1;
            async {
                Err(Status::permission_denied(
                    "your token holds no grant over x-",
                ))
            }
        })
        .await;
        assert!(matches!(answer, Err(Unanswered::Refused(_))), "{answer:?}");
        assert_eq!(sends, 1);
    }

    #[test]
    fn refusals_exit_3_for_scope_1_for_anything_else_and_4_when_unavailable()
    -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        let id: DelegatedId = "42.c7".parse()?;
        let dir = DelegatedDir::new(root.path(), id.clone());
        let parent = id.parent();
        let refusal = Refusal {
            id: &id,
            parent: &parent,
            operation: OperationId::from_bytes([1; 16]),
            dir: &dir,
            rerun: format!(
                "`fleetring agent delegate 42.c7 --operation-id {}` with the same flags",
                OperationId::from_bytes([1; 16])
            ),
        };
        let code = |status: Status| refusal.of(Unanswered::Refused(status)).code;
        assert_eq!(
            code(Status::permission_denied(
                "your token holds no grant over agent-42.c7-"
            )),
            exit::REFUSED
        );
        assert_eq!(
            code(Status::permission_denied(
                "your token doesn't cover admin:agent-42-memory with both its scopes and its grant"
            )),
            exit::REFUSED
        );
        assert_eq!(
            code(Status::permission_denied(
                "delegation needs a direct connection with the certificate of the caller or an \
                 ancestor"
            )),
            exit::FAILURE
        );
        assert_eq!(
            code(Status::invalid_argument(
                "a delegated token lasts at most 24 h"
            )),
            exit::FAILURE
        );
        assert_eq!(
            code(Status::resource_exhausted("too many delegations: 256")),
            exit::FAILURE
        );
        let renew = refusal.of(Unanswered::Refused(Status::permission_denied(
            "only an agent's token can delegate, and this token doesn't say it is one",
        )));
        assert_eq!(renew.code, exit::FAILURE);
        assert!(
            renew.message.contains("fleetring agent renew 42"),
            "{}",
            renew.message
        );
        let conflict = refusal.of(Unanswered::Refused(Status::already_exists(
            "operation x was requested with a different scopes",
        )));
        assert!(conflict.message.contains("--scope"), "{}", conflict.message);
        assert!(
            conflict.message.contains("operation`") || conflict.message.contains("/operation"),
            "names the file to delete: {}",
            conflict.message
        );
        let fixed = refusal.of(Unanswered::Refused(Status::already_exists(
            "operation x was requested with a different caller token's signing key",
        )));
        assert!(
            fixed.message.contains("can't be resent"),
            "{}",
            fixed.message
        );
        // A status that doesn't say whether the delegation was recorded keeps the ID: exit 4.
        for status in [
            Status::unknown("transport error"),
            Status::internal("x"),
            Status::aborted("x"),
        ] {
            let ambiguous = refusal.of(Unanswered::Refused(status));
            assert_eq!(ambiguous.code, exit::UNAVAILABLE, "{}", ambiguous.message);
            assert!(
                ambiguous.message.contains("doesn't say whether"),
                "{}",
                ambiguous.message
            );
        }
        let lost = refusal.of(Unanswered::Lost {
            sends: 6,
            last: Status::unavailable("no leader"),
        });
        assert_eq!(lost.code, exit::UNAVAILABLE);
        assert!(
            lost.message.contains(&format!(
                "--operation-id {}",
                OperationId::from_bytes([1; 16])
            )),
            "{}",
            lost.message
        );
        Ok(())
    }

    #[test]
    fn only_a_definitive_refusal_or_an_unresendable_conflict_gives_the_operation_up() {
        let refused = |status: Status| Unanswered::Refused(status).gives_up();
        assert!(refused(Status::permission_denied("x")));
        assert!(refused(Status::invalid_argument("x")));
        assert!(refused(Status::resource_exhausted("x")));
        assert!(refused(Status::failed_precondition("x")));
        assert!(refused(Status::unauthenticated("x")));
        for field in ["segment", "caller token", "caller token's signing key"] {
            assert!(
                refused(Status::already_exists(format!(
                    "operation x was requested with a different {field}"
                ))),
                "{field}"
            );
        }
        for field in ["scopes", "lifetime", "not_before"] {
            assert!(
                !refused(Status::already_exists(format!(
                    "operation x was requested with a different {field}"
                ))),
                "{field}"
            );
        }
        assert!(!refused(Status::unknown("transport error")));
        assert!(!refused(Status::internal("x")));
        assert!(!refused(Status::aborted("x")));
        assert!(!refused(Status::cancelled("x")));
        assert!(
            !Unanswered::Lost {
                sends: 6,
                last: Status::unavailable("x"),
            }
            .gives_up()
        );
    }

    #[test]
    fn a_child_without_a_writing_scope_is_read_only() {
        let texts = |scopes: &[&str]| -> Vec<String> {
            scopes.iter().map(|&scope| scope.to_owned()).collect()
        };
        assert!(!writes(&texts(&[
            "read:agent-42.c7-*",
            "stream:agent-42.c7-*"
        ])));
        assert!(writes(&texts(&["admin:agent-42.c7-*"])));
        assert!(writes(&texts(&["fork:agent-42-memory"])));
    }
}
