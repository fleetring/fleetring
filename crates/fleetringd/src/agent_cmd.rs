//! `fleetring agent add | list | show | renew | remove`, and what `fleetring mcp --agent` does
//! before it serves (ADR-0043, point 8).
//!
//! Minting happens only in `mcp --agent`, `agent add`, and `agent renew`. From dev mode's
//! authority it needs [`dev::check_dev_dir`] to pass first, whatever the contexts file says; from
//! any other it needs `--authority`. Nothing here prints a token or a key, and the configuration
//! `--print-config` prints is only the command, since `mcp --agent` finds its own credentials.
//!
//! This is trusted local development: agents running as the same user can read each other's
//! credentials (ADR-0044, point 11), and the output says so.

use std::error::Error;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use clap_complete::engine::ArgValueCandidates;
use fleetring_client::context::{Context, ContextError, ContextName, DEV_NAME, select};
use fleetring_control::Timestamp;
use fleetring_proto::v1::{DelegateTokenRequest, DelegateTokenResponse};
use fleetringd::Network;
use fleetringd::auth::{Action, BearerToken, MAX_TOKEN_LIFETIME, Scope};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::data::Client;
use fleetringd::dev::{self, Detached, DevChecked};
use fleetringd::fork_flow::ResendPolicy;
use fleetringd::mcp::Access;
use fleetringd::report::table;
use fleetringd::tls::{Identity, Security};
use fleetringd::transport::Backoff;
use fleetringd::upgrade::{Running, running};
use tonic::Code;

use crate::agent::{
    self, AUTHORITY_CERT_LIFETIME, Acquired, AgentDir, AgentError, AgentId, AgentRecord, AgentRef,
    DEV_CERT_LIFETIME, DelegatedDir, DelegatedId, Delegation, HeldDefaults, IssuedBy, Issuer,
    MintSpec, Minting, OldToken, OperationFrom, RENEW_TOKEN_WITHIN, Renewal, Renewed, ScopeRenewal,
    ScopesFrom, Support, delegation_support,
};

mod delegate;
pub(crate) mod revoke;
use self::delegate::Unanswered;
use crate::confirm::{self, Place};
use crate::exit;
use crate::format::{self, OutputArgs, OutputFormat};
use crate::progress::{MachineOutput, Progress};
use crate::resolve::EnvSnapshot;
use crate::{
    Target, completions, context_cmd, dev_cmd, format_time, note, parse_ttl, spinner_surroundings,
};

/// What the output says about isolation (ADR-0044, point 11.4; #114).
const TRUSTED_LOCAL: &str =
    "trusted local development: agents on this machine can read each other's credentials";

/// How long a revocation may take before the command gives up on it.
const REVOKE_TIMEOUT: Duration = Duration::from_secs(10);

/// `fleetring agent`'s commands.
#[derive(Debug, Subcommand)]
pub(crate) enum AgentCommand {
    /// Mints an agent's identity: a certificate and a scoped token
    ///
    /// A certificate for `agent-<id>` and a token scoped `admin:agent-<id>-*` and
    /// `stream:agent-<id>-*`, kept private in a directory of its own. Once every node reports
    /// the `delegation` feature, the token also holds `admin`, `stream`, and `grant` on
    /// `agent-<id>.*` and `grant` on `agent-<id>-*`, and says it is an agent's, so the agent can
    /// delegate. In dev mode it starts the dev node if none runs, and signs with dev mode's
    /// authority; for another fleet, pass `--authority`.
    Add(AgentAddArgs),
    /// Asks the control plane for a delegated agent's token, as its parent
    ///
    /// The parent is the child's ID less its last segment, `42` for `42.c7`, and must have an
    /// identity here. The child gets a narrower, shorter-lived token, recorded by the control
    /// plane, and an identity of its own that holds only that token: it connects with its root
    /// agent's certificate. Every node must report the `delegation` feature.
    #[command(after_help = delegate::AFTER_HELP)]
    Delegate(delegate::AgentDelegateArgs),
    /// Lists the context's agents, with their scopes and expiries
    ///
    /// Their principals, parents, scopes, and when their credentials expire, each delegated
    /// agent after its parent. With `--fleet`, the fleet's live delegation records instead.
    List {
        /// Lists the fleet's live delegation records instead of this machine's identities
        ///
        /// Records only, never tokens. Needs the fleet scope. The control node that recorded each is stamped by the leader,
        /// so after a compromise, list with `--since` over the whole time the node may have
        /// led, not only `--recorded-by`.
        #[arg(long)]
        fleet: bool,
        /// Only the records this control node recorded
        #[arg(long, value_name = "NODE_ID", requires = "fleet")]
        recorded_by: Option<String>,
        /// Only the records recorded at or after this time, RFC 3339
        #[arg(long, value_name = "TIME", value_parser = crate::parse_time, requires = "fleet")]
        since: Option<u64>,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Shows one agent: its principal, scopes, expiries, files, and MCP command
    ///
    /// The MCP command is the one that serves as the agent. It also shows when its token must be
    /// renewed by, and for a delegated agent its lineage: its parent, its root, its owner, its
    /// delegation ID, the issuer key that signed its token, and its operation ID.
    Show {
        /// The agent's ID, such as `42`, or a delegated agent's, such as `42.c7`
        #[arg(add = ArgValueCandidates::new(completions::agent_ids))]
        id: AgentRef,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Reissues an agent's certificate and token now, and revokes the old token
    ///
    /// An identity that holds exactly the scope 0.7 gave by default, `admin:agent-<id>-*` (or
    /// `read:agent-<id>-*` for a read-only one), also gets `stream:agent-<id>-*`, today's
    /// default. Once every node reports the `delegation` feature, an identity that holds exactly
    /// 0.7's or those defaults gets the defaults that let it delegate. Scopes chosen with
    /// `--scope` are kept as they are. A delegated agent, such as `42.c7`, is delegated again by
    /// its parent with the scopes it records, and its old delegation ID revoked. Renewing an agent
    /// never renews the delegated agents below it, which the old token's revocation ends.
    Renew {
        /// The agent's ID, such as `42`, or a delegated agent's, such as `42.c7`
        #[arg(add = ArgValueCandidates::new(completions::agent_ids))]
        id: AgentRef,
        /// The directory with the fleet's `ca.crt` and `ca.key`
        ///
        /// Dev mode's own in dev mode, and required elsewhere.
        #[arg(long, value_name = "DIR")]
        authority: Option<PathBuf>,
        #[command(flatten)]
        target: Target,
    },
    /// Revokes an agent and every agent below it, and deletes its directory
    ///
    /// A root agent revokes itself, as a principal and by its own token ID, without the fleet
    /// scope, and the command says when a new token for it may be minted. A delegated agent, such
    /// as `42.c7`, is revoked as a principal by its nearest ancestor with an identity here. Asks
    /// for the ID to be typed on a terminal first. Refuses if the fleet can't be reached to
    /// revoke it.
    Remove {
        /// The agent's ID, such as `42`, or a delegated agent's, such as `42.c7`
        #[arg(add = ArgValueCandidates::new(completions::agent_ids))]
        id: AgentRef,
        /// Removes it without a prompt
        #[arg(long)]
        yes: bool,
        /// Deletes the directory without revoking the token
        ///
        /// The token then stays valid until it expires.
        #[arg(long)]
        no_revoke: bool,
        #[command(flatten)]
        target: Target,
    },
}

/// `fleetring agent add`'s arguments (ADR-0043, point 8.7).
#[derive(Debug, Args)]
pub(crate) struct AgentAddArgs {
    /// The agent's ID: 1 to 32 lowercase letters, digits, or '_'
    ///
    /// Its principal is `agent-<id>`.
    id: AgentId,
    /// A scope the token carries instead of the defaults
    ///
    /// `<action>:<pattern>`, with the action `read`, `write`, `fork`, `admin`, `stream`, or
    /// `grant`. The defaults are `admin:agent-<id>-*` and `stream:agent-<id>-*`, and once every
    /// node reports `delegation`, the same on `agent-<id>.*` and `grant` on both. Repeat it for
    /// more; `agent renew` never widens scopes chosen with it. Nodes before 0.8 refuse a token
    /// that carries `fork` or `grant`.
    #[arg(long = "scope", value_name = "SCOPE")]
    scopes: Vec<Scope>,
    /// Records the identity as read-only, with default scopes that read and stream
    ///
    /// They are `read:agent-<id>-*` and `stream:agent-<id>-*`, and `mcp --agent` offers only the
    /// tools that read.
    #[arg(long)]
    read_only: bool,
    /// The directory with the fleet's `ca.crt` and `ca.key`
    ///
    /// In dev mode, dev mode's own; elsewhere, required.
    #[arg(long, value_name = "DIR")]
    authority: Option<PathBuf>,
    /// How long the certificate lasts: `<n>d`, `<n>h`, `<n>m`, or `<n>s`
    ///
    /// A year in dev mode, and 30 days with `--authority`. The token lasts as long, or 30 days if
    /// that is shorter.
    #[arg(long, value_parser = parse_ttl)]
    ttl: Option<Duration>,
    /// Prints an MCP client's configuration that runs `mcp --agent <id>`
    ///
    /// `claude` prints a `claude mcp add` command; `cursor` and `json` an `mcpServers` object. It
    /// holds no secret.
    #[arg(long, value_enum, value_name = "CLIENT", conflicts_with = "install")]
    print_config: Option<ConfigFormat>,
    /// Registers the server with an MCP client: `claude` runs `claude mcp add`
    #[arg(long, value_enum, value_name = "CLIENT")]
    install: Option<McpClient>,
    #[command(flatten)]
    target: Target,
}

/// A configuration `--print-config` prints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ConfigFormat {
    /// A `claude mcp add` command line.
    Claude,
    /// Cursor's `mcp.json`.
    Cursor,
    /// An `mcpServers` object, as most MCP clients read it.
    Json,
}

/// An MCP client `--install` registers the server with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum McpClient {
    /// Claude Code, through `claude mcp add`.
    Claude,
}

/// Where an agent command finds identities, and whether it is in dev mode.
#[derive(Debug)]
struct Situation {
    /// The selected context.
    context: Option<(ContextName, Context)>,
    /// The dev directory, in dev mode: the context's `dev_dir`, or `~/.fleetring` when no
    /// context is selected and nothing names a connection. It says where to look, and
    /// authorizes nothing (ADR-0043, point 8.1).
    dev_dir: Option<PathBuf>,
    /// Where the identities live.
    agents_dir: PathBuf,
}

impl Situation {
    /// Selects the context as every command does, and decides whether this is dev mode.
    fn of(target: &Target, env: &EnvSnapshot) -> Result<Self, Box<dyn Error>> {
        let contexts = target.contexts(env)?;
        let selected = match select(&contexts, target.context.as_ref(), env.context.as_deref()) {
            Ok(selected) => selected,
            // The `dev` context a printed configuration pins, before the first start writes it:
            // dev mode in the default directory.
            Err(ContextError::Unknown { name, .. }) if name == DEV_NAME => {
                return Ok(Self {
                    context: None,
                    dev_dir: Some(dev::default_dir()?),
                    agents_dir: agent::agents_dir(None, &|name| std::env::var_os(name))?,
                });
            }
            Err(error) => return Err(error.into()),
        };
        let dev_dir = match selected {
            Some((_, context, _)) => context.dev_dir.clone(),
            None if !target.names_a_connection(env) => Some(dev::default_dir()?),
            None => None,
        };
        let agents_dir = agent::agents_dir(
            selected.map(|(name, context, _)| (name, context)),
            &|name| std::env::var_os(name),
        )?;
        Ok(Self {
            context: selected.map(|(name, context, _)| (name.clone(), context.clone())),
            dev_dir,
            agents_dir,
        })
    }

    /// In dev mode, the dev node's control address, for when no context names one: read from
    /// the dev directory's configuration, which must pass [`dev::check_dev_dir`].
    fn dev_control(&self) -> Option<String> {
        let checked = dev::check_dev_dir(self.dev_dir.as_deref()?).ok()?;
        checked
            .config()
            .control_target()
            .map(|address| address.to_string())
    }

    fn selected(&self) -> Option<(&ContextName, &Context)> {
        self.context.as_ref().map(|(name, context)| (name, context))
    }

    /// The context a printed MCP command pins: the selected one, or `dev`, which a dev node
    /// writes.
    fn pinned(&self) -> String {
        self.context
            .as_ref()
            .map_or_else(|| DEV_NAME.to_owned(), |(name, _)| name.to_string())
    }

    /// Where the command acts, for a prompt: the fleet and the context, or the agents
    /// directory when no context is in use.
    fn place(&self, record: &AgentRecord) -> Place {
        Place {
            fleet: Some(record.fleet.clone()),
            via: match &self.context {
                Some((name, _)) => format!("context '{name}'"),
                None => format!("agents directory {}", self.agents_dir.display()),
            },
        }
    }
}

/// Whose identity `mcp --agent` serves with.
#[derive(Debug)]
enum Ready {
    /// A root agent's, which a start in dev mode mints or renews.
    Root(AgentDir),
    /// A delegated agent's, which a start never mints: its parent delegates it.
    Delegated(DelegatedDir),
}

/// What `mcp --agent` serves with.
#[derive(Debug)]
pub(crate) struct AgentReady {
    identity: Ready,
    record: AgentRecord,
    /// In dev mode, the dev node's control address, for when no context names one.
    dev_control: Option<String>,
    /// How `agent renew` would mint the identity, as far as the start knows: the dev fleet's
    /// answer in dev mode, and FR-081's defaults elsewhere, where a start asks no fleet.
    renewal: Minting,
}

impl AgentReady {
    /// The identity's record.
    pub(crate) fn record(&self) -> &AgentRecord {
        &self.record
    }

    /// The dev node's address in dev mode, for when no context names a control address.
    pub(crate) fn fallback_control(&self) -> Option<String> {
        self.dev_control.clone()
    }

    /// What the server says on stderr before it serves: who it acts as, where, when the
    /// credentials expire, and that this is trusted local development (ADR-0043, point 8.1).
    pub(crate) fn start_text(&self, context: Option<&ContextName>) -> String {
        let context = context.map_or_else(
            || "no context".to_owned(),
            |name| format!("context '{name}'"),
        );
        let record = &self.record;
        let dir = match &self.identity {
            Ready::Root(dir) => dir,
            Ready::Delegated(dir) => return delegated_start_text(dir, record, &context),
        };
        let mut text = String::new();
        // A start never widens an identity; it says how to.
        let id = dir.id();
        if let (Some(held), Some(widened)) =
            (id.held_defaults(record), id.widened(record, self.renewal))
        {
            let scopes: Vec<String> = widened.iter().map(ToString::to_string).collect();
            let lacks = match held {
                HeldDefaults::V07 => "the stream scope, so it can't follow its databases' changes",
                HeldDefaults::Fr081 | HeldDefaults::Delegating => {
                    "delegation, so it can't delegate"
                }
            };
            let _ = writeln!(
                text,
                "note: agent {id}'s identity predates {lacks}; `fleetring agent renew {id}` gives \
                 it {}",
                and_list(&scopes),
            );
        }
        let renewal = if self.dev_control.is_some() {
            "renewed at start when under 7 days".to_owned()
        } else {
            format!(
                "renew it with `fleetring agent renew {} --authority <dir>` where the fleet's \
                 authority is",
                dir.id()
            )
        };
        let _ = writeln!(
            text,
            "serving MCP as {} on {context}; certificate expires {}, token expires {} \
             ({renewal})",
            record.principal,
            record.cert_expires_at.as_deref().unwrap_or("-"),
            record.token_expires_at
        );
        if record
            .token_expiry()
            .is_some_and(|expiry| expiry <= SystemClock.now())
        {
            let _ = writeln!(
                text,
                "warning: the token expired at {}, so the fleet refuses every call",
                record.token_expires_at
            );
        }
        let _ = writeln!(text, "{TRUSTED_LOCAL}");
        text
    }
}

/// What the server says on stderr before it serves as a delegated agent (ADR-0047, point 10):
/// who it acts as, its parent and owner, where, when its token expires, and that this is trusted
/// local development.
fn delegated_start_text(dir: &DelegatedDir, record: &AgentRecord, context: &str) -> String {
    let id = dir.id();
    let mut text = format!(
        "serving MCP as {} on {context}, delegated by agent {} for {}, with agent {}'s \
         certificate; token expires {} (ask the parent for a new one: `fleetring agent renew \
         {id}`)\n",
        record.principal,
        id.parent(),
        record.owner,
        id.root(),
        record.token_expires_at
    );
    if record
        .token_expiry()
        .is_some_and(|expiry| expiry <= SystemClock.now())
    {
        let _ = writeln!(
            text,
            "warning: the token expired at {}, so the fleet refuses every call; ask the parent: \
             fleetring agent renew {id}",
            record.token_expires_at
        );
    }
    let _ = writeln!(text, "{TRUSTED_LOCAL}");
    text
}

/// `items` in a sentence: `a and b`, or `a, b and c`.
fn and_list(items: &[String]) -> String {
    match items.split_last() {
        None => String::new(),
        Some((last, [])) => last.clone(),
        Some((last, rest)) => format!("{} and {last}", rest.join(", ")),
    }
}

/// Whether the identity `record` describes could delegate if its token said it was an agent's:
/// its defaults would become the ones that delegate, or its scopes hold `grant`.
fn could_delegate(id: &AgentId, record: &AgentRecord) -> bool {
    id.widened(record, Minting::Delegating).is_some()
        || record.scopes.iter().any(|scope| {
            scope
                .parse::<Scope>()
                .is_ok_and(|scope| scope.action() == Some(Action::Grant))
        })
}

/// Notes on stderr why the identity `record` describes can't delegate yet, if `support` says
/// the fleet can't mint one that does and the identity could (ADR-0047, point 12.2).
fn note_delegation(support: &Support, id: &AgentId, record: &AgentRecord) {
    if could_delegate(id, record)
        && let Some(why) = support.note(id)
    {
        note(why);
    }
}

/// The error for an agent with no identity where the command looked, with the commands that
/// create one (ADR-0043, point 8.6).
pub(crate) fn no_identity(dir: &AgentDir, context: Option<(&ContextName, &Context)>) -> String {
    let id = dir.id();
    let path = dir.tls_dir().display();
    let hint = match context {
        Some((_, context)) if context.dev_dir.is_some() => format!(
            "run `fleetring agent add {id}`, or `fleetring mcp --agent {id}`, which mints one in \
             dev mode"
        ),
        Some((name, _)) => format!(
            "run `fleetring agent add {id} --context {name} --authority <dir with ca.key>` where \
             the fleet's authority is, and copy the directory it writes to {path} on this machine"
        ),
        None => format!(
            "in dev mode, run `fleetring agent add {id}`; for another fleet, run `fleetring agent \
             add {id} --context <name> --authority <dir with ca.key>` where its authority is, and \
             copy the directory to {path}"
        ),
    };
    format!("agent '{id}' has no identity in {path}\n  hint: {hint}")
}

/// `mcp --agent`, before it serves (ADR-0043, point 8.1). With `--agent` in dev mode, it starts
/// the dev node if none runs, checks the dev directory, and mints the identity or renews what is
/// due; elsewhere, or with only `FLEETRING_AGENT`, it uses the identity that exists. `None`
/// without an agent.
///
/// # Errors
///
/// If the identity doesn't exist outside dev mode, the dev directory fails its check, or the
/// node can't be started or the identity minted. Nothing is minted on any error before the mint.
///
/// A first mint gets `access`'s default scopes and records them, so `--read-only` on a first
/// start makes a reading identity. An identity that exists keeps its own scopes and access:
/// `access` neither widens nor narrows it, and only `agent renew` adds `stream` to an identity
/// minted before it was a default.
pub(crate) async fn prepare_agent(
    target: &Target,
    access: Access,
) -> Result<Option<AgentReady>, Box<dyn Error>> {
    let env = EnvSnapshot::capture();
    let Some((id, from)) = target.agent(&env)? else {
        return Ok(None);
    };
    let situation = Situation::of(target, &env)?;
    let id = match id {
        AgentRef::Root(id) => id,
        AgentRef::Delegated(id) => {
            // A delegated agent's token comes from its parent; a start never mints one.
            let dir = DelegatedDir::new(&situation.agents_dir, id);
            let record = dir.usable()?;
            return Ok(Some(AgentReady {
                identity: Ready::Delegated(dir),
                record,
                dev_control: situation.dev_control(),
                renewal: Minting::Fr081,
            }));
        }
    };
    let dir = AgentDir::new(&situation.agents_dir, id);
    let dev_dir = match (&situation.dev_dir, from) {
        (Some(dev_dir), crate::AgentFrom::Flag) => dev_dir,
        // Only `--agent` mints; a variable left in a shell never does.
        _ => {
            let record = dir
                .record()?
                .ok_or_else(|| no_identity(&dir, situation.selected()))?;
            return Ok(Some(AgentReady {
                identity: Ready::Root(dir),
                record,
                dev_control: None,
                renewal: Minting::Fr081,
            }));
        }
    };
    acquire_node(dev_dir, MachineOutput::Yes).await?;
    // After the node is found or started, and before the mint with nothing but the question
    // of what the node reports in between.
    let checked = dev::check_dev_dir(dev_dir)?;
    let support = delegation_support(dev_admin(&checked).ok().as_ref()).await;
    let spec = dev_spec(dir.id(), Vec::new(), access, None, support.minting());
    let acquired = dir.acquire(Issuer::Dev(&checked), &spec, SystemClock.now())?;
    match &acquired {
        Acquired::Minted(record) => {
            note(format!(
                "minted agent {}'s identity in {}",
                dir.id(),
                dir.tls_dir().display()
            ));
            note_delegation(&support, dir.id(), record);
        }
        Acquired::Renewed { old, renewal, .. } => {
            note(format!(
                "renewed agent {}'s {}",
                dir.id(),
                renewed(*renewal)
            ));
            if let Some(old) = old {
                match dev_admin(&checked) {
                    Ok(admin) => {
                        let no_other = || Err("no other client to revoke it with".into());
                        revoke_replaced(Some(&admin), no_other, old).await;
                    }
                    Err(error) => note(format!(
                        "warning: couldn't revoke the old token {} ({error}); it stays valid \
                         until {}",
                        old.id,
                        format_time(old.until.as_millis())
                    )),
                }
            }
        }
        Acquired::Reused(_) => {}
    }
    let control = checked
        .config()
        .control_target()
        .map(|address| address.to_string());
    Ok(Some(AgentReady {
        identity: Ready::Root(dir),
        record: acquired.record().clone(),
        dev_control: control,
        renewal: support.minting(),
    }))
}

/// Runs an `agent` command and returns what it prints on stdout.
pub(crate) async fn run(command: AgentCommand) -> Result<String, Box<dyn Error>> {
    match command {
        AgentCommand::Add(args) => add(args).await,
        AgentCommand::Delegate(args) => delegate::run(args).await,
        AgentCommand::List {
            fleet: true,
            recorded_by,
            since,
            output,
            target,
        } => delegate::list_fleet(recorded_by, since, output.format(), &target).await,
        AgentCommand::List {
            fleet: false,
            output,
            target,
            ..
        } => {
            let situation = situation(&target)?;
            list(&situation, output.format())
        }
        AgentCommand::Show { id, output, target } => {
            let situation = situation(&target)?;
            show(&situation, id, output.format())
        }
        AgentCommand::Renew {
            id: AgentRef::Root(id),
            authority,
            target,
        } => renew(id, authority.as_deref(), &target).await,
        AgentCommand::Renew {
            id: AgentRef::Delegated(id),
            authority,
            target,
        } => renew_delegated(id, authority.as_deref(), &target).await,
        AgentCommand::Remove {
            id,
            yes,
            no_revoke,
            target,
        } => {
            let confirm = if yes { Confirm::Given } else { Confirm::Ask };
            let revoke = if no_revoke {
                Revoke::Skip
            } else {
                Revoke::Required
            };
            remove(id, confirm, revoke, &target).await
        }
    }
}

/// The situation of an `agent` command, which names the agent as its argument.
fn situation(target: &Target) -> Result<Situation, Box<dyn Error>> {
    if target.tls.agent.is_some() {
        return Err("`agent` commands name the agent as their argument; leave out --agent".into());
    }
    Situation::of(target, &EnvSnapshot::capture())
}

/// `agent add`.
async fn add(args: AgentAddArgs) -> Result<String, Box<dyn Error>> {
    let situation = situation(&args.target)?;
    for warning in args.scopes.iter().filter_map(Scope::separator_warning) {
        note(format!("warning: {warning}"));
    }
    let pre_0_8 = agent::pre_0_8_note(&args.scopes);
    let access = if args.read_only {
        Access::ReadOnly
    } else {
        Access::ReadWrite
    };
    let dir = AgentDir::new(&situation.agents_dir, args.id.clone());
    // Before a node is started for it.
    if dir.record()?.is_some() {
        return Err(AgentError::Exists {
            id: args.id.to_string(),
            dir: dir.tls_dir().to_owned(),
        }
        .into());
    }
    let now = SystemClock.now();
    let (record, support) = match (&args.authority, &situation.dev_dir) {
        (Some(authority_dir), _) => {
            if situation.context.is_none() && situation.dev_dir.is_none() {
                return Err(
                    "an agent's identity is kept per context: pass --context <name>, \
                            the context of the fleet the authority is for"
                        .into(),
                );
            }
            let authority = agent::load_authority(authority_dir)?;
            if let Some((name, context)) = situation.selected()
                && let Some(fleet) = &context.fleet
                && fleet != authority.fleet().as_str()
            {
                return Err(format!(
                    "context '{name}' is for fleet '{fleet}', but the authority in {} is for \
                     fleet '{}'",
                    authority_dir.display(),
                    authority.fleet()
                )
                .into());
            }
            let owner = args
                .target
                .principal()
                .unwrap_or_else(|| "authority".to_owned());
            let support = delegation_support(args.target.client().ok().as_ref()).await;
            let minting = support.minting();
            let cert_ttl = args.ttl.unwrap_or(AUTHORITY_CERT_LIFETIME);
            let (scopes, scopes_from) = scopes_or_default(&args.id, args.scopes, access, minting);
            let spec = MintSpec {
                scopes,
                scopes_from,
                access,
                cert_ttl,
                token_ttl: cert_ttl.min(MAX_TOKEN_LIFETIME),
                owner,
                minting,
            };
            (
                dir.mint(Issuer::Authority(&authority), &spec, now)?,
                support,
            )
        }
        (None, Some(dev_dir)) => {
            acquire_node(dev_dir, MachineOutput::No).await?;
            let checked = dev::check_dev_dir(dev_dir)?;
            let support = delegation_support(dev_admin(&checked).ok().as_ref()).await;
            let spec = dev_spec(&args.id, args.scopes, access, args.ttl, support.minting());
            (dir.mint(Issuer::Dev(&checked), &spec, now)?, support)
        }
        (None, None) => {
            return Err(format!(
                "outside dev mode, `agent add` signs with the fleet's authority\n  hint: run \
                 `fleetring agent add {} --context <name> --authority <dir with ca.key>` where \
                 the authority is",
                args.id
            )
            .into());
        }
    };
    if let Some(pre_0_8) = pre_0_8 {
        note(pre_0_8);
    }
    note_delegation(&support, &args.id, &record);
    let context = situation.pinned();
    let invocation = Invocation::of(args.id.as_str(), &context)?;
    let summary = describe(
        &Listed::Root(dir),
        &record,
        &context,
        &invocation,
        Detail::Summary,
    );
    let mut stdout = String::new();
    match args.print_config {
        Some(format) => {
            // Stdout is the configuration alone, for a pipe.
            eprint!("{summary}");
            stdout = invocation.config(format)?;
        }
        None => stdout.push_str(&summary),
    }
    if let Some(McpClient::Claude) = args.install {
        install_claude(&invocation)?;
        stdout.push_str("registered with Claude Code as the MCP server `fleetring`\n");
    }
    Ok(stdout)
}

/// The scopes `--scope` gave, or the agent's defaults for `access` as `minting` says, and which
/// they are.
fn scopes_or_default(
    id: &AgentId,
    scopes: Vec<Scope>,
    access: Access,
    minting: Minting,
) -> (Vec<Scope>, ScopesFrom) {
    if scopes.is_empty() {
        (id.default_scopes(access, minting), ScopesFrom::Defaults)
    } else {
        (scopes, ScopesFrom::Flags)
    }
}

/// What a dev mode identity gets: a certificate for a year, or `ttl`, and a token for 30 days,
/// or `ttl` if that is shorter, signed by dev mode's admin and minted as `minting` says.
fn dev_spec(
    id: &AgentId,
    scopes: Vec<Scope>,
    access: Access,
    ttl: Option<Duration>,
    minting: Minting,
) -> MintSpec {
    let cert_ttl = ttl.unwrap_or(DEV_CERT_LIFETIME);
    let (scopes, scopes_from) = scopes_or_default(id, scopes, access, minting);
    MintSpec {
        scopes,
        scopes_from,
        access,
        cert_ttl,
        token_ttl: cert_ttl.min(MAX_TOKEN_LIFETIME),
        owner: dev::PRINCIPAL.to_owned(),
        minting,
    }
}

/// Starts the dev node of `dev_dir` in the background unless one runs from it, and writes the
/// `dev` context, saying so on stderr.
async fn acquire_node(dev_dir: &Path, machine: MachineOutput) -> Result<(), Box<dyn Error>> {
    if matches!(running(&dev_dir.join(dev::DATA_DIR)), Ok(Running::Yes(_))) {
        return Ok(());
    }
    let exe = std::env::current_exe()?;
    let arg0 = std::env::args_os()
        .next()
        .unwrap_or_else(|| exe.clone().into_os_string());
    let progress = Progress::start(
        "starting the dev node\u{2026}",
        &spinner_surroundings(machine),
    );
    let (prepared, detached) = dev::start_detached(
        dev_dir,
        &exe,
        &arg0,
        dev_cmd::DETACH_TIMEOUT,
        SystemClock.now(),
    )
    .await?;
    progress.finish_err();
    let context = context_cmd::write_dev_context(&prepared);
    if let Detached::Started { pid } = detached {
        note(format!(
            "started the dev node (pid {pid}) in {}; context {}; stop it with `fleetring dev \
             stop`",
            prepared.dir().display(),
            context.describe()
        ));
    }
    Ok(())
}

/// A client acting as dev mode's admin, the principal with the fleet scope a revocation needs.
fn dev_admin(checked: &DevChecked) -> Result<Client, Box<dyn Error>> {
    let tls = checked.tls_dir();
    let stem = format!("client.{}", dev::PRINCIPAL);
    let identity = Identity::load(
        &tls.join("ca.crt"),
        &tls.join(format!("{stem}.crt")),
        &tls.join(format!("{stem}.key")),
    )?;
    let control = checked
        .config()
        .control_target()
        .ok_or("the dev configuration names no control address")?;
    Ok(Client::new(
        control.to_string(),
        Network::default().with_security(Security::Tls(identity)),
    ))
}

/// What [`revoke()`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Revocation {
    /// The fleet refuses the token until this time, as the control node answered.
    Until(Timestamp),
    /// The token had expired already, so there was nothing to revoke.
    Expired,
}

impl Revocation {
    /// The words for what happened to `token`.
    fn describe(self, token: &OldToken) -> String {
        match self {
            Self::Until(until) => format!(
                "revoked token {} until {}",
                token.id,
                format_time(until.as_millis())
            ),
            Self::Expired => format!("token {} had expired already", token.id),
        }
    }
}

/// Revokes `token` through `client` by its ID until the token itself expires, which
/// [`OldToken`] reads from the token, never from `identity.toml`. With `fleet` the entry lasts
/// as asked; under the ancestor rule the control plane chooses.
async fn revoke(client: &Client, token: &OldToken) -> Result<Revocation, Unanswered> {
    if token.until <= SystemClock.now() {
        return Ok(Revocation::Expired);
    }
    let answer = revoke::send_revocation(client, &revoke::token_request(token)).await?;
    Ok(Revocation::Until(Timestamp::from_millis(answer.until_ms)))
}

/// Revokes a root agent's replaced token, and warns rather than fails if it can't (ADR-0043,
/// point 8.2): with `fleet` through `fleet_client` when there is one, and otherwise, or if that
/// lacks `fleet`, as the agent itself with the old token, `old_token`, under the ancestor rule,
/// which a relay may carry since it names the token's own ID (ADR-0047, points 4.5 and 11.3).
/// Returns whether it is revoked.
async fn revoke_replaced(
    fleet_client: Option<&Client>,
    as_itself: impl FnOnce() -> Result<Client, Box<dyn Error>>,
    old: &OldToken,
) -> bool {
    let by_fleet = match fleet_client {
        Some(client) => Some(revoke(client, old).await),
        None => None,
    };
    let outcome = match by_fleet {
        Some(Err(Unanswered::Refused(status))) if status.code() == Code::PermissionDenied => None,
        Some(outcome) => Some(outcome.map_err(|unanswered| revoke::describe(&unanswered))),
        None => None,
    };
    let outcome = match outcome {
        Some(outcome) => outcome,
        None => match as_itself() {
            Ok(client) => revoke(&client, old)
                .await
                .map_err(|unanswered| revoke::describe(&unanswered)),
            Err(error) => Err(error.to_string()),
        },
    };
    match outcome {
        Ok(revocation) => {
            note(revocation.describe(old));
            revocation != Revocation::Expired
        }
        Err(error) => {
            note(format!(
                "warning: couldn't revoke the old token {} ({error}); it stays valid until {}",
                old.id,
                format_time(old.until.as_millis())
            ));
            false
        }
    }
}

/// What a renewal reissued, in words.
fn renewed(renewal: Renewal) -> &'static str {
    match renewal {
        Renewal::Certificate => "certificate",
        Renewal::Token => "token",
        Renewal::Both | Renewal::None => "certificate and token",
    }
}

/// `agent renew`: reissues both credentials, revokes the old token, and gives an identity that
/// holds 0.7's or FR-081's defaults the defaults the fleet allows: the ones that delegate once
/// every member reports `delegation` (ADR-0047, point 12.4).
async fn renew(
    id: AgentId,
    authority: Option<&Path>,
    target: &Target,
) -> Result<String, Box<dyn Error>> {
    let situation = situation(target)?;
    let dir = AgentDir::new(&situation.agents_dir, id);
    let now = SystemClock.now();
    // Read before the renewal replaces it: the agent presents it to revoke it without `fleet`.
    let old_token = dir.token().ok();
    let (renewed, client, support) = match (authority, &situation.dev_dir) {
        (Some(authority_dir), _) => {
            let authority = agent::load_authority(authority_dir)?;
            let client = target.client().ok();
            let support = delegation_support(client.as_ref()).await;
            // A renewal takes the scopes, the access, and the owner from the record.
            let spec = MintSpec {
                scopes: Vec::new(),
                scopes_from: ScopesFrom::Defaults,
                access: Access::ReadWrite,
                cert_ttl: AUTHORITY_CERT_LIFETIME,
                token_ttl: MAX_TOKEN_LIFETIME,
                owner: String::new(),
                minting: support.minting(),
            };
            let renewed = dir.renew(Issuer::Authority(&authority), &spec, now)?;
            (renewed, client, support)
        }
        (None, Some(dev_dir)) => {
            let checked = dev::check_dev_dir(dev_dir)?;
            let client = dev_admin(&checked).ok();
            let support = delegation_support(client.as_ref()).await;
            let spec = dev_spec(
                dir.id(),
                Vec::new(),
                Access::ReadWrite,
                None,
                support.minting(),
            );
            let renewed = dir.renew(Issuer::Dev(&checked), &spec, now)?;
            (renewed, client, support)
        }
        (None, None) => {
            return Err(format!(
                "outside dev mode, `agent renew` signs with the fleet's authority\n  hint: run \
                 `fleetring agent renew {} --authority <dir with ca.key>` where the authority is",
                dir.id()
            )
            .into());
        }
    };
    let Renewed {
        record,
        old,
        scopes,
    } = renewed;
    // The renewed token carries the record's scopes, which the renewal has just parsed. The
    // defaults that delegate, renewed while every member reports `delegation`, leave no node
    // before 0.8 to refuse them.
    let reminted: Vec<Scope> = record
        .scopes
        .iter()
        .filter_map(|scope| scope.parse().ok())
        .collect();
    let delegating = support == Support::Every
        && dir.id().held_defaults(&record) == Some(HeldDefaults::Delegating);
    if !delegating && let Some(pre_0_8) = agent::pre_0_8_note(&reminted) {
        note(pre_0_8);
    }
    note_delegation(&support, dir.id(), &record);
    let as_itself = || -> Result<Client, Box<dyn Error>> {
        let token = old_token.as_ref().ok_or("the old token can't be read")?;
        target.client_as(dir.identity()?, token, situation.dev_control())
    };
    if revoke_replaced(client.as_ref(), as_itself, &old).await {
        let below = revoke::local_descendants(&situation.agents_dir, dir.id().as_str());
        if let Some(ended) = revoke::ended_below(
            &below,
            "renewing an agent never renews the agents below it: renew each with `fleetring \
             agent renew <id>`",
        ) {
            note(ended);
        }
    }
    Ok(format!(
        "renewed agent {}: certificate expires {}, token expires {}\n  scopes: {}\n",
        dir.id(),
        record.cert_expires_at.as_deref().unwrap_or("-"),
        record.token_expires_at,
        renewed_scopes(&record, &scopes)
    ))
}

/// `agent renew <child>`: delegates the child again as its parent, with the scopes it records,
/// under a new operation ID, writes the new token over the old, then revokes the old delegation
/// ID as the parent (ADR-0047, points 10 and 11). The delegated agents below the child, minted
/// from the old token, end with it and aren't renewed.
async fn renew_delegated(
    id: DelegatedId,
    authority: Option<&Path>,
    target: &Target,
) -> Result<String, Box<dyn Error>> {
    if authority.is_some() {
        return Err(exit::Usage(format!(
            "agent {id} is delegated: its parent asks the control plane for its new token, and \
             the authority signs nothing; leave out --authority"
        ))
        .into());
    }
    let situation = situation(target)?;
    let dir = DelegatedDir::new(&situation.agents_dir, id.clone());
    let record = dir.usable()?;
    let parent = id.parent();
    let parent_record = delegate::parent_record(&situation.agents_dir, &parent)?;
    let parent_token = revoke::token_of(&situation.agents_dir, &parent)?;
    // As the parent: the root's certificate and the parent's token.
    let client = target.client_as(
        dir.root().identity()?,
        &parent_token,
        situation.dev_control(),
    )?;
    let old = dir.old_token(&record, SystemClock.now());

    let pending = dir.begin_renewal()?;
    let operation = pending.operation();
    match pending.from() {
        OperationFrom::Earlier => note(format!(
            "resending operation {operation}, which an earlier renewal left in {}",
            dir.dir().display()
        )),
        OperationFrom::New => note(format!(
            "operation {operation}: if this command is interrupted, run it again to resend it"
        )),
    }
    let request = DelegateTokenRequest {
        operation_id: operation.to_bytes().to_vec(),
        segment: id.segment().to_owned(),
        scopes: record.scopes.clone(),
        lifetime_ms: None,
        not_before_ms: None,
    };
    // This command resends; the client mustn't as well.
    let control = client.control().clone().with_backoff(Backoff {
        attempts: 1,
        ..Backoff::default()
    });
    let answer = delegate::send(ResendPolicy::default(), || {
        control.delegate_token(request.clone())
    })
    .await;
    let response = match answer {
        Ok(response) => response,
        Err(unanswered) => {
            if unanswered.gives_up() {
                dir.abandon(pending)?;
            }
            let refusal = delegate::Refusal {
                id: &id,
                parent: &parent,
                operation,
                dir: &dir,
                rerun: format!("`fleetring agent renew {id}`"),
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
    let read_only = parent_record.read_only || !delegate::writes(&granted);
    let delegation = Delegation {
        token: BearerToken::new(token),
        principal,
        delegation_id,
        scopes: granted,
        scopes_from: record.scopes_from,
        read_only,
        expires_at: Timestamp::from_millis(expires_at_ms),
        issuer_key_id,
        parent: parent_record,
    };
    let renewed = dir.finish(pending, delegation)?;

    match revoke(&client, &old).await {
        Ok(revocation) => {
            note(revocation.describe(&old));
            if revocation != Revocation::Expired
                && let Some(ended) = revoke::ended_below(
                    &revoke::local_descendants(&situation.agents_dir, id.as_str()),
                    "renewing an agent never renews the agents below it: renew each with \
                     `fleetring agent renew <id>`",
                )
            {
                note(ended);
            }
        }
        Err(unanswered) => note(format!(
            "warning: couldn't revoke the old token {} ({}); it stays valid until {}",
            old.id,
            revoke::describe(&unanswered),
            format_time(old.until.as_millis())
        )),
    }
    let capped = if capped {
        " (capped at the parent's expiry)"
    } else {
        ""
    };
    Ok(format!(
        "renewed agent {id}: token expires {}{capped}\n  scopes: {}\n  delegation: {}\n",
        renewed.token_expires_at,
        renewed.scopes.join(", "),
        renewed.token_id
    ))
}

/// What `agent renew` says about the scopes the new token carries.
fn renewed_scopes(record: &AgentRecord, scopes: &ScopeRenewal) -> String {
    let held = record.scopes.join(", ");
    match scopes {
        ScopeRenewal::Widened { from, added } => {
            let what = if added.iter().any(|scope| scope.starts_with("grant:")) {
                "defaults since 0.8 that let it delegate"
            } else {
                "a default since 0.8"
            };
            let before = match from {
                HeldDefaults::V07 => "only 0.7's default",
                HeldDefaults::Fr081 | HeldDefaults::Delegating => {
                    "the defaults minted before every node could delegate"
                }
            };
            format!(
                "{held} (added {}, {what}; the identity held {before})",
                added.join(", ")
            )
        }
        ScopeRenewal::Kept if record.scopes_from == ScopesFrom::Flags => format!(
            "{held} (unchanged: chosen with --scope, which renew never widens; to change them, \
             remove the agent and add it again)"
        ),
        ScopeRenewal::Kept => format!("{held} (unchanged)"),
    }
}

/// Whether `agent remove` asks first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Confirm {
    Ask,
    Given,
}

/// Whether `agent remove` revokes the token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Revoke {
    Required,
    Skip,
}

/// `agent remove`: revokes the agent and every agent below it, then deletes the directory
/// (ADR-0043, point 8.7; ADR-0047, points 10 and 11.7).
async fn remove(
    id: AgentRef,
    confirm: Confirm,
    revoke_token: Revoke,
    target: &Target,
) -> Result<String, Box<dyn Error>> {
    match id {
        AgentRef::Root(id) => remove_root(id, confirm, revoke_token, target).await,
        AgentRef::Delegated(id) => remove_delegated(id, confirm, revoke_token, target).await,
    }
}

/// `agent remove <root>`: the agent revokes itself, its principal and then its own token ID in
/// one request, without `fleet`, and the command says when a new token may be minted. On a
/// fleet that can't use principals yet, the token alone, with `fleet`, as before 0.8.
async fn remove_root(
    id: AgentId,
    confirm: Confirm,
    revoke_token: Revoke,
    target: &Target,
) -> Result<String, Box<dyn Error>> {
    let situation = situation(target)?;
    let dir = AgentDir::new(&situation.agents_dir, id);
    let Some(record) = dir.record()? else {
        return Err(AgentError::NoIdentity {
            id: dir.id().to_string(),
            dir: dir.tls_dir().to_owned(),
        }
        .into());
    };
    if confirm == Confirm::Ask {
        confirm_removal(dir.id().as_str(), &record, dir.tls_dir(), &situation)?;
    }
    // Read from the token itself before the directory goes.
    let now = SystemClock.now();
    let token = dir.old_token(&record, now);
    let until = format_time(token.until.as_millis());
    let (revoked, replacement) = match revoke_token {
        Revoke::Skip => {
            note(format!(
                "warning: not revoking token {}: it stays valid until {until}",
                token.id
            ));
            (String::new(), String::new())
        }
        Revoke::Required if token.until <= now => (
            format!("{} and ", Revocation::Expired.describe(&token)),
            String::new(),
        ),
        Revoke::Required => {
            let principal = dir.id().principal().to_string();
            let refused = |error: String| {
                format!(
                    "can't revoke agent {} ({error}), so nothing was removed\n  hint: pass \
                     --no-revoke to delete the directory anyway; its token then stays valid \
                     until {until}",
                    dir.id()
                )
            };
            let as_itself =
                target.client_as(dir.identity()?, &dir.token()?, situation.dev_control())?;
            let request = revoke::principal_request(&principal, Some(&token));
            match revoke::send_revocation(&as_itself, &request).await {
                // A control node before 0.8 ignores the principal and answers for the token
                // alone, without `before`.
                Ok(answer) if answer.before_ms.is_none() => {
                    note(format!(
                        "warning: the control node revoked token {} but not {principal} as a \
                         principal, as a node before 0.8 does; the agents below it end by the \
                         token's ID",
                        token.id
                    ));
                    (
                        format!(
                            "revoked token {} until {}, but not {principal}, and ",
                            token.id,
                            format_time(answer.until_ms)
                        ),
                        String::new(),
                    )
                }
                Ok(answer) => {
                    let replacement = answer.before_ms.map_or_else(String::new, |before| {
                        format!(
                            "  a new token for {principal} may be minted from {}; one signed \
                             sooner is refused too\n",
                            format_time(
                                revoke::replacement_from(Timestamp::from_millis(before))
                                    .as_millis()
                            )
                        )
                    });
                    (
                        format!(
                            "revoked {principal} and every agent below it, and token {} until \
                             {}, and ",
                            token.id,
                            format_time(answer.until_ms)
                        ),
                        replacement,
                    )
                }
                Err(unanswered) if revoke::falls_back(&unanswered) => {
                    note(format!(
                        "warning: agent {} couldn't revoke itself as a principal ({}); revoking \
                         its token with the fleet scope instead",
                        dir.id(),
                        revoke::describe(&unanswered)
                    ));
                    let client = fleet_client(&situation, target)?;
                    let revocation = revoke(&client, &token)
                        .await
                        .map_err(|unanswered| refused(revoke::describe(&unanswered)))?;
                    (
                        format!("{} and ", revocation.describe(&token)),
                        String::new(),
                    )
                }
                Err(unanswered) => return Err(refused(revoke::describe(&unanswered)).into()),
            }
        }
    };
    dir.remove()?;
    Ok(format!(
        "removed agent {}: {revoked}deleted {}\n{replacement}",
        dir.id(),
        dir.tls_dir().display()
    ))
}

/// The client that revokes with `fleet`: dev mode's administrator, or the target's own
/// identity.
fn fleet_client(situation: &Situation, target: &Target) -> Result<Client, Box<dyn Error>> {
    match &situation.dev_dir {
        Some(dev_dir) => dev_admin(&dev::check_dev_dir(dev_dir)?),
        None => target.client(),
    }
}

/// `agent remove <child>`: revokes the child as a principal, ending it and every agent below
/// it, as its nearest ancestor with an identity here, then deletes its directory.
async fn remove_delegated(
    id: DelegatedId,
    confirm: Confirm,
    revoke_token: Revoke,
    target: &Target,
) -> Result<String, Box<dyn Error>> {
    let situation = situation(target)?;
    let dir = DelegatedDir::new(&situation.agents_dir, id.clone());
    let Some(record) = dir.record()? else {
        return Err(AgentError::NoDelegated {
            id: id.to_string(),
            dir: dir.dir().to_owned(),
        }
        .into());
    };
    if confirm == Confirm::Ask {
        confirm_removal(id.as_str(), &record, dir.dir(), &situation)?;
    }
    let now = SystemClock.now();
    let token = dir.old_token(&record, now);
    let until = format_time(token.until.as_millis());
    let principal = id.name().to_string();
    let refused = |error: String| {
        format!(
            "can't revoke {principal} ({error}), so nothing was removed\n  hint: pass \
             --no-revoke to delete the directory anyway; its token then stays valid until \
             {until}"
        )
    };
    let revoked = match revoke_token {
        Revoke::Skip => {
            note(format!(
                "warning: not revoking {principal}: its token {} stays valid until {until}",
                token.id
            ));
            String::new()
        }
        // Every agent below it expires no later than it does.
        Revoke::Required if token.until <= now => {
            format!("{} and ", Revocation::Expired.describe(&token))
        }
        Revoke::Required => {
            let Some((ancestor, ancestor_token)) =
                revoke::nearest_ancestor(&situation.agents_dir, &id, now)?
            else {
                return Err(refused(format!(
                    "no ancestor of agent {id} has a usable identity here to revoke it as; an \
                     operator can, with `fleetring token revoke --principal {principal}`"
                ))
                .into());
            };
            let client = target.client_as(
                dir.root().identity()?,
                &ancestor_token,
                situation.dev_control(),
            )?;
            let answer =
                revoke::send_revocation(&client, &revoke::principal_request(&principal, None))
                    .await
                    .map_err(|unanswered| refused(revoke::describe(&unanswered)))?;
            let until = answer.principal_until_ms.unwrap_or(answer.until_ms);
            format!(
                "revoked {principal} and every agent below it until {}, as agent {ancestor}, and ",
                format_time(until)
            )
        }
    };
    dir.remove()?;
    if !revoked.is_empty()
        && let Some(ended) = revoke::ended_below(
            &revoke::local_descendants(&situation.agents_dir, id.as_str()),
            "remove each with `fleetring agent remove <id> --no-revoke`",
        )
    {
        note(ended);
    }
    Ok(format!(
        "removed agent {id}: {revoked}deleted {}\n",
        dir.dir().display()
    ))
}

/// Asks for agent `id`'s ID to be typed on the terminal; refuses without one.
fn confirm_removal(
    id: &str,
    record: &AgentRecord,
    dir: &Path,
    situation: &Situation,
) -> Result<(), Box<dyn Error>> {
    confirm::confirm(&confirm::remove_agent(
        id,
        &record.principal,
        &dir.display().to_string(),
        &situation.place(record),
    ))
}

/// An identity in the agents directory: a root agent's or a delegated agent's.
#[derive(Debug, Clone)]
enum Listed {
    Root(AgentDir),
    Delegated(DelegatedDir),
}

impl Listed {
    /// Agent `id`'s identity under `agents_dir`.
    fn of(agents_dir: &Path, id: AgentRef) -> Self {
        match id {
            AgentRef::Root(id) => Self::Root(AgentDir::new(agents_dir, id)),
            AgentRef::Delegated(id) => Self::Delegated(DelegatedDir::new(agents_dir, id)),
        }
    }

    /// The agent's ID.
    fn id(&self) -> AgentRef {
        match self {
            Self::Root(dir) => AgentRef::Root(dir.id().clone()),
            Self::Delegated(dir) => AgentRef::Delegated(dir.id().clone()),
        }
    }

    /// The identity's own directory.
    fn dir(&self) -> &Path {
        match self {
            Self::Root(dir) => dir.tls_dir(),
            Self::Delegated(dir) => dir.dir(),
        }
    }

    fn token_file(&self) -> PathBuf {
        match self {
            Self::Root(dir) => dir.token_file(),
            Self::Delegated(dir) => dir.token_file(),
        }
    }

    fn record_file(&self) -> PathBuf {
        match self {
            Self::Root(dir) => dir.record_file(),
            Self::Delegated(dir) => dir.record_file(),
        }
    }

    fn record(&self) -> Result<Option<AgentRecord>, AgentError> {
        match self {
            Self::Root(dir) => dir.record(),
            Self::Delegated(dir) => dir.record(),
        }
    }
}

/// `agent list`: every identity in the agents directory, each delegated agent after its parent.
fn list(situation: &Situation, format: OutputFormat) -> Result<String, Box<dyn Error>> {
    let mut agents = Vec::new();
    let entries = match std::fs::read_dir(&situation.agents_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return render_list(&agents, format);
        }
        Err(error) => {
            return Err(format!("can't read {}: {error}", situation.agents_dir.display()).into());
        }
    };
    for entry in entries {
        let entry = entry?;
        let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<AgentRef>().ok())
        else {
            continue;
        };
        let listed = Listed::of(&situation.agents_dir, id);
        match listed.record() {
            Ok(Some(record)) => agents.push((listed, record)),
            Ok(None) => {}
            Err(error) => note(format!(
                "warning: skipping agent '{}': {error}",
                listed.id()
            )),
        }
    }
    agents.sort_by(|(left, _), (right, _)| left.id().segments().cmp(&right.id().segments()));
    render_list(&agents, format)
}

fn render_list(
    agents: &[(Listed, AgentRecord)],
    format: OutputFormat,
) -> Result<String, Box<dyn Error>> {
    let listed: Vec<serde_json::Value> = agents
        .iter()
        .map(|(listed, record)| agent_json(listed, record))
        .collect();
    format::render(format, &serde_json::json!({ "agents": listed }), |_| {
        let rows: Vec<[String; 7]> = agents
            .iter()
            .map(|(listed, record)| {
                [
                    listed.id().to_string(),
                    record.principal.clone(),
                    record
                        .parent
                        .as_deref()
                        .map_or_else(|| "-".to_owned(), |parent| agent::id_of(parent).to_owned()),
                    record.scopes.join(","),
                    read_only_text(record).to_owned(),
                    record
                        .cert_expires_at
                        .clone()
                        .unwrap_or_else(|| "-".to_owned()),
                    record.token_expires_at.clone(),
                ]
            })
            .collect();
        Ok(table(
            [
                "ID",
                "PRINCIPAL",
                "PARENT",
                "SCOPES",
                "READ-ONLY",
                "CERT EXPIRES",
                "TOKEN EXPIRES",
            ],
            &rows,
        ))
    })
}

/// `agent show`.
fn show(
    situation: &Situation,
    id: AgentRef,
    format: OutputFormat,
) -> Result<String, Box<dyn Error>> {
    let listed = Listed::of(&situation.agents_dir, id);
    let record = match &listed {
        Listed::Root(dir) => dir
            .record()?
            .ok_or_else(|| no_identity(dir, situation.selected()))?,
        Listed::Delegated(dir) => dir.record()?.ok_or_else(|| AgentError::NoDelegated {
            id: dir.id().to_string(),
            dir: dir.dir().to_owned(),
        })?,
    };
    let shown = serde_json::json!({ "agents": [agent_json(&listed, &record)] });
    format::render(format, &shown, |_| {
        let context = situation.pinned();
        let invocation = Invocation::of(listed.id().as_str(), &context)?;
        Ok(describe(
            &listed,
            &record,
            &context,
            &invocation,
            Detail::Full,
        ))
    })
}

/// An identity as `agent list` and `agent show` print it in JSON. The lineage fields are `null`
/// for a root agent, and `cert_expires_at` for a delegated agent, which has no certificate.
fn agent_json(listed: &Listed, record: &AgentRecord) -> serde_json::Value {
    serde_json::json!({
        "id": listed.id().as_str(),
        "principal": record.principal,
        "parent": record.parent.as_deref().map(agent::id_of),
        "root": record.root.as_deref().map(agent::id_of),
        "owner": record.owner,
        "scopes": record.scopes,
        "read_only": record.read_only,
        "cert_expires_at": record.cert_expires_at,
        "token_expires_at": record.token_expires_at,
        "issued_by": record.issued_by.to_string(),
        "delegation_id": record.is_delegated().then_some(&record.token_id),
        "issuer_key_id": record.issuer_key_id,
        "operation_id": record.operation,
        "dir": listed.dir(),
    })
}

/// Whether the identity serves only the reading tools, in words.
fn read_only_text(record: &AgentRecord) -> &'static str {
    match record.access() {
        Access::ReadOnly => "yes",
        Access::ReadWrite => "no",
    }
}

/// How much [`describe`] says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Detail {
    /// What `agent add` prints.
    Summary,
    /// What `agent show` prints: the files too.
    Full,
}

/// An identity as key and value lines, ending with the trusted local development line.
fn describe(
    listed: &Listed,
    record: &AgentRecord,
    context: &str,
    invocation: &Invocation,
    detail: Detail,
) -> String {
    let id = listed.id();
    let mut text = match detail {
        Detail::Summary => format!(
            "added agent {id} as {} on context '{context}', issued by {}\n",
            record.principal,
            match record.issued_by {
                IssuedBy::Dev => "dev mode's authority",
                IssuedBy::Authority => "the authority",
                IssuedBy::Delegation => "delegation",
            }
        ),
        Detail::Full => format!("agent {id}\n"),
    };
    let mut line = |key: &str, value: &str| {
        let _ = writeln!(text, "  {key:<13}{value}");
    };
    line("principal", &record.principal);
    line("owner", &record.owner);
    if let Some(parent) = &record.parent {
        line("parent", agent::id_of(parent));
    }
    if let Some(root) = &record.root {
        line("root", agent::id_of(root));
    }
    line("fleet", &record.fleet);
    line("scopes", &record.scopes.join(", "));
    line("read-only", read_only_text(record));
    if let Some(cert_expires_at) = &record.cert_expires_at {
        line("cert expires", cert_expires_at);
    }
    line("token expires", &record.token_expires_at);
    if record.is_delegated() {
        line("delegation", &record.token_id);
    }
    if let Some(issuer_key_id) = &record.issuer_key_id {
        line("issuer key", issuer_key_id);
    }
    if let Some(operation) = &record.operation {
        line("operation", operation);
    }
    let renew_by = match record.issued_by {
        IssuedBy::Dev => format!(
            "{}; the next `fleetring mcp --agent {id}` start renews it once fewer than {} days \
             remain",
            record.token_expires_at,
            RENEW_TOKEN_WITHIN.as_secs() / (24 * 60 * 60)
        ),
        IssuedBy::Authority => format!(
            "{}; run `fleetring agent renew {id} --authority <dir>` before then",
            record.token_expires_at
        ),
        IssuedBy::Delegation => format!(
            "{}; its parent delegates a new token before then",
            record.token_expires_at
        ),
    };
    line("renew by", &renew_by);
    line("directory", &listed.dir().display().to_string());
    if let Listed::Delegated(dir) = listed {
        line(
            "certificate",
            &format!(
                "agent {}'s, in {}",
                dir.id().root(),
                dir.root().tls_dir().display()
            ),
        );
    }
    if detail == Detail::Full {
        line("token file", &listed.token_file().display().to_string());
        line("record", &listed.record_file().display().to_string());
    }
    line("mcp command", &invocation.shell_words());
    text.push_str(TRUSTED_LOCAL);
    text.push('\n');
    text
}

/// The command an MCP client runs to serve as an agent: this binary, absolute, with `mcp
/// --agent <id> --context <context>` (ADR-0043, point 8.8).
#[derive(Debug, Clone)]
struct Invocation {
    exe: String,
    args: Vec<String>,
}

impl Invocation {
    fn of(id: &str, context: &str) -> Result<Self, Box<dyn Error>> {
        let exe = this_binary()?;
        let exe = exe
            .to_str()
            .ok_or_else(|| format!("{} isn't valid UTF-8", exe.display()))?
            .to_owned();
        Ok(Self {
            exe,
            args: ["mcp", "--agent", id, "--context", context]
                .map(str::to_owned)
                .to_vec(),
        })
    }

    /// The command as one line for a POSIX shell.
    fn shell_words(&self) -> String {
        std::iter::once(self.exe.as_str())
            .chain(self.args.iter().map(String::as_str))
            .map(shell_quote)
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// `claude mcp add`'s arguments, before the command.
    fn claude_args() -> [&'static str; 4] {
        ["mcp", "add", "fleetring", "--"]
    }

    /// The configuration `--print-config` prints, which holds no secret.
    fn config(&self, format: ConfigFormat) -> Result<String, Box<dyn Error>> {
        Ok(match format {
            ConfigFormat::Claude => format!(
                "claude {} {}\n",
                Self::claude_args().join(" "),
                self.shell_words()
            ),
            ConfigFormat::Cursor | ConfigFormat::Json => format!(
                "{}\n",
                serde_json::to_string_pretty(&serde_json::json!({
                    "mcpServers": {
                        "fleetring": {"command": self.exe, "args": self.args}
                    }
                }))?
            ),
        })
    }
}

/// This binary's path as it was run, made absolute without resolving links, so that a binary
/// run as `fleetringd` is configured as `fleetringd`; found on `PATH` if it was run by name.
fn this_binary() -> std::io::Result<PathBuf> {
    if let Some(arg0) = std::env::args_os().next() {
        let path = PathBuf::from(&arg0);
        if path.components().count() > 1 {
            return std::path::absolute(path);
        }
        if let Some(found) = on_path(&arg0) {
            return Ok(found);
        }
    }
    std::env::current_exe()
}

/// The first file named `name` in a `PATH` directory, made absolute.
fn on_path(name: &OsString) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
        .and_then(|found| std::path::absolute(found).ok())
}

/// The program `name` on `PATH`, made absolute: on Windows with each of `PATHEXT`'s
/// extensions, `.exe` and `.cmd` among them.
fn program_on_path(name: &str) -> Option<PathBuf> {
    if !cfg!(windows) {
        return on_path(&OsString::from(name));
    }
    let extensions = std::env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_owned());
    extensions
        .split(';')
        .filter(|extension| !extension.is_empty())
        .find_map(|extension| on_path(&OsString::from(format!("{name}{extension}"))))
}

/// Runs `claude mcp add fleetring -- <command>`, as an argument vector, never through a shell.
fn install_claude(invocation: &Invocation) -> Result<(), Box<dyn Error>> {
    // Found on PATH alone and run by its absolute path, never from the working directory, which
    // Windows' own search would try first.
    let Some(claude) = program_on_path("claude") else {
        return Err(
            "can't run `claude`: it isn't on PATH\n  hint: pass `--print-config claude` \
                    and run it yourself"
                .into(),
        );
    };
    let status = std::process::Command::new(claude)
        .args(Invocation::claude_args())
        .arg(&invocation.exe)
        .args(&invocation.args)
        .stdin(std::process::Stdio::null())
        .status();
    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => Err(format!(
            "`claude mcp add` failed ({status})\n  hint: pass `--print-config claude` and run \
             it yourself"
        )
        .into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(
            "can't run `claude`: it isn't on PATH\n  hint: pass `--print-config claude` and run \
             it yourself"
                .into(),
        ),
        Err(error) => Err(format!(
            "can't run `claude`: {error}\n  hint: pass `--print-config claude` and run it \
             yourself"
        )
        .into()),
    }
}

/// `text` as one word for a POSIX shell.
fn shell_quote(text: &str) -> String {
    let plain = !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"/._-:+,@%=".contains(&byte));
    if plain {
        text.to_owned()
    } else {
        format!("'{}'", text.replace('\'', r"'\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invocation() -> Invocation {
        Invocation {
            exe: "/opt/fleet ring/fleetringd".to_owned(),
            args: ["mcp", "--agent", "42", "--context", "dev"]
                .map(str::to_owned)
                .to_vec(),
        }
    }

    #[test]
    fn the_printed_configuration_is_the_command_alone() -> Result<(), Box<dyn Error>> {
        let invocation = invocation();
        assert_eq!(
            invocation.config(ConfigFormat::Claude)?,
            "claude mcp add fleetring -- '/opt/fleet ring/fleetringd' mcp --agent 42 --context \
             dev\n"
        );
        let json: serde_json::Value =
            serde_json::from_str(&invocation.config(ConfigFormat::Json)?)?;
        assert_eq!(
            json,
            serde_json::json!({"mcpServers": {"fleetring": {
                "command": "/opt/fleet ring/fleetringd",
                "args": ["mcp", "--agent", "42", "--context", "dev"],
            }}})
        );
        assert_eq!(
            invocation.config(ConfigFormat::Cursor)?,
            invocation.config(ConfigFormat::Json)?
        );
        Ok(())
    }

    #[test]
    fn agent_list_json_pins_its_fields() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let dir = AgentDir::new(root.path(), "42".parse()?);
        let record = AgentRecord {
            version: 1,
            principal: "agent-42".to_owned(),
            owner: "admin".to_owned(),
            parent: None,
            root: None,
            fleet: "dev".to_owned(),
            scopes: vec!["admin:agent-42-*".to_owned()],
            scopes_from: ScopesFrom::Defaults,
            read_only: false,
            token_id: "t1".to_owned(),
            token_expires_at: "2026-12-01T00:00:00Z".to_owned(),
            cert_expires_at: Some("2027-11-01T00:00:00Z".to_owned()),
            issued_by: IssuedBy::Dev,
            issuer_key_id: None,
            operation: None,
        };
        let child_dir = DelegatedDir::new(root.path(), "42.c7".parse()?);
        let child = AgentRecord {
            version: 2,
            principal: "agent-42.c7".to_owned(),
            parent: Some("agent-42".to_owned()),
            root: Some("agent-42".to_owned()),
            scopes: vec!["admin:agent-42.c7-*".to_owned()],
            token_id: "d1".to_owned(),
            cert_expires_at: None,
            issued_by: IssuedBy::Delegation,
            issuer_key_id: Some("k1".to_owned()),
            operation: Some("op".to_owned()),
            ..record.clone()
        };
        let listed: serde_json::Value = serde_json::from_str(&render_list(
            &[
                (Listed::Root(dir), record),
                (Listed::Delegated(child_dir), child),
            ],
            OutputFormat::Json,
        )?)?;
        let item = listed["agents"][0].as_object().ok_or("an agent")?;
        let keys: Vec<&str> = item.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [
                "cert_expires_at",
                "delegation_id",
                "dir",
                "id",
                "issued_by",
                "issuer_key_id",
                "operation_id",
                "owner",
                "parent",
                "principal",
                "read_only",
                "root",
                "scopes",
                "token_expires_at"
            ]
        );
        assert_eq!(item["id"], "42");
        assert_eq!(item["read_only"], false);
        for field in [
            "parent",
            "root",
            "delegation_id",
            "issuer_key_id",
            "operation_id",
        ] {
            assert_eq!(item[field], serde_json::Value::Null, "{field}");
        }
        // A delegated agent: its lineage by ID, and no certificate.
        let child = listed["agents"][1].as_object().ok_or("a child")?;
        assert_eq!(child.keys().count(), keys.len());
        assert_eq!(child["id"], "42.c7");
        assert_eq!(child["parent"], "42");
        assert_eq!(child["root"], "42");
        assert_eq!(child["owner"], "admin");
        assert_eq!(child["delegation_id"], "d1");
        assert_eq!(child["issuer_key_id"], "k1");
        assert_eq!(child["operation_id"], "op");
        assert_eq!(child["cert_expires_at"], serde_json::Value::Null);
        assert_eq!(child["issued_by"], "delegation");
        Ok(())
    }

    #[test]
    fn shell_quoting_keeps_plain_words_and_quotes_the_rest() {
        assert_eq!(shell_quote("--agent"), "--agent");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
        assert_eq!(shell_quote("$(rm -rf ~)"), "'$(rm -rf ~)'");
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn a_missing_identity_names_the_commands_that_make_one() {
        let dir = AgentDir::new(Path::new("/data/agents/prod"), "42".parse().expect("an ID"));
        let name: ContextName = "prod".parse().expect("a name");
        let text = no_identity(&dir, Some((&name, &Context::default())));
        let path = Path::new("/data/agents/prod").join("42");
        assert!(
            text.starts_with(&format!(
                "agent '42' has no identity in {}\n  hint: ",
                path.display()
            )),
            "{text}"
        );
        assert!(
            text.contains("fleetring agent add 42 --context prod --authority <dir with ca.key>"),
            "{text}"
        );
        let dev = Context {
            dev_dir: Some(PathBuf::from("/home/me/.fleetring")),
            ..Context::default()
        };
        assert!(no_identity(&dir, Some((&name, &dev))).contains("`fleetring agent add 42`"));
    }
}
