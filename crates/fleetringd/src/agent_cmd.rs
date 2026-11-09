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
use fleetring_proto::v1::revocation::Revoked;
use fleetring_proto::v1::{self as proto, RevokeTokenRequest};
use fleetringd::Network;
use fleetringd::auth::{MAX_TOKEN_LIFETIME, Scope};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::data::Client;
use fleetringd::dev::{self, Detached, DevChecked};
use fleetringd::mcp::Access;
use fleetringd::report::table;
use fleetringd::tls::{Identity, Security};
use fleetringd::upgrade::{Running, running};

use crate::agent::{
    self, AUTHORITY_CERT_LIFETIME, Acquired, AgentDir, AgentError, AgentId, AgentRecord,
    DEV_CERT_LIFETIME, IssuedBy, Issuer, MintSpec, OldToken, RENEW_TOKEN_WITHIN, Renewal, Renewed,
    ScopeRenewal, ScopesFrom,
};
use crate::confirm::{self, Place};
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
    /// `stream:agent-<id>-*`, kept private in a directory of its own. In dev mode it starts the dev node if none runs, and signs with dev
    /// mode's authority; for another fleet, pass `--authority`.
    Add(AgentAddArgs),
    /// Lists the context's agents, with their scopes and expiries
    ///
    /// Their principals, scopes, and when their credentials expire.
    List {
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Shows one agent: its principal, scopes, expiries, files, and MCP command
    ///
    /// The MCP command is the one that serves as the agent. It also shows when its token must be
    /// renewed by.
    Show {
        /// The agent's ID.
        #[arg(add = ArgValueCandidates::new(completions::agent_ids))]
        id: AgentId,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Reissues an agent's certificate and token now, and revokes the old token
    ///
    /// An identity that holds exactly the scope 0.7 gave by default, `admin:agent-<id>-*` (or
    /// `read:agent-<id>-*` for a read-only one), also gets `stream:agent-<id>-*`, today's
    /// default. Scopes chosen with `--scope` are kept as they are.
    Renew {
        /// The agent's ID.
        #[arg(add = ArgValueCandidates::new(completions::agent_ids))]
        id: AgentId,
        /// The directory with the fleet's `ca.crt` and `ca.key`
        ///
        /// Dev mode's own in dev mode, and required elsewhere.
        #[arg(long, value_name = "DIR")]
        authority: Option<PathBuf>,
        #[command(flatten)]
        target: Target,
    },
    /// Revokes an agent's token and deletes its directory
    ///
    /// Asks for the ID to be typed on a terminal first. Refuses if the fleet can't be reached to
    /// revoke the token.
    Remove {
        /// The agent's ID.
        #[arg(add = ArgValueCandidates::new(completions::agent_ids))]
        id: AgentId,
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
    /// The defaults are `admin:agent-<id>-*` and `stream:agent-<id>-*`. Repeat it for more;
    /// `agent renew` never widens scopes chosen with it.
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

/// What `mcp --agent` serves with.
#[derive(Debug)]
pub(crate) struct AgentReady {
    dir: AgentDir,
    record: AgentRecord,
    /// In dev mode, the dev node's control address, for when no context names one.
    dev_control: Option<String>,
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
        let mut text = String::new();
        // A start never widens an identity; it says how to.
        if let Some(widened) = self.dir.id().widened(record) {
            let scopes: Vec<String> = widened.iter().map(ToString::to_string).collect();
            let _ = writeln!(
                text,
                "note: agent {id}'s identity predates the stream scope, so it can't follow its \
                 databases' changes; `fleetring agent renew {id}` gives it {}",
                scopes.join(" and "),
                id = self.dir.id(),
            );
        }
        let renewal = if self.dev_control.is_some() {
            "renewed at start when under 7 days".to_owned()
        } else {
            format!(
                "renew it with `fleetring agent renew {} --authority <dir>` where the fleet's \
                 authority is",
                self.dir.id()
            )
        };
        let _ = writeln!(
            text,
            "serving MCP as {} on {context}; certificate expires {}, token expires {} \
             ({renewal})",
            record.principal, record.cert_expires_at, record.token_expires_at
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
    let dir = AgentDir::new(&situation.agents_dir, id);
    let dev_dir = match (&situation.dev_dir, from) {
        (Some(dev_dir), crate::AgentFrom::Flag) => dev_dir,
        // Only `--agent` mints; a variable left in a shell never does.
        _ => {
            let record = dir
                .record()?
                .ok_or_else(|| no_identity(&dir, situation.selected()))?;
            return Ok(Some(AgentReady {
                dir,
                record,
                dev_control: None,
            }));
        }
    };
    acquire_node(dev_dir, MachineOutput::Yes).await?;
    // Immediately before the mint, whatever found or started the node.
    let checked = dev::check_dev_dir(dev_dir)?;
    let spec = dev_spec(dir.id(), Vec::new(), access, None);
    let acquired = dir.acquire(Issuer::Dev(&checked), &spec, SystemClock.now())?;
    match &acquired {
        Acquired::Minted(_) => note(format!(
            "minted agent {}'s identity in {}",
            dir.id(),
            dir.tls_dir().display()
        )),
        Acquired::Renewed { old, renewal, .. } => {
            note(format!(
                "renewed agent {}'s {}",
                dir.id(),
                renewed(*renewal)
            ));
            if let Some(old) = old {
                match dev_admin(&checked) {
                    Ok(admin) => revoke_old(&admin, old).await,
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
        dir,
        record: acquired.record().clone(),
        dev_control: control,
    }))
}

/// Runs an `agent` command and returns what it prints on stdout.
pub(crate) async fn run(command: AgentCommand) -> Result<String, Box<dyn Error>> {
    match command {
        AgentCommand::Add(args) => add(args).await,
        AgentCommand::List { output, target } => {
            let situation = situation(&target)?;
            list(&situation, output.format())
        }
        AgentCommand::Show { id, output, target } => {
            let situation = situation(&target)?;
            show(&situation, id, output.format())
        }
        AgentCommand::Renew {
            id,
            authority,
            target,
        } => renew(id, authority.as_deref(), &target).await,
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
    let record = match (&args.authority, &situation.dev_dir) {
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
            let cert_ttl = args.ttl.unwrap_or(AUTHORITY_CERT_LIFETIME);
            let (scopes, scopes_from) = scopes_or_default(&args.id, args.scopes, access);
            let spec = MintSpec {
                scopes,
                scopes_from,
                access,
                cert_ttl,
                token_ttl: cert_ttl.min(MAX_TOKEN_LIFETIME),
                owner,
            };
            dir.mint(Issuer::Authority(&authority), &spec, now)?
        }
        (None, Some(dev_dir)) => {
            acquire_node(dev_dir, MachineOutput::No).await?;
            let checked = dev::check_dev_dir(dev_dir)?;
            let spec = dev_spec(&args.id, args.scopes, access, args.ttl);
            dir.mint(Issuer::Dev(&checked), &spec, now)?
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
    let context = situation.pinned();
    let invocation = Invocation::of(&args.id, &context)?;
    let summary = describe(&dir, &record, &context, &invocation, Detail::Summary);
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

/// The scopes `--scope` gave, or the agent's defaults for `access`, and which they are.
fn scopes_or_default(id: &AgentId, scopes: Vec<Scope>, access: Access) -> (Vec<Scope>, ScopesFrom) {
    if scopes.is_empty() {
        (id.default_scopes(access), ScopesFrom::Defaults)
    } else {
        (scopes, ScopesFrom::Flags)
    }
}

/// What a dev mode identity gets: a certificate for a year, or `ttl`, and a token for 30 days,
/// or `ttl` if that is shorter, signed by dev mode's admin.
fn dev_spec(id: &AgentId, scopes: Vec<Scope>, access: Access, ttl: Option<Duration>) -> MintSpec {
    let cert_ttl = ttl.unwrap_or(DEV_CERT_LIFETIME);
    let (scopes, scopes_from) = scopes_or_default(id, scopes, access);
    MintSpec {
        scopes,
        scopes_from,
        access,
        cert_ttl,
        token_ttl: cert_ttl.min(MAX_TOKEN_LIFETIME),
        owner: dev::PRINCIPAL.to_owned(),
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

/// What [`revoke`] did.
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

/// Revokes `token` through `client` until the token itself expires, which [`OldToken`] reads
/// from the token, never from `identity.toml`.
async fn revoke(client: &Client, token: &OldToken) -> Result<Revocation, Box<dyn Error>> {
    if token.until <= SystemClock.now() {
        return Ok(Revocation::Expired);
    }
    let request = RevokeTokenRequest {
        revocation: Some(proto::Revocation {
            revoked: Some(Revoked::TokenId(token.id.clone())),
            until_ms: token.until.as_millis(),
        }),
    };
    let answer = tokio::time::timeout(REVOKE_TIMEOUT, client.control().revoke_token(request))
        .await
        .map_err(|_| {
            format!(
                "the fleet didn't answer within {}s",
                REVOKE_TIMEOUT.as_secs()
            )
        })?
        .map_err(|status| crate::exit::describe(&status))?;
    Ok(Revocation::Until(Timestamp::from_millis(answer.until_ms)))
}

/// Revokes a replaced token, and warns rather than fails if it can't (ADR-0043, point 8.2).
async fn revoke_old(client: &Client, old: &OldToken) {
    match revoke(client, old).await {
        Ok(revocation) => note(revocation.describe(old)),
        Err(error) => note(format!(
            "warning: couldn't revoke the old token {} ({error}); it stays valid until {}",
            old.id,
            format_time(old.until.as_millis())
        )),
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
/// holds 0.7's default scope today's defaults.
async fn renew(
    id: AgentId,
    authority: Option<&Path>,
    target: &Target,
) -> Result<String, Box<dyn Error>> {
    let situation = situation(target)?;
    let dir = AgentDir::new(&situation.agents_dir, id);
    let now = SystemClock.now();
    let (renewed, client) = match (authority, &situation.dev_dir) {
        (Some(authority_dir), _) => {
            let authority = agent::load_authority(authority_dir)?;
            // A renewal takes the scopes and the access from the record.
            let spec = MintSpec {
                scopes: Vec::new(),
                scopes_from: ScopesFrom::Defaults,
                access: Access::ReadWrite,
                cert_ttl: AUTHORITY_CERT_LIFETIME,
                token_ttl: MAX_TOKEN_LIFETIME,
                owner: String::new(),
            };
            let renewed = dir.renew(Issuer::Authority(&authority), &spec, now)?;
            (renewed, target.client().ok())
        }
        (None, Some(dev_dir)) => {
            let checked = dev::check_dev_dir(dev_dir)?;
            let spec = dev_spec(dir.id(), Vec::new(), Access::ReadWrite, None);
            let renewed = dir.renew(Issuer::Dev(&checked), &spec, now)?;
            (renewed, dev_admin(&checked).ok())
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
    match client {
        Some(client) => revoke_old(&client, &old).await,
        None => note(format!(
            "warning: no client to revoke the old token {} with; it stays valid until {}",
            old.id,
            format_time(old.until.as_millis())
        )),
    }
    Ok(format!(
        "renewed agent {}: certificate expires {}, token expires {}\n  scopes: {}\n",
        dir.id(),
        record.cert_expires_at,
        record.token_expires_at,
        renewed_scopes(&record, &scopes)
    ))
}

/// What `agent renew` says about the scopes the new token carries.
fn renewed_scopes(record: &AgentRecord, scopes: &ScopeRenewal) -> String {
    let held = record.scopes.join(", ");
    match scopes {
        ScopeRenewal::Widened { added } => format!(
            "{held} (added {}, a default since 0.8; the identity held only 0.7's default)",
            added.join(", ")
        ),
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

/// `agent remove`: revokes the token, then deletes the directory (ADR-0043, point 8.7).
async fn remove(
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
        confirm_removal(&dir, &record, &situation.place(&record))?;
    }
    // Read from the token itself before the directory goes.
    let token = dir.old_token(&record, SystemClock.now());
    let until = format_time(token.until.as_millis());
    let revoked = match revoke_token {
        Revoke::Skip => {
            note(format!(
                "warning: not revoking token {}: it stays valid until {until}",
                token.id
            ));
            String::new()
        }
        Revoke::Required => {
            let client = match &situation.dev_dir {
                Some(dev_dir) => dev_admin(&dev::check_dev_dir(dev_dir)?)?,
                None => target.client()?,
            };
            let revocation = revoke(&client, &token).await.map_err(|error| {
                format!(
                    "can't revoke token {} ({error}), so nothing was removed\n  hint: pass \
                     --no-revoke to delete the directory anyway; the token then stays valid \
                     until {until}",
                    token.id
                )
            })?;
            format!("{} and ", revocation.describe(&token))
        }
    };
    dir.remove()?;
    Ok(format!(
        "removed agent {}: {revoked}deleted {}\n",
        dir.id(),
        dir.tls_dir().display()
    ))
}

/// Asks for the agent's ID to be typed on the terminal; refuses without one.
fn confirm_removal(
    dir: &AgentDir,
    record: &AgentRecord,
    place: &Place,
) -> Result<(), Box<dyn Error>> {
    confirm::confirm(&confirm::remove_agent(
        dir.id().as_str(),
        &record.principal,
        &dir.tls_dir().display().to_string(),
        place,
    ))
}

/// `agent list`.
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
            .and_then(|name| name.parse::<AgentId>().ok())
        else {
            continue;
        };
        let dir = AgentDir::new(&situation.agents_dir, id);
        match dir.record() {
            Ok(Some(record)) => agents.push((dir, record)),
            Ok(None) => {}
            Err(error) => note(format!("warning: skipping agent '{}': {error}", dir.id())),
        }
    }
    agents.sort_by(|(left, _), (right, _)| left.id().cmp(right.id()));
    render_list(&agents, format)
}

fn render_list(
    agents: &[(AgentDir, AgentRecord)],
    format: OutputFormat,
) -> Result<String, Box<dyn Error>> {
    let listed: Vec<serde_json::Value> = agents
        .iter()
        .map(|(dir, record)| agent_json(dir, record))
        .collect();
    format::render(format, &serde_json::json!({ "agents": listed }), |_| {
        let rows: Vec<[String; 6]> = agents
            .iter()
            .map(|(dir, record)| {
                [
                    dir.id().to_string(),
                    record.principal.clone(),
                    record.scopes.join(","),
                    read_only_text(record).to_owned(),
                    record.cert_expires_at.clone(),
                    record.token_expires_at.clone(),
                ]
            })
            .collect();
        Ok(table(
            [
                "ID",
                "PRINCIPAL",
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
    id: AgentId,
    format: OutputFormat,
) -> Result<String, Box<dyn Error>> {
    let dir = AgentDir::new(&situation.agents_dir, id);
    let Some(record) = dir.record()? else {
        return Err(no_identity(&dir, situation.selected()).into());
    };
    let shown = serde_json::json!({ "agents": [agent_json(&dir, &record)] });
    format::render(format, &shown, |_| {
        let context = situation.pinned();
        let invocation = Invocation::of(dir.id(), &context)?;
        Ok(describe(&dir, &record, &context, &invocation, Detail::Full))
    })
}

fn agent_json(dir: &AgentDir, record: &AgentRecord) -> serde_json::Value {
    serde_json::json!({
        "id": dir.id().as_str(),
        "principal": record.principal,
        "scopes": record.scopes,
        "read_only": record.read_only,
        "cert_expires_at": record.cert_expires_at,
        "token_expires_at": record.token_expires_at,
        "issued_by": record.issued_by.to_string(),
        "dir": dir.tls_dir(),
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
    dir: &AgentDir,
    record: &AgentRecord,
    context: &str,
    invocation: &Invocation,
    detail: Detail,
) -> String {
    let id = dir.id();
    let mut text = match detail {
        Detail::Summary => format!(
            "added agent {id} as {} on context '{context}', issued by {}\n",
            record.principal,
            match record.issued_by {
                IssuedBy::Dev => "dev mode's authority",
                IssuedBy::Authority => "the authority",
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
        line("parent", parent);
    }
    line("fleet", &record.fleet);
    line("scopes", &record.scopes.join(", "));
    line("read-only", read_only_text(record));
    line("cert expires", &record.cert_expires_at);
    line("token expires", &record.token_expires_at);
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
    };
    line("renew by", &renew_by);
    line("directory", &dir.tls_dir().display().to_string());
    if detail == Detail::Full {
        line("token file", &dir.token_file().display().to_string());
        line("record", &dir.record_file().display().to_string());
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
    fn of(id: &AgentId, context: &str) -> Result<Self, Box<dyn Error>> {
        let exe = this_binary()?;
        let exe = exe
            .to_str()
            .ok_or_else(|| format!("{} isn't valid UTF-8", exe.display()))?
            .to_owned();
        Ok(Self {
            exe,
            args: ["mcp", "--agent", id.as_str(), "--context", context]
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
            fleet: "dev".to_owned(),
            scopes: vec!["admin:agent-42-*".to_owned()],
            scopes_from: ScopesFrom::Defaults,
            read_only: false,
            token_id: "t1".to_owned(),
            token_expires_at: "2026-12-01T00:00:00Z".to_owned(),
            cert_expires_at: "2027-11-01T00:00:00Z".to_owned(),
            issued_by: IssuedBy::Dev,
        };
        let listed: serde_json::Value =
            serde_json::from_str(&render_list(&[(dir, record)], OutputFormat::Json)?)?;
        let item = listed["agents"][0].as_object().ok_or("an agent")?;
        let keys: Vec<&str> = item.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [
                "cert_expires_at",
                "dir",
                "id",
                "issued_by",
                "principal",
                "read_only",
                "scopes",
                "token_expires_at"
            ]
        );
        assert_eq!(item["id"], "42");
        assert_eq!(item["read_only"], false);
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
