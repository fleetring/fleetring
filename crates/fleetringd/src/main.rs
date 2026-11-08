//! The `fleetring` binary: a node, and the commands that talk to a running fleet. It also runs as
//! `fleetringd`, its compatibility name until 1.0 (ADR-0043).

use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::io::{IsTerminal as _, Read, Write as _};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::OnceLock;
use std::time::Duration;

use clap::{Args, FromArgMatches as _, Parser, Subcommand};
use fleetring::{NodeId, ReplicationFactor};
use fleetring_client::context::{ContextName, Contexts, select};
use fleetring_client::output::{
    DatabaseList, FleetStatus, NodeStatusReport, OrphanList, ReplicaHash, VerifyReport, printable,
};
use fleetring_control::{
    Acks, DatabaseName, Description, LabelKey, LabelValue, Labels, Liveness, parse_label,
};
use fleetring_control::{KeyId, Millis, ParseReleaseError, Release, Timestamp, TokenId};
use fleetring_proto::v1::revocation::Revoked;
use fleetring_proto::v1::{Revocation, RevokeTokenRequest};
use fleetringd::auth::{BearerToken, DEFAULT_TOKEN_LIFETIME, Grant, MAX_TOKEN_LIFETIME, Scope};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::data::{
    ArchiveDescription, Client, DatabaseSpec, Expiry, LabelRequirement, LeaseUpdate, LeaseUpdates,
    ListFilter, Location, MetadataView, NewMetadata, Orphan, ReplicaInfo, Restoration,
    RestoredExpiry, restored_metadata,
};
use fleetringd::dev::{self, DevError, Prepared};
use fleetringd::durability::{
    ArchiveKeys, RestoreTarget, build_store, encode_metadata, finish_offline, restore_into,
};
use fleetringd::engine::{
    Execution, Internal, MAX_COUNT_LIMIT, Position, Rows, SchemaRequest, SqliteEngine, Statement,
    Value,
};
use fleetringd::mcp::{self, Access, McpSettings};
use fleetringd::observe::{LogColour, log_subscriber};
use fleetringd::replication::ChainHash;
use fleetringd::report::{
    archive_report, database_location, database_report, expiry_text, fleet_status, lease_events,
    liveness_name, metadata_report, node_list, node_list_text, node_status, orphan_report,
    query_result, render_archived_metadata, render_text, schema_report, schema_unavailable, table,
};
use fleetringd::tls::{
    AUTHORITY_CERT_FILE, Authority, DEFAULT_AUTHORITY_LIFETIME, DEFAULT_CLIENT_LIFETIME,
    DEFAULT_NODE_LIFETIME, FleetName, Identity, Peer, Principal, Security, Validity,
};
use fleetringd::transport::Credentials;
#[cfg(unix)]
use fleetringd::upgrade::RESTARTS_VAR;
use fleetringd::upgrade::{self, LocalNode, Mode, Options, ProcessFile, Restart, StartCommand};
use fleetringd::{
    Config, Daemon, LogFormat, Network, NodeRole, ParseLogFormatError, Plaintext, Stop,
};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::util::SubscriberInitExt as _;

mod agent;
mod agent_cmd;
mod compat;
mod completions;
mod confirm;
mod context_cmd;
mod dev_cmd;
mod edit;
mod exit;
mod format;
mod help;
#[cfg(test)]
mod man;
mod progress;
mod resolve;
mod serve_check;
mod stop;
mod watch_cmd;

use agent::{AgentDir, AgentId};
use agent_cmd::AgentCommand;
use clap_complete::engine::ArgValueCandidates;
use compat::{Invocation, ProgramName};
use completions::CompletionsArgs;
use confirm::Place;
use context_cmd::ContextCommand;
use dev_cmd::DevArgs;
use exit::Failed;
use format::{OutputArgs, OutputFormat};
use progress::{
    ColorChoice, MachineOutput, Progress, Quiet, Stepped, Surroundings, SuspendingStderr,
};
use resolve::{
    ContextsFile, ContextsUnusable, EnvSnapshot, Resolved, SecurityFiles, TargetFlags, TokenSource,
    Wire,
};
use stop::StopArgs;

/// The environment variable that chooses the log format when `--log-format` isn't given.
const LOG_FORMAT_VAR: &str = "FLEETRING_LOG_FORMAT";

/// Places, replicates, and fails over SQLite databases across machines you own.
///
/// `serve` runs a node. The other commands talk to a running fleet.
#[derive(Debug, Parser)]
#[command(
    name = "fleetring",
    version,
    arg_required_else_help = true,
    max_term_width = 100
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
    /// Writes log lines as `text` or `json`
    ///
    /// Overrides `FLEETRING_LOG_FORMAT` and the configuration file's `[log] format`.
    #[arg(long, global = true, value_parser = parse_log_format)]
    log_format: Option<LogFormat>,
    /// Prints the result and errors only
    ///
    /// No progress, notes, warnings or override warnings. A command with no result then prints
    /// nothing.
    #[arg(short, long, global = true)]
    quiet: bool,
    /// When to colour log lines and the picker
    ///
    /// `auto` colours on a terminal unless `NO_COLOR` is set or `TERM` is `dumb`; `always` and
    /// `never` do as they say.
    #[arg(long, global = true, value_enum, default_value_t = ColorChoice::Auto)]
    color: ColorChoice,
}

/// `fleetring serve`'s flags.
#[derive(Debug, Args)]
struct DaemonArgs {
    /// The TOML configuration file.
    #[arg(long)]
    config: PathBuf,
    /// Overrides `node.id`.
    #[arg(long)]
    node_id: Option<NodeId>,
    /// Overrides `data_dir`.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Overrides `listen`.
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// Overrides `node.advertise`.
    #[arg(long)]
    advertise: Option<SocketAddr>,
    /// Overrides `control.address`.
    #[arg(long)]
    control: Option<SocketAddr>,
    /// Overrides `role`: `both`, `data`, or `control`.
    #[arg(long, value_parser = parse_role)]
    role: Option<NodeRole>,
    /// Runs without a `[tls]` table, in plaintext, for local experiments only
    ///
    /// Nothing on the wire is authenticated or encrypted.
    #[arg(long)]
    insecure_plaintext: bool,
    /// Checks the configuration and these flags as `serve` would, and starts nothing
    ///
    /// Reads the file, applies the flags, and checks every rule the node checks before it
    /// starts, in the same code. It reads none of the files the configuration names
    /// (certificates, keys, tokens, credentials), no environment variable and no data directory,
    /// and binds nothing. It exits 0 if the node would start. Otherwise it prints the first
    /// problem, as the node would stop at it, naming the file, the field, and the line and column
    /// where the parser gives them, and exits 1. `fleetring upgrade` runs the new release's check
    /// on the node's command line before it swaps the binary.
    #[arg(long)]
    check: bool,
    /// The check's output format: `table`, `json`, or `jsonl`
    ///
    /// `table` is for people; `json` is one object: `config`, `valid`, `node`, `role`, and
    /// `problems`, each `field`, `line`, `column` and `message`; `jsonl` is the same object on one
    /// line. Needs `--check`.
    #[arg(short = 'o', long, value_enum, requires = "check")]
    output: Option<OutputFormat>,
    /// Short for `-o json`; needs `--check`
    #[arg(long, requires = "check", conflicts_with = "output")]
    json: bool,
}

/// What `fleetring serve --help` says: the details, and the compatibility form (ADR-0043,
/// point 1.4).
const SERVE_HELP: &str = "\
Runs a node.

Runs a node of a fleet from its configuration file; the flags override the file's settings.
With `--check`, it checks the file and the flags as a start would, and starts nothing.

Until 1.0, the flags without `serve` (`fleetring --config <file>`, or `fleetringd --config
<file>`, as 0.6 ran a node) also run a node, and log a warning.";

#[derive(Debug, Subcommand)]
enum Command {
    /// Runs a node
    #[command(long_about = SERVE_HELP)]
    Serve(DaemonArgs),
    /// Runs a node for development; `-d` runs it in the background
    ///
    /// One node, on loopback only, with a local authority and an admin principal that every
    /// command can act as (ADR-0039). The first run creates them, with the node's configuration,
    /// in the directory; later runs reuse them. Every start writes the `dev` context and makes it
    /// current unless another context is, so the other commands need no flags. `dev stop`, `dev
    /// status` and `dev logs` act on the node `-d` started. Not for production.
    Dev(DevArgs),
    /// Stops a node started in a terminal or a script, after it drains
    ///
    /// Sends the node SIGTERM through its process file, which drains it, and waits until it has
    /// exited. Refuses a node a supervisor runs. `dev stop` stops the dev node.
    Stop(StopArgs),
    /// Names, lists and selects contexts, so commands need no flags
    ///
    /// A context names a fleet's control addresses, certificates and principal (ADR-0043).
    #[command(subcommand)]
    Context(ContextCommand),
    /// Makes another context current
    ///
    /// By name, `-` for the previous one, or with no name a picker on a terminal. Without a
    /// terminal and without a name it lists the contexts and exits 2.
    Switch {
        /// The context to switch to, or `-` for the previous one.
        #[arg(allow_hyphen_values = true, add = ArgValueCandidates::new(completions::context_names))]
        target: Option<String>,
    },
    /// Prints a completion script for bash, zsh, fish, or powershell
    ///
    /// The script completes commands, flags and values, with context names and agent IDs read
    /// from this machine's files.
    #[command(after_long_help = completions::COMPLETIONS_HELP)]
    Completions(CompletionsArgs),
    /// Creates the authority, and issues node and client certificates
    #[command(subcommand)]
    Cert(CertCommand),
    /// Issues and revokes bearer tokens
    ///
    /// A token is a signed, expiring credential that names a principal and carries its scopes.
    #[command(subcommand)]
    Token(TokenCommand),
    /// Creates, lists, writes, reads, watches, and drops databases
    #[command(subcommand)]
    Db(DbCommand),
    /// Lists, drains, undrains, and removes the fleet's members
    #[command(subcommand)]
    Node(NodeCommand),
    /// Follows the leases data nodes hold
    #[command(subcommand)]
    Lease(LeaseCommand),
    /// Lists and removes orphans: copies the control plane doesn't know
    ///
    /// An orphan is a copy a data node keeps, closed, because the control plane didn't know its
    /// database, or held it only at a lower epoch, and the node saw no drop (ADR-0042).
    #[command(subcommand)]
    Orphan(OrphanCommand),
    /// Shows and verifies archives, and restores from one with no fleet
    #[command(subcommand)]
    Archive(ArchiveCommand),
    /// Installs another release in place of this binary
    ///
    /// The release is verified with cosign against the release workflow's identity, and then by
    /// checksum; this binary is kept as `<name>.previous`. With `--config`, the release checks the
    /// node's configuration with `serve --check` before it is swapped in, and one it refuses
    /// stops the upgrade unless `--no-restart`. With `--config` naming a running node, it first
    /// checks that the fleet allows the release, then drains the node, restarts it in place, and
    /// waits until it has rejoined and caught up (ADR-0038).
    Upgrade(UpgradeArgs),
    /// Serves the Model Context Protocol on stdio, as one principal
    ///
    /// An agent's MCP client can then create, find, use, describe, and drop databases, acting as
    /// the one principal these credentials name and within its scopes (ADR-0041). It reads the
    /// same flags and variables as every other command, never credentials from the client, and
    /// runs until the client closes stdin. stdout carries the protocol alone; logs go to stderr.
    #[command(after_long_help = MCP_HELP)]
    Mcp(McpArgs),
    /// Adds, lists, shows, renews, and removes agents' identities
    ///
    /// An identity is a principal `agent-<id>` with a certificate and a scoped token, kept in a
    /// directory per agent (ADR-0043, point 8). In dev mode, `fleetring mcp --agent <id>` adds
    /// one by itself. Trusted local development: agents running as the same user can read each
    /// other's credentials.
    #[command(subcommand)]
    Agent(AgentCommand),
    /// Shows the fleet's members and ring, or one data node's view
    ///
    /// The members with their liveness and release, the membership epoch, and the ring
    /// fingerprint, as the control plane sees them; with `--node` or `--address`, as one data
    /// node sees them.
    Status {
        /// Asks this data node instead of the control plane
        ///
        /// It reports its membership epoch, ring fingerprint, and the databases it serves. Its
        /// address comes from the control plane's membership, so `--control` or `--config` is
        /// needed too.
        #[arg(long, conflicts_with = "address")]
        node: Option<NodeId>,
        /// Asks the data node at `host:port` instead of the control plane
        ///
        /// Nothing else is needed.
        #[arg(long, conflicts_with_all = ["control", "config"])]
        address: Option<String>,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
}

/// `fleetring mcp`'s flags (ADR-0041, point 9).
#[derive(Debug, Args)]
struct McpArgs {
    /// Offers only the tools that read: `db_list`, `db_show`, `query`, and `changes`
    ///
    /// The principal's scopes still bound what they read.
    #[arg(long)]
    read_only: bool,
    /// The most bytes one result's JSON holds: from 64KiB to 4MiB
    ///
    /// Bytes, with an optional KiB or MiB. Rows, listed databases, and schema objects past it are
    /// left out, and the result says so.
    #[arg(long, value_parser = parse_result_size, default_value = "256KiB")]
    max_result_bytes: usize,
    #[command(flatten)]
    target: Target,
}

/// What `fleetring mcp --help` adds below the flags: how an MCP client starts the server.
const MCP_HELP: &str = "\
An MCP client starts the server as a command. `--agent <id>` is the whole setup: in dev
mode it starts a dev node if none runs, mints the agent's identity (agent-<id>, scoped
admin:agent-<id>-* and stream:agent-<id>-*) on first use, and renews it at start when it is
close to expiring.
The configuration holds no secret (Claude Code's .mcp.json, Cursor's mcp.json):

  {\"mcpServers\": {\"fleetring\": {\"command\": \"/usr/local/bin/fleetring\",
    \"args\": [\"mcp\", \"--agent\", \"42\", \"--context\", \"dev\"]}}}

`fleetring agent add 42 --print-config json` prints it with this binary's path, and
`claude mcp add fleetring -- fleetring mcp --agent 42 --context dev` registers it with
Claude Code. For a fleet that isn't dev mode's, `fleetring agent add 42 --context <name>
--authority <dir>` mints the identity where the authority is.

This is trusted local development: agents on this machine can read each other's
credentials, so separate identities don't isolate agents from each other.";

/// `fleetring upgrade`'s flags (ADR-0038).
#[derive(Debug, Args)]
struct UpgradeArgs {
    /// The release to install, such as 0.4.0; the newest final release by default
    ///
    /// Naming the installed release reinstalls it.
    #[arg(long, value_parser = parse_release)]
    version: Option<Release>,
    /// Reports the installed and the newest release, and changes nothing
    ///
    /// It also reports whether the fleet allows the newest release, and exits 1 if a newer
    /// release exists that the fleet doesn't allow.
    #[arg(long, conflicts_with_all = ["rollback", "no_restart"])]
    check: bool,
    /// Replaces the binary, but leaves the running node on the release it runs
    ///
    /// It also installs a release that refuses the node's configuration, and says so: edit the
    /// configuration until `fleetring serve --config <file> --check` passes, then restart the
    /// node. Until then, any restart, by a supervisor, a crash or a reboot, leaves it down.
    #[arg(long)]
    no_restart: bool,
    /// Swaps back the binary the last upgrade replaced, `<name>.previous`
    ///
    /// It restarts the node as an upgrade does. A second rollback rolls forward again.
    #[arg(long, conflicts_with_all = ["version", "base_url", "releases_url"])]
    rollback: bool,
    /// Downloads the release's files from this mirror URL instead of GitHub
    ///
    /// A mirror's directory for one release, as `install.sh --base-url` takes it. Needs
    /// `--version`.
    #[arg(long, env = "FLEETRING_BASE_URL", requires = "version")]
    base_url: Option<String>,
    /// Reads the list of releases from this URL instead of GitHub's
    ///
    /// The list is the JSON GitHub's API returns; a mirror's lets `upgrade` find its newest
    /// release.
    #[arg(long, env = "FLEETRING_RELEASES_URL")]
    releases_url: Option<String>,
    /// How long each step may take: `<n>m` or `<n>s`; 5m by default
    ///
    /// The steps are each download, the configuration check, the drain, the rejoin, and the
    /// catch-up.
    #[arg(long, value_parser = parse_ttl)]
    timeout: Option<Duration>,
    /// Prints what this binary runs as one JSON object, and changes nothing
    ///
    /// The object holds its release, protocol, control-log format, and fleet features. An
    /// upgrade asks a release's binary this before it installs it, to check the fleet's
    /// features.
    #[arg(long, exclusive = true)]
    software: bool,
    /// Confirms without a prompt that no node here starts with `fleetring serve`
    ///
    /// Needed to install or roll back to a release before 0.7, which can't run `serve`, when no
    /// `--config` names a running node to check.
    #[arg(long, conflicts_with = "check")]
    yes: bool,
    #[command(flatten)]
    target: Target,
}

/// How long each step of an upgrade may take unless `--timeout` says otherwise.
const UPGRADE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

fn parse_release(text: &str) -> Result<Release, String> {
    let text = text
        .strip_prefix("fleetringd-v")
        .or_else(|| text.strip_prefix('v'))
        .unwrap_or(text);
    text.parse()
        .map_err(|error: ParseReleaseError| error.to_string())
}

#[derive(Debug, Subcommand)]
enum ArchiveCommand {
    /// Shows what an archive holds for a database
    ///
    /// Its snapshots, how far its segments reach, and anything a restore won't use.
    Show {
        /// The archived database.
        name: DatabaseName,
        /// The `[[archive]]` entry's name.
        #[arg(long)]
        archive: String,
        /// Asks the data node at `host:port` instead of the first live member
        #[arg(long)]
        address: Option<String>,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Restores an archive into a scratch copy, and compares it with the replicas
    ///
    /// A data node restores the archived database into a scratch copy, verifying every batch
    /// against the hash chain, and discards it. The command then asks each live replica for its
    /// hash at the restored position, and reports whether they agree with the archive.
    Verify {
        /// The archived database.
        name: DatabaseName,
        /// The `[[archive]]` entry's name.
        #[arg(long)]
        archive: String,
        /// Asks the data node at `host:port` instead of the first live member
        #[arg(long)]
        address: Option<String>,
        #[command(flatten)]
        point: PointArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Restores an archived database into a new directory, with no fleet
    ///
    /// For disaster recovery. The directory then holds a plain SQLite database, `data.db`, that
    /// `sqlite3` opens directly. Every batch is verified against the hash chain on the way. `db
    /// restore` is the online restore, into the fleet.
    Restore {
        /// The archived database to restore.
        name: DatabaseName,
        /// The directory to write, which must not exist.
        #[arg(long)]
        to: PathBuf,
        /// A daemon's configuration file, whose `[[archive]]` entry names the store.
        #[arg(long)]
        config: PathBuf,
        /// The `[[archive]]` entry's name.
        #[arg(long)]
        archive: String,
        #[command(flatten)]
        point: PointArgs,
    },
}

/// Which point of an archive a command restores: the latest unless one is named.
#[derive(Debug, Args)]
struct PointArgs {
    /// Restores exactly the log up to this position, a transaction boundary
    ///
    /// Positions are hex, as `db locate` and `archive show` print them.
    #[arg(long, value_parser = parse_position, conflicts_with = "at_time")]
    at_position: Option<Position>,
    /// Restores every segment archived at or before this time, in RFC 3339
    ///
    /// Such as `2026-11-03T09:30:00Z`, by the archiving nodes' clocks. The restore may stop up to
    /// one segment interval before it, and never holds a write acknowledged after it.
    #[arg(long, value_parser = parse_time)]
    at_time: Option<u64>,
}

impl PointArgs {
    fn target(&self) -> RestoreTarget {
        match (&self.at_position, self.at_time) {
            (Some(position), _) => RestoreTarget::Position(position.clone()),
            (None, Some(ms)) => RestoreTarget::Time(ms),
            (None, None) => RestoreTarget::Latest,
        }
    }
}

#[derive(Debug, Subcommand)]
enum LeaseCommand {
    /// Follows the leases one data node holds, until Ctrl-C
    ///
    /// First `held <database> epoch <n>` for each database it serves, then `gained` or `lost`
    /// lines as leases move, until the stream ends or Ctrl-C (exit 130). `-o jsonl` prints one
    /// object per line, flushed as each arrives.
    Watch {
        /// The data node to follow
        ///
        /// Its address comes from the control plane's membership, so `--control` or `--config` is
        /// needed too.
        #[arg(long, required_unless_present = "address", conflicts_with = "address")]
        node: Option<NodeId>,
        /// The address of the data node to follow, `host:port`
        ///
        /// Nothing else is needed.
        #[arg(long, conflicts_with_all = ["control", "config"])]
        address: Option<String>,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
}

#[derive(Debug, Subcommand)]
enum OrphanCommand {
    /// Lists the orphans each data node keeps, or one node's
    ///
    /// Each with the epoch the copy recorded and its size. `--node` or `--address` names one
    /// node.
    List {
        /// The data node to ask
        ///
        /// Its address comes from the control plane's membership, so `--control` or `--config` is
        /// needed too.
        #[arg(long, conflicts_with = "address")]
        node: Option<NodeId>,
        /// The address of the data node to ask, `host:port`
        ///
        /// Nothing else is needed.
        #[arg(long, conflicts_with_all = ["control", "config"])]
        address: Option<String>,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Deletes a data node's orphan copy of a database, for good
    ///
    /// Deletes its files and its epochs. Refuses a name the node serves, follows, or is
    /// retiring. Needs the `fleet` scope. Asks first on a terminal.
    Remove {
        /// The database the orphan copy is of.
        name: DatabaseName,
        /// The data node that keeps the orphan
        ///
        /// Its address comes from the control plane's membership, so `--control` or `--config` is
        /// needed too.
        #[arg(long, required_unless_present = "address", conflicts_with = "address")]
        node: Option<NodeId>,
        /// The address of the data node that keeps the orphan, `host:port`
        ///
        /// Nothing else is needed.
        #[arg(long, conflicts_with_all = ["control", "config"])]
        address: Option<String>,
        /// Deletes without asking
        ///
        /// Without it, the command asks first on a terminal, and refuses elsewhere.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        target: Target,
    },
}

#[derive(Debug, Subcommand)]
#[expect(
    clippy::large_enum_variant,
    reason = "a command is parsed once per process, and clap can't box a flattened `Target`"
)]
enum TokenCommand {
    /// Issues a token signed with the authority's key, and prints it
    ///
    /// Run it where `ca.key` is, off the nodes; nodes verify the token with `ca.crt` alone.
    Issue {
        /// The principal the token names
        ///
        /// A client sending it must connect with a certificate for the same principal, unless that
        /// principal forwards tokens. A name that starts with `agent-` may not contain `.`, which
        /// is reserved for delegated agents.
        #[arg(long)]
        principal: Principal,
        /// A scope the token carries: `<action>:<pattern>`, or `fleet`
        ///
        /// The action is `read`, `write`, `admin`, or `stream`, and the pattern a database name or
        /// a prefix ending in `*`. Repeat it for more.
        #[arg(long = "scope", required = true)]
        scopes: Vec<Scope>,
        /// How long the token lasts: `<n>d`, `<n>h`, `<n>m`, or `<n>s`
        ///
        /// One hour by default, and 30 days at most.
        #[arg(long, value_parser = parse_ttl)]
        ttl: Option<Duration>,
        /// The directory with `ca.crt` and `ca.key`.
        #[arg(long)]
        dir: PathBuf,
    },
    /// Revokes a token by its ID, or every token a signing key issued
    ///
    /// Every node refuses it within one heartbeat interval.
    Revoke {
        /// The token's ID, as `token issue` printed it (its `jti` claim).
        ///
        /// IDs are base64url, so one may start with `-`; the value is taken as given.
        #[arg(
            long,
            allow_hyphen_values = true,
            required_unless_present = "key_id",
            conflicts_with = "key_id"
        )]
        token_id: Option<TokenId>,
        /// A signing key's ID, to revoke every token it signed, for good
        ///
        /// The ID is the token header's `kid`.
        #[arg(long, allow_hyphen_values = true)]
        key_id: Option<KeyId>,
        /// How long to keep a token's revocation; 30 days by default
        ///
        /// At least what remains of the token's lifetime. 30 days is the longest a token lasts.
        #[arg(long, value_parser = parse_ttl, conflicts_with = "key_id")]
        ttl: Option<Duration>,
        /// Revokes a signing key without asking
        ///
        /// Without it, `--key-id` asks first on a terminal, and refuses elsewhere; `--token-id`
        /// never asks.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        target: Target,
    },
}

#[derive(Debug, Subcommand)]
enum CertCommand {
    /// Creates the authority: `ca.crt` and `ca.key` in the directory
    ///
    /// Keep `ca.key` off every node; it is the one secret that issues certificates.
    Init {
        /// The fleet's name, which every certificate the authority issues names.
        #[arg(long)]
        fleet: FleetName,
        /// The directory to write into; created if missing.
        #[arg(long)]
        dir: PathBuf,
        /// How long the authority's certificate lasts: `<n>d`, `<n>h`, `<n>m`, or `<n>s`
        ///
        /// Ten years by default.
        #[arg(long, value_parser = parse_ttl)]
        ttl: Option<Duration>,
    },
    /// Issues a node certificate: `node.<id>.crt` and `node.<id>.key`
    ///
    /// The files go into the directory, and the certificate names the node's ID and the
    /// addresses it is reached at.
    Node {
        /// The node's ID, as `[node] id` in its configuration.
        id: NodeId,
        /// The IP addresses and DNS names peers and clients dial the node at
        ///
        /// Comma-separated. Not the `[node] host` placement label.
        #[arg(long, value_delimiter = ',', required = true)]
        addresses: Vec<String>,
        /// The directory with `ca.crt` and `ca.key`, which the new files go into.
        #[arg(long)]
        dir: PathBuf,
        /// How long the certificate lasts: `<n>d`, `<n>h`, `<n>m`, or `<n>s`
        ///
        /// 30 days by default.
        #[arg(long, value_parser = parse_ttl)]
        ttl: Option<Duration>,
    },
    /// Issues a client certificate, naming the principal the client acts as
    ///
    /// The files are `client.<principal>.crt` and `client.<principal>.key`, in the directory or
    /// in `--out`.
    Client {
        /// The principal's name
        ///
        /// A name that starts with `agent-` may not contain `.`, which is reserved for delegated
        /// agents.
        principal: Principal,
        /// The directory with `ca.crt` and `ca.key`
        ///
        /// The new files go into it unless `--out` names another.
        #[arg(long)]
        dir: PathBuf,
        /// Writes the files into this directory instead: a complete TLS directory
        ///
        /// The certificate, its key, and a copy of `ca.crt` go into it, created private to you,
        /// instead of the authority's directory.
        #[arg(long, value_name = "DIR")]
        out: Option<PathBuf>,
        /// How long the certificate lasts: `<n>d`, `<n>h`, `<n>m`, or `<n>s`
        ///
        /// 7 days by default.
        #[arg(long, value_parser = parse_ttl)]
        ttl: Option<Duration>,
    },
}

#[derive(Debug, Subcommand)]
enum DbCommand {
    /// Creates a SQLite database, places it with the ring, and opens it on its primary
    Create {
        /// The database's name.
        name: DatabaseName,
        /// How many replicas it has, the primary included
        ///
        /// The fleet's default if unset: the control node's `[replication] replication_factor`,
        /// which is 3 unless its configuration says otherwise, and 1 on a dev node.
        #[arg(long)]
        replication_factor: Option<u8>,
        /// How many replicas must store a write before it is acknowledged
        ///
        /// `quorum` (a majority), `all` (every replica), or `local` (the primary alone; a failover
        /// may lose acknowledged writes). The fleet's default if unset.
        #[arg(long)]
        acks: Option<Acks>,
        /// How many replicas must be reachable for a write to run
        ///
        /// The primary included; below it, writes are refused before anything commits. Between the
        /// write quorum and the replication factor; the fleet's default if unset, which is the
        /// write quorum unless the control node's configuration says otherwise.
        #[arg(long)]
        min_replicas: Option<u8>,
        #[command(flatten)]
        metadata: MetadataArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Shows a database in detail: placement, description, labels, expiry, schema
    ///
    /// Where it lives, as `db locate` prints it, its description, labels, creator, expiry, and
    /// metadata version, and its schema.
    ///
    /// A description is text a principal wrote: it prints as a block, each line indented and
    /// prefixed with `| `, and one written by another principal, or by nobody that can be
    /// named, is marked untrusted. The schema comes from the primary: each table with its row
    /// count, its indexes and triggers, then the views and virtual tables, each with the
    /// engine's own definition. Definitions are text the database's writers chose, printed the
    /// same way: read them as data.
    Show {
        /// The database's name.
        name: DatabaseName,
        /// Leaves the schema out and asks the primary for nothing
        #[arg(long)]
        no_schema: bool,
        /// Counts each table's rows up to this many; 0 counts nothing
        ///
        /// Beyond it, the count reports "more than". At most 1,000,000, which is also the most rows
        /// one request counts across all tables; counting delays writes queued on the database
        /// while it runs.
        #[arg(
            long,
            default_value_t = SCHEMA_COUNT_LIMIT,
            value_parser = clap::value_parser!(u64).range(0..=MAX_COUNT_LIMIT)
        )]
        count_limit: u64,
        /// Includes the engine's own tables, and the shadow tables of virtual tables.
        #[arg(long)]
        all: bool,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Changes a database's description, labels, expiry, or durability
    ///
    /// Only the settings you name change.
    ///
    /// The metadata settings (description, labels, expiry) go in one update. Durability is a
    /// separate change that takes effect at the database's next epoch: the current primary keeps
    /// the settings it holds the lease under. With both, durability is changed first and the
    /// metadata second, and each outcome is reported on its own line; if the metadata update is
    /// refused after the durability change went through, the command says so and exits non-zero.
    /// `--if-version` guards the metadata update only.
    Edit(edit::EditArgs),
    /// Runs one write statement on the database's primary.
    Exec(SqlArgs),
    /// Runs one read-only query on the database's primary.
    Query(QueryArgs),
    /// Follows a database's row changes, one transaction per line, until Ctrl-C
    ///
    /// Streams each committed transaction's inserted, updated and deleted rows from the
    /// database's primary, as the write quorum acknowledges them (ADR-0045), from now, or after
    /// the position `--from` names. Needs the `stream` scope on the database, which `read`,
    /// `write` and `admin` don't grant, and a database that records its changes: one a logical
    /// change sink names, as dev mode names every database.
    #[command(after_long_help = watch_cmd::WATCH_HELP)]
    Watch(watch_cmd::WatchArgs),
    /// Shows where a database lives: its primary, followers, and epoch
    ///
    /// With its durability settings and its replicas' positions.
    Locate {
        /// The database's name.
        name: DatabaseName,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Lists the databases, with their placement and durability
    ///
    /// Each with its engine, epoch, replication factor, durability settings, primary, and
    /// replicas.
    List {
        /// Lists only the databases this node is primary for.
        #[arg(long)]
        node: Option<NodeId>,
        /// Lists only the databases with this label: `key=value`, or `key`
        ///
        /// `key` alone matches any value. Repeat it to require several.
        #[arg(long = "label", value_name = "KEY=VALUE|KEY")]
        labels: Vec<LabelRequirement>,
        /// Lists only the databases whose names start with this.
        #[arg(long)]
        prefix: Option<String>,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Checks that every replica of a database holds the same log
    ///
    /// Asks every replica for its hash chain at one position, the latest they all hold unless
    /// `--at` names one, and reports whether they agree. Each replica recomputes the hash from
    /// its log on disk, so a copy that changed since it was written is named.
    Verify {
        /// The database's name.
        name: DatabaseName,
        /// The position to verify at, as `db locate` prints it
        ///
        /// Instead of the latest position every replica holds.
        #[arg(long, value_parser = parse_position)]
        at: Option<Position>,
        /// A hash recorded earlier at that position, to check that nothing changed since
        ///
        /// The command also reports whether the log still hashes to it.
        #[arg(long)]
        expect: Option<ChainHash>,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Creates a new database from an archived one
    ///
    /// The first live member of its replica set restores it, verifying every batch against the
    /// hash chain, and serves it. The archived database needs not exist any more; restoring under
    /// its own name needs it dropped first.
    Restore {
        /// The new database's name.
        name: DatabaseName,
        /// The `[[archive]]` entry's name.
        #[arg(long)]
        archive: String,
        /// The archived database to restore.
        #[arg(long)]
        from: DatabaseName,
        #[command(flatten)]
        point: PointArgs,
        /// How many replicas the new database has, the primary included
        ///
        /// The fleet's default if unset, as for `db create`.
        #[arg(long)]
        replication_factor: Option<u8>,
        /// The acknowledgement mode, as for `db create`; the fleet's default if unset.
        #[arg(long)]
        acks: Option<Acks>,
        #[command(flatten)]
        metadata: MetadataArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Drops a database for good
    ///
    /// Every replica that is up deletes its copy; one that is down or unreachable keeps it as an
    /// orphan for `orphan remove`.
    Drop {
        /// The database's name.
        name: DatabaseName,
        /// Drops without asking
        ///
        /// Without it, the command asks first on a terminal, and refuses elsewhere.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        target: Target,
    },
}

#[derive(Debug, Subcommand)]
enum NodeCommand {
    /// Lists the members, with their liveness, drain state and release
    ///
    /// As the control plane sees them.
    ///
    /// Every member is a data node: membership records no role, so there is no role column.
    List {
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Hands the node's primaries to followers, and gives it no new ones
    ///
    /// Every database the node is primary for moves to a follower, without waiting for a
    /// failover. The node stays a follower until it stops or restarts, or `node undrain`;
    /// stopping it then costs clients only a redirect.
    Drain {
        /// The node's ID.
        id: NodeId,
        /// Drains without asking
        ///
        /// Without it, a drain of a node that holds a database's only replica asks first on a
        /// terminal, and refuses elsewhere.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        target: Target,
    },
    /// Clears a node's draining mark, so that it may take primaries again
    ///
    /// Without a restart, as after a drain that turned out not to be needed. Refused until every
    /// member runs a release that supports it.
    Undrain {
        /// The node's ID.
        id: NodeId,
        #[command(flatten)]
        target: Target,
    },
    /// Removes a member that is gone for good from the fleet
    ///
    /// As a node that leaves on shutdown does, so that its release no longer holds the fleet
    /// back. Refuses a live member unless `--force`; a live node joins again at its next heartbeat
    /// anyway. Asks first on a terminal.
    Remove {
        /// The node's ID.
        id: NodeId,
        /// Removes the member even though it is live.
        #[arg(long)]
        force: bool,
        /// Removes without asking
        ///
        /// Without it, the command asks first on a terminal, and refuses elsewhere.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        target: Target,
    },
}

/// What `db create` and `db restore` record about the new database besides its placement.
#[derive(Debug, Args)]
struct MetadataArgs {
    /// What the database holds and how it was set up; `-` reads it from stdin
    ///
    /// At most 1 KiB of text; tab and line feed are allowed, other control and invisible
    /// characters aren't. From stdin, its trailing line feeds are dropped. The control node
    /// records you as the creator.
    #[arg(long, allow_hyphen_values = true)]
    description: Option<String>,
    /// A label, `key=value`, to find the database by (`db list --label`)
    ///
    /// Repeat it for several, at most 32.
    #[arg(long = "label", value_name = "KEY=VALUE", value_parser = parse_label_arg)]
    labels: Vec<(LabelKey, LabelValue)>,
    #[command(flatten)]
    expiry: ExpiryArgs,
}

impl MetadataArgs {
    /// The metadata the flags ask for, reading the description from `stdin` if it is `-`.
    fn read(self, stdin: impl Read) -> Result<NewMetadata, Box<dyn Error>> {
        let description = self
            .description
            .map(|text| description_arg(&text, stdin))
            .transpose()?;
        let labels = Labels::try_from_pairs(self.labels)?;
        Ok(NewMetadata {
            description,
            labels,
            expiry: self.expiry.expiry(),
        })
    }
}

/// When a new database is dropped, if it is: `--expires-in` or `--expires-at`.
#[derive(Debug, Args)]
struct ExpiryArgs {
    /// Drops the database this long from now: `<n>d`, `<n>h`, `<n>m`, or `<n>s`
    ///
    /// At least 1m. The control node counts it from its own clock. Every copy is deleted; an
    /// archive keeps the database as of what reached it, and without one it is gone. Extend it
    /// with `db edit`.
    #[arg(long, value_name = "DURATION", value_parser = parse_ttl, conflicts_with = "expires_at")]
    expires_in: Option<Duration>,
    /// Drops the database at this time, in RFC 3339
    ///
    /// Such as `2026-11-05T10:00:00Z`: at least a minute from now by the control node's clock.
    #[arg(long, value_name = "TIME", value_parser = parse_rfc3339)]
    expires_at: Option<Timestamp>,
}

impl ExpiryArgs {
    /// The expiry the flags ask for, if any.
    fn expiry(&self) -> Option<Expiry> {
        match (self.expires_in, self.expires_at) {
            (Some(duration), _) => Some(Expiry::In(duration)),
            (None, Some(at)) => Some(Expiry::At(at)),
            (None, None) => None,
        }
    }
}

#[derive(Debug, Args)]
struct SqlArgs {
    /// The database's name.
    name: DatabaseName,
    /// One SQL statement, with `?` for each parameter.
    sql: String,
    /// The parameters, in order: `null`, an integer, a decimal number, or text
    ///
    /// Prefix one with `text:`, `int:`, `real:`, or `blob:` (hex digits) to choose its type.
    #[arg(allow_negative_numbers = true, value_parser = parse_param)]
    params: Vec<Value>,
    /// Sends the request to this data node instead of the primary
    ///
    /// The command follows the node's redirect. Its address comes from the control plane's
    /// membership.
    #[arg(long, conflicts_with = "address")]
    node: Option<NodeId>,
    /// Sends the request to the data node at `host:port` instead of the primary
    ///
    /// The command follows the node's redirect.
    #[arg(long)]
    address: Option<String>,
    #[command(flatten)]
    target: Target,
}

/// `db query`'s arguments: the statement, and how to print its rows.
#[derive(Debug, Args)]
struct QueryArgs {
    #[command(flatten)]
    sql: SqlArgs,
    #[command(flatten)]
    output: OutputArgs,
}

/// How a command finds the control plane, and who it acts as. Commands talk to the control node,
/// which knows every database's placement; data nodes redirect database calls to the right
/// primary on their own.
///
/// Each setting is resolved on its own, and the first of these wins: the setting's flag, the
/// context `--context` names, the setting's environment variable, the context
/// `FLEETRING_CONTEXT` names or else the current context, and the built-in default
/// (`docs/cli.md`, section 3).
#[derive(Debug, Args)]
struct Target {
    /// The control node's address, `host:port`
    ///
    /// The node that runs the control role, not any node in the fleet; a data-only node answers
    /// with the control node's address. Defaults to `FLEETRING_CONTROL`, or else the context's
    /// `control`, when neither this nor `--config` is given.
    #[arg(long, conflicts_with = "config")]
    control: Option<String>,
    /// A daemon's configuration file, to take the control node's address from
    #[arg(long)]
    config: Option<PathBuf>,
    /// Uses this context for every setting it names, instead of the current one
    ///
    /// It outranks the environment variables; `FLEETRING_CONTEXT`, and else the current context,
    /// rank below them.
    #[arg(
        long,
        value_name = "NAME",
        add = ArgValueCandidates::new(completions::context_names)
    )]
    context: Option<ContextName>,
    #[command(flatten)]
    tls: TlsArgs,
    /// The settings, resolved the first time a command needs them.
    #[arg(skip)]
    resolved: OnceLock<Box<Resolved>>,
}

/// The identity a command connects with: a client certificate from `cert client`.
///
/// The identity resolves as a unit: the highest step of the order (flag, `--context`, variable,
/// current context) that names a TLS directory, an authority, a certificate or a key supplies
/// all of it, and the steps below supply none. Within it, a file's own path wins over the
/// principal's file under the TLS directory, which wins over the directory's default; a step
/// above it that names only `--as` picks among its files.
#[derive(Debug, Args)]
struct TlsArgs {
    /// A directory with `ca.crt` and the client certificates `cert client` writes
    ///
    /// `client.crt` and `client.key`, and the `client.<principal>.crt` and `.key` files `--as`
    /// picks from. Defaults to `FLEETRING_TLS_DIR`, or else the context's `tls_dir`.
    #[arg(long, value_name = "DIR")]
    tls_dir: Option<PathBuf>,
    /// The trusted authority certificates, PEM
    ///
    /// Defaults to `FLEETRING_CA`, the context's `ca`, or `ca.crt` in the TLS directory.
    #[arg(long)]
    ca: Option<PathBuf>,
    /// The client certificate, PEM; overrides `--as`
    ///
    /// Defaults to `FLEETRING_CERT`, the context's `cert`, or `client.crt` in the TLS directory.
    #[arg(long)]
    cert: Option<PathBuf>,
    /// The client certificate's private key, PEM; overrides `--as`
    ///
    /// Defaults to `FLEETRING_KEY`, the context's `key`, or `client.key` in the TLS directory.
    #[arg(long)]
    key: Option<PathBuf>,
    /// Acts as this principal, with its certificate in the TLS directory
    ///
    /// Uses `client.<principal>.crt` and `client.<principal>.key` under the TLS directory, as
    /// `cert client <principal>` writes them. An explicit certificate or key at the same step
    /// wins over this for that file, and this wins over `client.crt` and `client.key`. Defaults
    /// to `FLEETRING_AS`, or else the context's `as`.
    #[arg(long = "as", value_name = "PRINCIPAL")]
    principal: Option<Principal>,
    /// Connects in plaintext, to a daemon started with `--insecure-plaintext`
    #[arg(
        long,
        conflicts_with_all = ["ca", "cert", "key", "principal", "tls_dir"]
    )]
    insecure_plaintext: bool,
    /// A bearer token from `token issue`, sent with every request
    ///
    /// The request then acts with the token's scopes. Defaults to `FLEETRING_TOKEN`, if set.
    #[arg(long, conflicts_with = "token_file")]
    token: Option<String>,
    /// A file that holds a bearer token, or `-` for stdin
    ///
    /// Defaults to `FLEETRING_TOKEN_FILE`, or else the context's `token_file`.
    #[arg(long, value_name = "PATH")]
    token_file: Option<PathBuf>,
    /// Acts as the agent with this ID, with the identity minted for it
    ///
    /// The identity `fleetring agent add` or `fleetring mcp --agent` minted: `agent-<id>`, its
    /// certificate, and its token, from its directory under the context's agents directory.
    /// Defaults to `FLEETRING_AGENT`.
    #[arg(
        long,
        value_name = "ID",
        conflicts_with_all = [
            "principal", "cert", "key", "tls_dir", "token", "token_file", "insecure_plaintext"
        ],
        add = ArgValueCandidates::new(completions::agent_ids)
    )]
    agent: Option<AgentId>,
}

/// Where the agent a command acts as was named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentFrom {
    /// `--agent`.
    Flag,
    /// `FLEETRING_AGENT`.
    Variable,
}

/// What a command with no control address anywhere says.
const NO_CONTROL: &str = "no control address\n  hint: run `fleetring dev -d` for a local node, or \
                          `fleetring context add <name> --control <host:port> …`";

/// How long to wait for one control address before trying the next.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Set once at the start of `main`, from `-q`.
static QUIET: OnceLock<Quiet> = OnceLock::new();

/// Set once at the start of `main`, from `--color`.
static COLOR: OnceLock<ColorChoice> = OnceLock::new();

/// What `--color` asked for.
fn color() -> ColorChoice {
    COLOR.get().copied().unwrap_or(ColorChoice::Auto)
}

/// Whether `-q` was given.
fn quiet() -> Quiet {
    QUIET.get().copied().unwrap_or(Quiet::No)
}

/// Prints `line` on stderr unless `-q` was given: a warning, a note, anything but the result and
/// an error (`docs/cli.md`, section 5).
fn note(line: impl std::fmt::Display) {
    if quiet() == Quiet::No {
        eprintln!("{line}");
    }
}

/// What a command warns when a token arrives on the command line (`docs/cli.md`, section 11).
const TOKEN_ON_ARGV: &str = "warning: --token on the command line is visible to other processes; \
                             use FLEETRING_TOKEN or --token-file";

/// The first of `addresses` that accepts a connection, or the first if none does, so the usual
/// connection error names it. A single address is returned as it is, without a probe.
fn first_reachable(addresses: &[String]) -> String {
    use std::net::{TcpStream, ToSocketAddrs as _};
    let Some(first) = addresses.first() else {
        return String::new();
    };
    if addresses.len() == 1 {
        return first.clone();
    }
    let reachable = |address: &&String| {
        address.to_socket_addrs().is_ok_and(|mut found| {
            found.any(|to| TcpStream::connect_timeout(&to, PROBE_TIMEOUT).is_ok())
        })
    };
    addresses.iter().find(reachable).unwrap_or(first).clone()
}

impl Target {
    /// What the command line gave, as the precedence sees it.
    fn flags(&self) -> Result<TargetFlags, Box<dyn Error>> {
        let config = match &self.config {
            Some(path) => Some(
                Config::load(path)?
                    .control_target()
                    .ok_or("the configuration names no control address; pass --control")?
                    .to_string(),
            ),
            None => None,
        };
        let token = match (&self.tls.token, &self.tls.token_file) {
            (Some(token), _) => Some(TokenSource::Value(token.clone())),
            (None, Some(path)) if path == Path::new("-") => Some(TokenSource::Stdin),
            (None, Some(path)) => Some(TokenSource::File(path.clone())),
            (None, None) => None,
        };
        Ok(TargetFlags {
            control: self.control.clone(),
            config,
            tls_dir: self.tls.tls_dir.clone(),
            ca: self.tls.ca.clone(),
            cert: self.tls.cert.clone(),
            key: self.tls.key.clone(),
            principal: self.tls.principal.as_ref().map(ToString::to_string),
            token,
            wire: if self.tls.insecure_plaintext {
                Wire::Plaintext
            } else {
                Wire::Tls
            },
            agent: None,
        })
    }

    /// Selects the context and resolves the settings, once; prints the override warnings
    /// unless `-q` was given, terminal or not.
    fn resolved(&self) -> Result<&Resolved, Box<dyn Error>> {
        if let Some(resolved) = self.resolved.get() {
            return Ok(resolved);
        }
        let mut env = EnvSnapshot::capture();
        let mut flags = self.flags()?;
        let contexts = self.contexts(&env)?;
        let selected = select(&contexts, self.context.as_ref(), env.context.as_deref())?;
        if let Some((id, from)) = self.agent(&env)? {
            let agents_dir = agent::agents_dir(
                selected.map(|(name, context, _)| (name, context)),
                &|name| std::env::var_os(name),
            )?;
            let dir = AgentDir::new(&agents_dir, id);
            if dir.record()?.is_none() {
                return Err(agent_cmd::no_identity(
                    &dir,
                    selected.map(|(name, context, _)| (name, context)),
                )
                .into());
            }
            for warning in dir.loose_files() {
                note(warning);
            }
            let principal = Some(dir.id().principal().to_string());
            let token = Some(TokenSource::File(dir.token_file()));
            let label = Some(dir.id().to_string());
            match from {
                AgentFrom::Flag => {
                    flags.tls_dir = Some(dir.tls_dir().to_owned());
                    flags.principal = principal;
                    flags.token = token;
                    flags.agent = label;
                    env.agent = None;
                }
                AgentFrom::Variable => {
                    let clash = [
                        (env.tls_dir.is_some(), resolve::TLS_DIR_VAR),
                        (env.ca.is_some(), resolve::CA_VAR),
                        (env.cert.is_some(), resolve::CERT_VAR),
                        (env.key.is_some(), resolve::KEY_VAR),
                        (env.principal.is_some(), resolve::AS_VAR),
                        (env.token.is_some(), resolve::TOKEN_VAR),
                        (env.token_file.is_some(), resolve::TOKEN_FILE_VAR),
                    ]
                    .into_iter()
                    .find_map(|(set, name)| set.then_some(name));
                    if let Some(name) = clash {
                        return Err(format!(
                            "{} and {name} both name an identity; unset one",
                            resolve::AGENT_VAR
                        )
                        .into());
                    }
                    env.tls_dir = Some(dir.tls_dir().to_owned());
                    env.principal = principal;
                    env.token_file = Some(dir.token_file());
                }
            }
        }
        let mut resolved = resolve::resolve(&flags, &env, selected);
        for warning in &resolved.warnings {
            note(warning);
        }
        if self.tls.token.is_some() {
            note(TOKEN_ON_ARGV);
        }
        // Stdin can be read once, and a command may ask for the token more than once.
        if resolved.token == Some(TokenSource::Stdin) {
            resolved.token = Some(TokenSource::Value(TokenSource::Stdin.read()?));
        }
        Ok(self.resolved.get_or_init(|| Box::new(resolved)))
    }

    /// The contexts file, unless `FLEETRING_CONTEXT=` skips it and `--context` doesn't need it,
    /// or there is no home directory and no context was asked for.
    fn contexts(&self, env: &EnvSnapshot) -> Result<Contexts, Box<dyn Error>> {
        let named = self.context.is_some() || env.context.is_some();
        Ok(match context_cmd::store() {
            Ok(_) if env.contexts == ContextsFile::Skip && self.context.is_none() => {
                Contexts::empty()
            }
            Ok(store) => store.load().map_err(|source| ContextsUnusable {
                path: store.path().to_owned(),
                source,
            })?,
            Err(_) if !named => Contexts::empty(),
            Err(error) => return Err(error.into()),
        })
    }

    /// The agent `--agent` or else `FLEETRING_AGENT` names, and which.
    fn agent(&self, env: &EnvSnapshot) -> Result<Option<(AgentId, AgentFrom)>, Box<dyn Error>> {
        if let Some(id) = &self.tls.agent {
            return Ok(Some((id.clone(), AgentFrom::Flag)));
        }
        Ok(match &env.agent {
            Some(text) => Some((text.parse()?, AgentFrom::Variable)),
            None => None,
        })
    }

    /// Whether the command line or the environment names any part of a connection: a control
    /// address or any part of an identity. Without a context, none means dev mode (ADR-0043,
    /// point 8.1).
    fn names_a_connection(&self, env: &EnvSnapshot) -> bool {
        let tls = &self.tls;
        self.control.is_some()
            || self.config.is_some()
            || tls.tls_dir.is_some()
            || tls.ca.is_some()
            || tls.cert.is_some()
            || tls.key.is_some()
            || tls.principal.is_some()
            || tls.insecure_plaintext
            || tls.token.is_some()
            || tls.token_file.is_some()
            || env.control.is_some()
            || env.tls_dir.is_some()
            || env.ca.is_some()
            || env.cert.is_some()
            || env.key.is_some()
            || env.principal.is_some()
            || env.token.is_some()
            || env.token_file.is_some()
    }

    /// A client for the control address the settings resolve to, or for `fallback` when they
    /// resolve to none.
    fn client_or(&self, fallback: Option<String>) -> Result<Client, Box<dyn Error>> {
        let resolved = self.resolved()?;
        let control = if resolved.control.is_empty() {
            fallback.ok_or(NO_CONTROL)?
        } else {
            first_reachable(&resolved.control)
        };
        let security = self.security()?;
        Ok(Client::new(control, self.network_with(security)?))
    }

    /// The identity the command connects with, loaded and checked: its certificate must name
    /// the fleet its authority names, and the fleet the context names if the context supplied
    /// it.
    fn security(&self) -> Result<Security, Box<dyn Error>> {
        let resolved = self.resolved()?;
        let files = match &resolved.security {
            SecurityFiles::Plaintext => return Ok(Security::Plaintext),
            SecurityFiles::Unresolved(why) => return Err(why.clone().into()),
            SecurityFiles::Tls(files) => files,
        };
        let identity = Identity::load(&files.ca, &files.cert, &files.key)?;
        let found = identity.fleet();
        let authorities = identity.authority_fleets();
        if !authorities.is_empty() && !authorities.contains(&found) {
            let names: Vec<String> = authorities.iter().map(ToString::to_string).collect();
            return Err(format!(
                "the certificate {} is for fleet '{found}', but the authority {} is for fleet '{}'",
                files.cert.display(),
                files.ca.display(),
                names.join("', '")
            )
            .into());
        }
        if let (Some(fleet), Some(context)) = (&resolved.fleet, &resolved.context)
            && found.as_str() != fleet
        {
            return Err(format!(
                "context '{context}' is for fleet '{fleet}', but its certificate is for fleet \
                 '{found}'"
            )
            .into());
        }
        Ok(Security::Tls(identity))
    }

    /// The bearer token the command sends, if any.
    fn token(&self) -> Result<Option<String>, Box<dyn Error>> {
        Ok(self
            .resolved()?
            .token
            .as_ref()
            .map(TokenSource::read)
            .transpose()?)
    }

    fn network_with(&self, security: Security) -> Result<Network, Box<dyn Error>> {
        let network = Network::default().with_security(security);
        Ok(match self.token()? {
            Some(token) => {
                network.with_credentials(Credentials::bearer(&BearerToken::new(token.trim()))?)
            }
            None => network,
        })
    }

    fn network(&self) -> Result<Network, Box<dyn Error>> {
        self.network_with(self.security()?)
    }

    /// The principal the command acts as, so that what others wrote can be told apart from
    /// what it wrote: the one its token names, if it sends one, else its certificate's. Read
    /// without verifying; the nodes verify. `None` in plaintext without a token, or when
    /// neither can be read.
    fn principal(&self) -> Option<String> {
        if let Some(token) = self.token().ok()? {
            return BearerToken::new(token.trim())
                .claimed_principal()
                .map(|principal| principal.to_string());
        }
        match self.security().ok()? {
            Security::Tls(identity) => match identity.peer() {
                Peer::Principal(name) => Some(name.to_string()),
                _ => None,
            },
            _ => None,
        }
    }

    /// Where the command acts, for a confirmation: the fleet and the context, or the fleet and
    /// the control address when no context is in use (ADR-0043, point 4.6).
    fn place(&self) -> Result<Place, Box<dyn Error>> {
        let resolved = self.resolved()?;
        let fleet = match (&resolved.fleet, self.security()) {
            (Some(fleet), _) => Some(fleet.clone()),
            (None, Ok(Security::Tls(identity))) => Some(identity.fleet().to_string()),
            _ => None,
        };
        let via = match &resolved.context {
            Some(context) => format!("context '{context}'"),
            None => resolved.control.first().map_or_else(
                || "no control address".to_owned(),
                |control| format!("control {control}"),
            ),
        };
        Ok(Place { fleet, via })
    }

    fn client(&self) -> Result<Client, Box<dyn Error>> {
        let resolved = self.resolved()?;
        if resolved.control.is_empty() {
            return Err(NO_CONTROL.into());
        }
        let security = self.security()?;
        Ok(Client::new(
            first_reachable(&resolved.control),
            self.network_with(security)?,
        ))
    }
}

impl DaemonArgs {
    /// The check's output format, from `-o` and `--json`.
    fn check_format(&self) -> OutputFormat {
        if self.json {
            OutputFormat::Json
        } else {
            self.output.unwrap_or(OutputFormat::Table)
        }
    }

    fn apply(self, mut config: Config) -> Config {
        if let Some(id) = self.node_id {
            config.node.id = id;
        }
        if let Some(data_dir) = self.data_dir {
            config.data_dir = data_dir;
        }
        if let Some(listen) = self.listen {
            config.listen = listen;
        }
        if self.advertise.is_some() {
            config.node.advertise = self.advertise;
        }
        if self.control.is_some() {
            config.control.address = self.control;
        }
        if let Some(role) = self.role {
            config.role = role;
        }
        if self.insecure_plaintext {
            config.insecure_plaintext = Plaintext::Allowed;
        }
        config
    }
}

/// A label as `key=value`.
fn parse_label_arg(text: &str) -> Result<(LabelKey, LabelValue), String> {
    parse_label(text).map_err(|error| error.to_string())
}

/// A description from a flag: the text itself, or, for `-`, everything `stdin` holds, without
/// its trailing line feeds.
fn description_arg(text: &str, mut stdin: impl Read) -> Result<Description, Box<dyn Error>> {
    if text != "-" {
        return Ok(text.parse()?);
    }
    let mut read = String::new();
    stdin
        .read_to_string(&mut read)
        .map_err(|error| format!("can't read the description from stdin: {error}"))?;
    Ok(read.trim_end_matches('\n').parse()?)
}

/// A lifetime as `<n>d`, `<n>h`, `<n>m`, or `<n>s`.
fn parse_ttl(text: &str) -> Result<Duration, String> {
    let (digits, unit) = text.split_at(text.len().saturating_sub(1));
    let count: u64 = digits
        .parse()
        .map_err(|_| format!("'{text}' isn't a lifetime: expected <n>d, <n>h, <n>m, or <n>s"))?;
    let seconds = match unit {
        "d" => count.checked_mul(24 * 60 * 60),
        "h" => count.checked_mul(60 * 60),
        "m" => count.checked_mul(60),
        "s" => Some(count),
        _ => {
            return Err(format!(
                "'{text}' isn't a lifetime: expected <n>d, <n>h, <n>m, or <n>s"
            ));
        }
    }
    .ok_or_else(|| format!("'{text}' is too long a lifetime"))?;
    if seconds == 0 {
        return Err("a lifetime must be at least 1s".to_owned());
    }
    Ok(Duration::from_secs(seconds))
}

/// A size in bytes, with an optional `KiB`, `MiB`, or `GiB`.
fn parse_size(text: &str) -> Result<usize, String> {
    let invalid = || format!("'{text}' isn't a size: expected bytes, or <n>KiB, <n>MiB, or <n>GiB");
    let (digits, unit) = match text.find(|character: char| !character.is_ascii_digit()) {
        Some(at) => text.split_at(at),
        None => (text, ""),
    };
    let count: usize = digits.parse().map_err(|_| invalid())?;
    let unit: usize = match unit {
        "" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        _ => return Err(invalid()),
    };
    count
        .checked_mul(unit)
        .ok_or_else(|| format!("'{text}' is too large a size"))
}

/// `--max-result-bytes`: a size from 64 KiB to 4 MiB.
fn parse_result_size(text: &str) -> Result<usize, String> {
    let size = parse_size(text)?;
    if !(mcp::MIN_MAX_RESULT_BYTES..=mcp::MAX_MAX_RESULT_BYTES).contains(&size) {
        return Err(format!(
            "'{text}' is outside 64KiB to 4MiB: a result must hold a database's metadata, and \
             a client takes a few MiB at most"
        ));
    }
    Ok(size)
}

fn parse_role(text: &str) -> Result<NodeRole, String> {
    match text {
        "both" => Ok(NodeRole::Both),
        "data" => Ok(NodeRole::Data),
        "control" => Ok(NodeRole::Control),
        other => Err(format!(
            "unknown role '{other}': expected both, data, or control"
        )),
    }
}

fn parse_param(text: &str) -> Result<Value, String> {
    if let Some(rest) = text.strip_prefix("text:") {
        return Ok(Value::Text(rest.to_owned()));
    }
    if let Some(rest) = text.strip_prefix("int:") {
        return rest
            .parse()
            .map(Value::Integer)
            .map_err(|error| format!("'{rest}' isn't an integer: {error}"));
    }
    if let Some(rest) = text.strip_prefix("real:") {
        return rest
            .parse()
            .map(Value::Real)
            .map_err(|error| format!("'{rest}' isn't a number: {error}"));
    }
    if let Some(rest) = text.strip_prefix("blob:") {
        return hex(rest).map(Value::Blob);
    }
    if text.eq_ignore_ascii_case("null") {
        return Ok(Value::Null);
    }
    if let Ok(integer) = text.parse() {
        return Ok(Value::Integer(integer));
    }
    let numeric = text.bytes().any(|byte| byte.is_ascii_digit())
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || b"+-.eE".contains(&byte));
    match text.parse::<f64>() {
        Ok(real) if numeric => Ok(Value::Real(real)),
        _ => Ok(Value::Text(text.to_owned())),
    }
}

/// A time in RFC 3339, as a timestamp.
fn parse_rfc3339(text: &str) -> Result<Timestamp, String> {
    parse_time(text).map(Timestamp::from_millis)
}

/// A time in RFC 3339, as milliseconds since the Unix epoch.
fn parse_time(text: &str) -> Result<u64, String> {
    let time = time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
        .map_err(|error| {
            format!("'{text}' isn't an RFC 3339 time, such as 2026-11-03T09:30:00Z: {error}")
        })?;
    u64::try_from(time.unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| format!("'{text}' is before 1970"))
}

/// Milliseconds since the Unix epoch in RFC 3339.
fn format_time(ms: u64) -> String {
    i128::from(ms)
        .checked_mul(1_000_000)
        .and_then(|nanos| time::OffsetDateTime::from_unix_timestamp_nanos(nanos).ok())
        .and_then(|time| {
            time.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_else(|| format!("{ms} ms"))
}

fn parse_position(text: &str) -> Result<Position, String> {
    if text.is_empty() {
        return Err("a position is hex digits".to_owned());
    }
    hex(text).map(Position::from)
}

fn hex(text: &str) -> Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) {
        return Err(format!("'{text}' has an odd number of hex digits"));
    }
    (0..text.len())
        .step_by(2)
        .map(|at| {
            text.get(at..at + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(|| format!("'{text}' isn't hex"))
        })
        .collect()
}

/// What the process runs: a command, or the daemon.
#[derive(Debug)]
enum Run {
    /// A command that talks to a fleet, or works offline.
    Command(Box<Command>),
    /// `dev -d`, `dev stop`, `dev status` or `dev logs`, which choose their own exit code.
    Dev(Box<DevArgs>),
    /// The daemon, from a configuration file with `serve`'s flags applied, or from dev mode's
    /// directory.
    Daemon {
        config: Box<Config>,
        /// Set in dev mode: the prepared directory, for the banner.
        dev: Option<Box<Prepared>>,
    },
}

/// Parses `raw`, the process's arguments with the program first, as the binary named `name`:
/// that name prefixes `--version` and usage lines, and 0.6's bare daemon flags mean `serve`
/// (ADR-0043, point 1).
///
/// # Errors
///
/// clap's error, which prints help, the version, or a usage error.
fn parse(raw: Vec<OsString>, name: ProgramName) -> Result<(Cli, Invocation), clap::Error> {
    // 0.6 never had a `fleet`, so the bare daemon flags are for the other names alone.
    let (args, invocation) = match name {
        ProgramName::Fleet => (raw, Invocation::AsWritten),
        ProgramName::Fleetring | ProgramName::Fleetringd => compat::imply_serve(raw),
    };
    let mut command = help::root_command(name);
    let mut matches = command.try_get_matches_from_mut(args)?;
    let cli =
        Cli::from_arg_matches_mut(&mut matches).map_err(|error| error.format(&mut command))?;
    Ok((cli, invocation))
}

fn main() -> ExitCode {
    // A node `dev -d` started leaves the terminal's session first, so that closing the terminal
    // sends it no SIGHUP, which would restart it in place (ADR-0043, point 5.2).
    #[cfg(unix)]
    if dev::started_detached() {
        leave_the_session();
    }
    let raw: Vec<OsString> = std::env::args_os().collect();
    let name = ProgramName::from_arg0(raw.first().map(OsString::as_os_str));
    PROGRAM.get_or_init(|| name);
    // A shell's completion script runs the binary with `FLEETRING_COMPLETE` set: answer, and exit.
    completions::complete_if_asked(name);
    let arg0 = raw.first().cloned();
    let (cli, invocation) = match parse(raw, name) {
        Ok(parsed) => parsed,
        Err(error) => {
            let hint = edit::removed_verb_hint(&error)
                .or_else(|| edit::renamed_hint(&error))
                .or_else(|| stop::target_hint(&error));
            let code = error.exit_code();
            // Help and version requests are not errors; clap prints them to stdout.
            let _ = error.print();
            if let Some(hint) = hint {
                eprintln!("{}", hinted(hint));
            }
            return ExitCode::from(u8::try_from(code).unwrap_or(exit::USAGE));
        }
    };
    let Cli {
        command,
        log_format: format_flag,
        quiet,
        color,
    } = cli;
    let _ = QUIET.set(if quiet { Quiet::Yes } else { Quiet::No });
    let _ = COLOR.set(color);
    // Contexts are files: no logging, no runtime, no network.
    let command = match command {
        Command::Context(command) => return finish(context_cmd::run(command)),
        Command::Switch { target } => return finish(context_cmd::switch(target.as_deref())),
        Command::Completions(args) => return finish(completions::print(&args, name, arg0)),
        // A check starts nothing, so it needs no logging and no runtime either.
        Command::Serve(args) if args.check => {
            let format = args.check_format();
            return finish(serve_check::run(args, format));
        }
        command => command,
    };
    // The daemon reads its configuration before logging starts, since it may name the format;
    // dev mode prepares its directory first, since the configuration is in it.
    let run = match command {
        Command::Serve(args) => match Config::load(&args.config) {
            Ok(config) => Run::Daemon {
                config: Box::new(args.apply(config)),
                dev: None,
            },
            Err(error) => {
                report(&error);
                return ExitCode::FAILURE;
            }
        },
        Command::Dev(args) if args.runs_in_foreground() => match prepare_dev(&args) {
            Ok(prepared) => Run::Daemon {
                config: Box::new(prepared.config().clone()),
                dev: Some(Box::new(prepared)),
            },
            Err(error) => {
                report(&error);
                return ExitCode::FAILURE;
            }
        },
        Command::Dev(args) => Run::Dev(Box::new(args)),
        command => Run::Command(Box::new(command)),
    };
    let config = match &run {
        Run::Daemon { config, .. } => Some(config.as_ref()),
        Run::Command(_) | Run::Dev(_) => None,
    };
    let variable = std::env::var(LOG_FORMAT_VAR).ok();
    let format = match log_format(format_flag, variable.as_deref(), config) {
        Ok(format) => format,
        Err(value) => {
            eprintln!(
                "{}: invalid {LOG_FORMAT_VAR} '{value}'; use text or json",
                program()
            );
            return ExitCode::from(exit::USAGE);
        }
    };
    // The daemon logs at info; a command prints its result and logs only warnings.
    let default_filter = match run {
        Run::Command(_) | Run::Dev(_) => "warn",
        Run::Daemon { .. } => "info",
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| default_filter.into());
    // A detached node's log, a journal, or a pipe gets no escape sequences unless `--color
    // always` asks for them.
    let colour = if Surroundings::capture(MachineOutput::No, Quiet::No).colour(color) {
        LogColour::Ansi
    } else {
        LogColour::Plain
    };
    if let Err(error) = log_subscriber(format, colour, filter, SuspendingStderr).try_init() {
        report(&error);
        return ExitCode::FAILURE;
    }
    if invocation == Invocation::ServeImplied {
        tracing::warn!(
            "starting without `serve` is kept until 1.0; run `fleetring serve --config <file>`"
        );
    }
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            report(&error);
            return ExitCode::FAILURE;
        }
    };
    let result = match run {
        Run::Command(command) => runtime
            .block_on(run_command(*command))
            .map(|()| Stopped::Exit),
        Run::Dev(args) => {
            let code = finish(runtime.block_on(dev_cmd::run(*args)));
            runtime.shutdown_timeout(RUNTIME_GRACE);
            return code;
        }
        Run::Daemon { config, dev } => {
            runtime.block_on(run_daemon(*config, dev.map(|prepared| *prepared)))
        }
    };
    // Every task, connection, and database is closed by now; the runtime's threads go too,
    // before a restart executes the binary again.
    runtime.shutdown_timeout(RUNTIME_GRACE);
    match result {
        Ok(Stopped::Restart { exe, restarts }) => restart_in_place(&exe, restarts),
        Ok(Stopped::Exit) => ExitCode::SUCCESS,
        Err(error) => failure(error.as_ref()),
    }
}

/// The exit code of a command that ran without a runtime, reporting its error.
fn finish(result: Result<ExitCode, Box<dyn Error>>) -> ExitCode {
    result.unwrap_or_else(|error| failure(error.as_ref()))
}

/// Reports `error`, unless the command has said all there is to say, and returns its exit code
/// (`docs/cli.md`, section 8).
fn failure(error: &(dyn Error + 'static)) -> ExitCode {
    if !error.is::<exit::Silent>() {
        report(error);
    }
    ExitCode::from(exit::code_of(error))
}

/// The log format: `--log-format`, else `FLEETRING_LOG_FORMAT`, else the daemon's `[log]
/// format`, else text.
///
/// # Errors
///
/// The variable's value if it names no format.
fn log_format(
    flag: Option<LogFormat>,
    variable: Option<&str>,
    config: Option<&Config>,
) -> Result<LogFormat, String> {
    if let Some(format) = flag {
        return Ok(format);
    }
    if let Some(value) = variable {
        return value.parse().map_err(|_| value.to_owned());
    }
    Ok(config.map_or(LogFormat::Text, |config| config.log.format))
}

fn parse_log_format(text: &str) -> Result<LogFormat, String> {
    text.parse()
        .map_err(|error: ParseLogFormatError| error.to_string())
}

/// How long the runtime's remaining threads get to finish once the daemon has stopped.
const RUNTIME_GRACE: Duration = Duration::from_secs(10);

/// Executes `exe`, the binary the daemon started from, with the same arguments and the same
/// `argv[0]`, keeping the process ID, so that systemd, a container, or a terminal sees the same
/// process (ADR-0038), and a node started as `fleetringd` still calls itself that (ADR-0043,
/// point 3.6). Returns only if that fails.
#[cfg(unix)]
fn restart_in_place(exe: &Path, restarts: u32) -> ExitCode {
    use std::os::unix::process::CommandExt as _;
    tracing::info!(exe = %exe.display(), "restarting in place");
    let error = std::process::Command::new(exe)
        .arg0(
            std::env::args_os()
                .next()
                .unwrap_or_else(|| exe.as_os_str().to_owned()),
        )
        .args(std::env::args_os().skip(1))
        .env(RESTARTS_VAR, restarts.saturating_add(1).to_string())
        .exec();
    report(&error);
    ExitCode::FAILURE
}

#[cfg(not(unix))]
fn restart_in_place(_: &Path, _: u32) -> ExitCode {
    ExitCode::SUCCESS
}

/// Leaves the terminal's session, as `setsid` does: a process that already leads one gets
/// `EPERM`, which leaves it where it is, as after a restart in place.
#[cfg(unix)]
fn leave_the_session() {
    match rustix::process::setsid() {
        Ok(_) | Err(rustix::io::Errno::PERM) => {}
        Err(error) => eprintln!("{}: can't leave the terminal's session: {error}", program()),
    }
}

/// Prepares dev mode's directory: creates what it lacks, and loads its configuration.
fn prepare_dev(args: &DevArgs) -> Result<Prepared, DevError> {
    let dir = match &args.data_dir {
        Some(dir) => dir.clone(),
        None => dev::default_dir()?,
    };
    dev::prepare(&dir, args.port, SystemClock.now())
}

async fn run_daemon(config: Config, dev: Option<Prepared>) -> Result<Stopped, Box<dyn Error>> {
    // Read before an upgrade can replace the file: the path a restart executes again.
    let exe = std::env::current_exe()?;
    let data_dir = config.data_dir.clone();
    // Listened for before the daemon logs its start, so that no signal sent once it has started
    // finds the default action, which ends the process.
    let signals = StopSignals::listen();
    // Until the banner; log lines while it starts go around the spinner.
    let starting = dev.as_ref().map(|_| {
        Progress::start(
            "starting the dev node\u{2026}",
            &spinner_surroundings(MachineOutput::No),
        )
    });
    let daemon = Daemon::start(config).await?;
    if let Some(starting) = starting {
        // The banner is the result, so there is no line to leave behind.
        starting.finish_err();
    }
    if let Some(prepared) = &dev {
        // Written once the node serves, so a start that fails leaves the contexts alone.
        let context = context_cmd::write_dev_context(prepared);
        eprint!(
            "\n{}\n",
            prepared.banner(daemon.listen_address(), daemon.admin_address(), &context)
        );
    }
    let process = ProcessFile::this_process(exe.clone());
    if let Err(error) = process.write(&data_dir) {
        tracing::warn!(%error, "can't write the process file; `fleetring upgrade` can't restart this node in place");
    }
    let (told, why) = tokio::sync::oneshot::channel();
    daemon
        .run_until_stop(async {
            let stop = signals.next().await;
            let _unheard = told.send(stop);
            stop
        })
        .await;
    process.remove(&data_dir);
    Ok(match why.await {
        Ok(Stop::Restart) => Stopped::Restart {
            exe,
            restarts: process.restarts,
        },
        _ => Stopped::Exit,
    })
}

async fn run_command(command: Command) -> Result<(), Box<dyn Error>> {
    let output = match command {
        Command::Serve(_) | Command::Dev(_) => {
            return Err("serve and dev run a node, not a command".into());
        }
        Command::Context(_) | Command::Switch { .. } | Command::Completions(_) => {
            return Err("context, switch and completions don't talk to a fleet".into());
        }
        Command::Stop(args) => stop::run(args).await?,
        Command::Cert(command) => run_cert(command)?,
        Command::Token(command) => run_token(command).await?,
        Command::Status {
            node,
            address,
            output,
            target,
        } => match node_address(node.as_ref(), address, &target).await? {
            Some(address) => {
                // The node answers on its own, about itself.
                let client = Client::new(address.clone(), target.network()?);
                let status = node_status(&client.node_status(&address).await.map_err(failed)?);
                format::render(output.format(), &status, |status| {
                    Ok(format_node_status(status))
                })?
            }
            None => {
                let status = fleet_status(&target.client()?.status().await.map_err(failed)?);
                format::render(output.format(), &status, |status| Ok(format_status(status)))?
            }
        },
        Command::Mcp(McpArgs {
            read_only,
            max_result_bytes,
            target,
        }) => {
            // `--agent`: in dev mode, a node and an identity first (ADR-0043, point 8.1).
            let ready = agent_cmd::prepare_agent(
                &target,
                if read_only {
                    Access::ReadOnly
                } else {
                    Access::ReadWrite
                },
            )
            .await?;
            let client = match &ready {
                Some(ready) => target.client_or(ready.fallback_control())?,
                None => target.client()?,
            };
            // The principal the credentials name, read as `db show` reads it, so that only its
            // own descriptions are trusted.
            let principal = target
                .principal()
                .and_then(|principal| principal.parse().ok());
            let access = match &ready {
                _ if read_only => Access::ReadOnly,
                Some(ready) => ready.record().access(),
                None => Access::ReadWrite,
            };
            if let Some(ready) = &ready
                && quiet() == Quiet::No
            {
                eprint!("{}", ready.start_text(target.resolved()?.context.as_ref()));
            }
            let settings = McpSettings {
                access,
                max_result_bytes,
                principal,
            };
            mcp::serve(client, settings).await?;
            String::new()
        }
        Command::Agent(command) => agent_cmd::run(command).await?,
        Command::Db(DbCommand::Create {
            name,
            replication_factor,
            acks,
            min_replicas,
            metadata,
            target,
        }) => {
            let mut spec = database_spec(replication_factor)?;
            if let Some(acks) = acks {
                spec = spec.with_acks(acks);
            }
            if let Some(min_replicas) = min_replicas {
                spec = spec.with_min_replicas(min_replicas);
            }
            let metadata = metadata.read(std::io::stdin().lock())?;
            let client = target.client()?;
            let progress = Progress::start(
                format!("creating {}\u{2026}", shown(&name)),
                &spinner_surroundings(MachineOutput::No),
            );
            // A plain creation keeps the call every release knows (ADR-0041, point 2).
            let location = if metadata.is_empty() {
                client.create_database_with(&name, spec).await
            } else {
                client
                    .create_database_with_metadata(&name, spec, &metadata)
                    .await
            }
            .map_err(metadata_failed)?;
            progress.finish_ok(format!("created {}", shown(&name)));
            warn_if_under_replicated(&location);
            let mut output = format!("created {}\n{}", shown(&name), format_location(&location));
            if let Some(metadata) = &location.metadata {
                output.push_str(&format_metadata_summary(metadata));
            }
            output
        }
        Command::Db(DbCommand::Show {
            name,
            no_schema,
            count_limit,
            all,
            output,
            target,
        }) => {
            let client = target.client()?;
            let location = client.locate(&name).await.map_err(failed)?;
            let mut report = database_report(&location, SystemClock.now());
            if !no_schema {
                let request = SchemaRequest {
                    count_limit,
                    internal: if all { Internal::Show } else { Internal::Hide },
                };
                report = match client.schema(&name, request).await {
                    Ok(view) => report.with_schema(schema_report(&view)),
                    Err(status) => match schema_unavailable(&status) {
                        Some(reason) => report.without_schema(reason),
                        None => return Err(failed(status)),
                    },
                };
            }
            format::render(output.format(), &report, |report| {
                let mut text = Vec::new();
                render_text(report, target.principal().as_deref(), &mut text)?;
                Ok(String::from_utf8(text)?)
            })?
        }
        Command::Db(DbCommand::Edit(args)) => edit::run(args).await?,
        Command::Db(DbCommand::Watch(args)) => return watch_cmd::run(args).await,
        Command::Db(DbCommand::Exec(args)) => {
            let (client, name, statement) = args.into_parts().await?;
            let written = client.execute(&name, statement).await.map_err(failed)?;
            let mut output = format!(
                "{} row{} affected; acknowledged by {} of {} replica{}",
                written.rows_affected,
                plural(written.rows_affected),
                written.acknowledged_by,
                written.replicas,
                plural(u64::from(written.replicas)),
            );
            if written.under_replicated {
                output.push_str("; under-replicated");
            }
            output.push('\n');
            output
        }
        Command::Db(DbCommand::Query(QueryArgs { sql, output })) => {
            let (client, name, statement) = sql.into_parts().await?;
            let rows = client.query(&name, statement).await.map_err(failed)?;
            format::render(output.format(), &query_result(&rows), |_| {
                Ok(format_rows(&rows))
            })?
        }
        Command::Db(DbCommand::Locate {
            name,
            output,
            target,
        }) => {
            let location = target.client()?.locate(&name).await.map_err(failed)?;
            format::render(
                output.format(),
                &database_location(&location, SystemClock.now()),
                |_| Ok(format_location(&location)),
            )?
        }
        Command::Db(DbCommand::List {
            node,
            labels,
            prefix,
            output,
            target,
        }) => {
            let filter = ListFilter {
                labels,
                prefix,
                ..ListFilter::default()
            };
            let mut databases = target
                .client()?
                .list_databases_where(&filter)
                .await
                .map_err(failed)?
                .databases;
            if let Some(node) = &node {
                databases.retain(|location| location.primary.as_ref() == Some(node));
            }
            let now = SystemClock.now();
            let mut list = DatabaseList {
                databases: databases
                    .iter()
                    .map(|location| database_location(location, now))
                    .collect(),
            };
            list.databases.sort_by(|a, b| a.database.cmp(&b.database));
            format::render(output.format(), &list, |_| Ok(format_databases(&databases)))?
        }
        Command::Db(DbCommand::Verify {
            name,
            at,
            expect,
            output,
            target,
        }) => {
            let format = output.format();
            let progress = Progress::start(
                format!("verifying {}\u{2026}", shown(&name)),
                &spinner_surroundings(format.machine()),
            );
            let report = verify(&target.client()?, &name, at, expect).await?;
            let Some(problem) = verify_problem(&report) else {
                progress.finish_ok(format!("verified {}", shown(&name)));
                return emit(&format::render(format, &report, |report| {
                    Ok(verify_text(report))
                })?);
            };
            progress.finish_err();
            match format {
                // The report is the result, and the failed check the exit code.
                OutputFormat::Json | OutputFormat::Jsonl => {
                    emit(&format::render(format, &report, |_| Ok(String::new()))?)?;
                    return Err(problem.into());
                }
                OutputFormat::Table => {
                    return Err(format!("{}{problem}", verify_text(&report)).into());
                }
            }
        }
        Command::Db(DbCommand::Restore {
            name,
            archive,
            from,
            point,
            replication_factor,
            acks,
            metadata,
            target,
        }) => {
            let mut spec = database_spec(replication_factor)?;
            if let Some(acks) = acks {
                spec = spec.with_acks(acks);
            }
            let metadata = metadata.read(std::io::stdin().lock())?;
            let client = target.client()?;
            let progress = Progress::start(
                format!("restoring {}\u{2026}", shown(&name)),
                &spinner_surroundings(MachineOutput::No),
            );
            let restored = client
                .restore_database_with_metadata(
                    &name,
                    spec,
                    &metadata,
                    &archive,
                    &from,
                    &point.target(),
                )
                .await
                .map_err(metadata_failed)?;
            progress.finish_ok(format!("restored {}", shown(&name)));
            warn_if_under_replicated(&restored.location);
            // The follow-ups, in order, once the restore has returned (ADR-0041, point 10): the
            // archive's description and labels, whose failure is a warning, then the expiry the
            // flags asked for, whose failure fails the command.
            let archived = restore_archived_metadata(&client, &name, &restored, &metadata).await;
            let expiry = client.set_restored_expiry(&name, metadata.expiry).await;
            let mut output = format!(
                "restored {name} from {from} in archive {archive}\n{}{}{archived}",
                format_restored(
                    restored.position.as_ref(),
                    &restored.chain_hash,
                    &restored.snapshot,
                    restored.segments,
                    restored.archived_at_ms
                ),
                format_location(&restored.location),
                name = shown(&name),
                from = shown(&from)
            );
            match &expiry {
                RestoredExpiry::Set(at) => {
                    let _ = writeln!(output, "expires: {}", format_time(at.as_millis()));
                }
                RestoredExpiry::Failed { message, .. } => {
                    emit(&output)?;
                    eprintln!(
                        "warning: the database was restored without its expiry: {}",
                        printable(message)
                    );
                    eprintln!(
                        "{}",
                        hinted(&format!(
                            "hint: fleetring db edit {} --expires-in <d>",
                            shown(&name)
                        ))
                    );
                    return Err(exit::Silent(exit::FAILURE).into());
                }
                _ => {}
            }
            output
        }
        Command::Archive(ArchiveCommand::Show {
            name,
            archive,
            address,
            output,
            target,
        }) => {
            let client = target.client()?;
            let address = archive_node(&client, address).await?;
            let description = client
                .describe_archive(&address, &archive, &name)
                .await
                .map_err(failed)?;
            format::render(
                output.format(),
                &archive_report(&archive, &description),
                |_| Ok(format_archive(&archive, &description)),
            )?
        }
        Command::Archive(ArchiveCommand::Verify {
            name,
            archive,
            address,
            point,
            target,
        }) => {
            let progress = Progress::start(
                format!("verifying the archive of {}\u{2026}", shown(&name)),
                &spinner_surroundings(MachineOutput::No),
            );
            let report =
                verify_archive(&target.client()?, &name, &archive, address, &point.target())
                    .await?;
            progress.finish_ok(format!("verified the archive of {}", shown(&name)));
            report
        }
        Command::Archive(ArchiveCommand::Restore {
            name: from,
            to,
            config,
            archive,
            point,
        }) => {
            let progress = Progress::start(
                format!("restoring {} from the archive\u{2026}", shown(&from)),
                &spinner_surroundings(MachineOutput::No),
            );
            let report = restore_offline(&config, &archive, &from, &to, &point.target()).await?;
            progress.finish_ok(format!("restored {} into {}", shown(&from), to.display()));
            report
        }
        Command::Db(DbCommand::Drop { name, yes, target }) => {
            if !yes {
                confirm::confirm(&confirm::drop_database(&shown(&name), &target.place()?))?;
            }
            let epoch = target
                .client()?
                .drop_database(&name)
                .await
                .map_err(failed)?;
            format!("dropped {} at epoch {}\n", shown(&name), epoch.get())
        }
        Command::Node(NodeCommand::List { output, target }) => {
            let nodes = node_list(&target.client()?.status().await.map_err(failed)?);
            format::render(output.format(), &nodes, |nodes| Ok(node_list_text(nodes)))?
        }
        Command::Node(NodeCommand::Drain { id, yes, target }) => {
            let client = target.client()?;
            if !yes {
                let only = only_replica_of(&client, &id).await?;
                if !only.is_empty() {
                    confirm::confirm(&confirm::drain_only_replica(
                        id.as_str(),
                        &only,
                        &target.place()?,
                    ))?;
                }
            }
            let progress = Progress::start(
                format!("draining {id}\u{2026}"),
                &spinner_surroundings(MachineOutput::No),
            );
            // The control plane hands every primary over before it answers, so the message
            // doesn't change per database.
            let primaries = client.drain_node(&id).await.map_err(failed)?;
            progress.finish_ok(format!("drained {id}"));
            format_drain(&id, &primaries)
        }
        Command::Upgrade(args) => run_upgrade(args).await?,
        Command::Node(NodeCommand::Remove {
            id,
            force,
            yes,
            target,
        }) => {
            let client = target.client()?;
            let view = client.status().await.map_err(failed)?;
            let Some((_, liveness)) = view.members().find(|(member, _)| member.id() == &id) else {
                return Err(format!("no member of the fleet has the node ID '{id}'").into());
            };
            if liveness == Liveness::Live && !force {
                return Err(format!(
                    "node '{id}' is live; stop it first, or pass --force (a live node joins \
                     again at its next heartbeat)"
                )
                .into());
            }
            if !yes {
                confirm::confirm(&confirm::remove_node(id.as_str(), &target.place()?))?;
            }
            let epoch = client.remove_node(&id).await.map_err(failed)?;
            format!(
                "removed {id} from the fleet at membership epoch {}\n",
                epoch.get()
            )
        }
        Command::Node(NodeCommand::Undrain { id, target }) => {
            target.client()?.undrain_node(&id).await.map_err(failed)?;
            format!("{id} is no longer draining; it may take primaries again\n")
        }
        Command::Orphan(OrphanCommand::List {
            node,
            address,
            output,
            target,
        }) => {
            let orphans = list_orphans(node.as_ref(), address, &target).await?;
            let list = OrphanList {
                orphans: orphans
                    .iter()
                    .map(|(node, orphan)| orphan_report(node, orphan))
                    .collect(),
            };
            format::render(output.format(), &list, |_| Ok(format_orphans(&orphans)))?
        }
        Command::Orphan(OrphanCommand::Remove {
            name,
            node,
            address,
            yes,
            target,
        }) => {
            let address = node_address(node.as_ref(), address, &target)
                .await?
                .ok_or("pass --node or --address")?;
            let label = node.map_or_else(|| address.clone(), |node| node.to_string());
            if !yes {
                confirm::confirm(&confirm::remove_orphan(
                    &shown(&name),
                    &label,
                    &target.place()?,
                ))?;
            }
            let client = Client::new(address.clone(), target.network()?);
            let progress = Progress::start(
                format!("removing the orphan copy of {}\u{2026}", shown(&name)),
                &spinner_surroundings(MachineOutput::No),
            );
            let epoch = client
                .remove_orphan(&address, &name)
                .await
                .map_err(failed)?;
            progress.finish_ok(format!("removed the orphan copy of {}", shown(&name)));
            format!(
                "removed the orphan copy of {} from epoch {} on {label}\n",
                shown(&name),
                epoch.get()
            )
        }
        Command::Lease(LeaseCommand::Watch {
            node,
            address,
            output,
            target,
        }) => {
            let address = node_address(node.as_ref(), address, &target)
                .await?
                .ok_or("pass --node or --address")?;
            // The node answers on its own, about the leases it holds.
            let client = Client::new(address.clone(), target.network()?);
            let updates = client.watch_leases(&address).await.map_err(failed)?;
            return watch_leases(updates, output.format()).await;
        }
    };
    emit(&output)
}

/// Prints `output` on stdout. A reader that has gone away, as `| head` leaves it, ends the
/// command quietly (`docs/cli.md`, section 10).
fn emit(output: &str) -> Result<(), Box<dyn Error>> {
    let mut stdout = std::io::stdout().lock();
    match stdout
        .write_all(output.as_bytes())
        .and_then(|()| stdout.flush())
    {
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        other => Ok(other?),
    }
}

/// Prints each lease update as it arrives, flushed, until the stream ends, stdout closes (exit
/// 0), or Ctrl-C (exit 130). In `json` and `jsonl` each lease is one object on its own line.
async fn watch_leases(
    mut updates: LeaseUpdates,
    format: OutputFormat,
) -> Result<(), Box<dyn Error>> {
    let interrupted = tokio::signal::ctrl_c();
    tokio::pin!(interrupted);
    // The node names itself in the stream's first message.
    let mut node: Option<String> = None;
    loop {
        let update = tokio::select! {
            _ = &mut interrupted => return Err(exit::Silent(exit::INTERRUPTED).into()),
            update = updates.next() => {
                let update: Result<Option<LeaseUpdate>, tonic::Status> = update;
                update.map_err(failed)?
            }
        };
        let Some(update) = update else {
            return Ok(());
        };
        let text = match format {
            OutputFormat::Table => format_lease_update(&update),
            OutputFormat::Json | OutputFormat::Jsonl => {
                if let LeaseUpdate::Held { node: held, .. } = &update {
                    node = Some(held.to_string());
                }
                lease_events(&update, node.as_deref())
                    .iter()
                    .map(format::line)
                    .collect::<Result<String, _>>()?
            }
        };
        let mut stdout = std::io::stdout().lock();
        match stdout
            .write_all(text.as_bytes())
            .and_then(|()| stdout.flush())
        {
            Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => return Ok(()),
            other => other?,
        }
    }
}

/// Runs `upgrade` and returns what it prints. A check that finds a newer release the fleet
/// doesn't allow prints its report and fails.
async fn run_upgrade(args: UpgradeArgs) -> Result<String, Box<dyn Error>> {
    if args.software {
        return Ok(format!("{}\n", fleetringd::software::running_json()));
    }
    let mode = if args.rollback {
        Mode::Rollback
    } else if args.check {
        Mode::Check
    } else {
        Mode::Install
    };
    let node = match &args.target.config {
        Some(path) => {
            let config = Config::load(path)?;
            Some(LocalNode {
                id: config.node.id.clone(),
                data_dir: config.data_dir.clone(),
                config: std::path::absolute(path)?,
            })
        }
        None => None,
    };
    let fleet = if args.target.control.is_some() || args.target.config.is_some() {
        Some(args.target.client()?)
    } else {
        None
    };
    // Starts at the first step, after any question the upgrade asks, and clears itself if the
    // upgrade fails.
    let stepped = Stepped::new(spinner_surroundings(MachineOutput::No));
    let outcome = upgrade::run(Options {
        mode,
        restart: if args.no_restart {
            Restart::No
        } else {
            Restart::InPlace
        },
        version: args.version,
        base_url: args.base_url,
        releases_url: args.releases_url,
        node,
        fleet,
        timeout: args.timeout.unwrap_or(UPGRADE_TIMEOUT),
        steps: stepped.steps(),
        start_command: if args.yes {
            StartCommand::Confirmed
        } else if std::io::stdin().is_terminal() {
            StartCommand::Ask
        } else {
            StartCommand::Unknown
        },
    })
    .await?;
    stepped.finish_ok(match mode {
        Mode::Check => "checked the release",
        Mode::Install => "upgraded",
        Mode::Rollback => "rolled back",
    });
    if outcome.blocked {
        emit(&outcome.report)?;
        return Err("the fleet doesn't allow the newest release yet".into());
    }
    Ok(outcome.report)
}

/// What a spinner may show on in this process: none with `-q`, or for machine output.
fn spinner_surroundings(machine_output: MachineOutput) -> Surroundings {
    Surroundings::capture(machine_output, quiet())
}

/// The address of the data node a command asks directly: `address` as given, or the address the
/// control plane's membership records for `node`; `None` if neither is given.
async fn node_address(
    node: Option<&NodeId>,
    address: Option<String>,
    target: &Target,
) -> Result<Option<String>, Box<dyn Error>> {
    match node {
        Some(node) => Ok(Some(member_address(&target.client()?, node).await?)),
        None => Ok(address),
    }
}

/// The data node an archive command asks: `address` as given, or the first live member with an
/// address.
async fn archive_node(client: &Client, address: Option<String>) -> Result<String, Box<dyn Error>> {
    if let Some(address) = address {
        return Ok(address);
    }
    let view = client.status().await.map_err(failed)?;
    view.members()
        .filter(|(_, liveness)| *liveness == Liveness::Live)
        .find_map(|(member, _)| member.address().map(str::to_owned))
        .ok_or_else(|| "no live data node has an address; pass --address".into())
}

/// What a restore reports: where it got, verified how, from what.
fn format_restored(
    position: Option<&Position>,
    chain_hash: &ChainHash,
    snapshot: &str,
    segments: u32,
    archived_at_ms: Option<u64>,
) -> String {
    format!(
        "position: {}\nchain hash: {chain_hash}\nsnapshot: {snapshot}\nsegments applied: \
         {segments}\nlast segment archived: {}\n",
        position.map_or_else(|| "the log's beginning".to_owned(), ToString::to_string),
        archived_at_ms.map_or_else(|| "none applied".to_owned(), format_time),
    )
}

/// `archive show`'s report.
fn format_archive(archive: &str, description: &ArchiveDescription) -> String {
    let mut output = format!(
        "database: {}\narchive: {archive}\nlatest: {}\nlast segment archived: {}\n\
         segments: {} ({} bytes)\nsnapshots:\n",
        shown(&description.database),
        description
            .latest
            .as_ref()
            .map_or_else(|| "nothing archived".to_owned(), ToString::to_string),
        description
            .latest_ms
            .map_or_else(|| "none".to_owned(), format_time),
        description.segments,
        description.segment_bytes,
    );
    if description.snapshots.is_empty() {
        output.push_str("  none\n");
    }
    for snapshot in &description.snapshots {
        let _ = writeln!(
            output,
            "  {}: position {} epoch {} taken {} ({} bytes) metadata: {}",
            snapshot.id,
            snapshot
                .position
                .as_ref()
                .map_or_else(|| "begin".to_owned(), ToString::to_string),
            snapshot.epoch.get(),
            format_time(snapshot.taken_at_ms),
            snapshot.bytes,
            if snapshot.has_metadata { "yes" } else { "no" }
        );
    }
    if description.incomplete > 0 {
        let _ = writeln!(output, "incomplete snapshots: {}", description.incomplete);
    }
    for damaged in &description.damaged {
        let _ = writeln!(output, "unusable: {damaged}");
    }
    output
}

/// Runs `archive verify`: a scratch restore on a data node, then each live replica's hash at
/// the restored position, compared with the archive's.
async fn verify_archive(
    client: &Client,
    name: &DatabaseName,
    archive: &str,
    address: Option<String>,
    target: &RestoreTarget,
) -> Result<String, Box<dyn Error>> {
    let address = archive_node(client, address).await?;
    let verified = client
        .verify_archive(&address, archive, name, target)
        .await
        .map_err(failed)?;
    let mut output = format!(
        "database: {}\narchive: {archive}\n{}",
        shown(name),
        format_restored(
            verified.position.as_ref(),
            &verified.chain_hash,
            &verified.snapshot,
            verified.segments,
            None
        )
    );
    let location = match client.locate(name).await {
        Ok(location) => location,
        Err(status) if status.code() == tonic::Code::NotFound => {
            output.push_str("the archive verifies; there is no live database to compare it with\n");
            return Ok(output);
        }
        Err(status) => return Err(failed(status)),
    };
    let mut differ = Vec::new();
    let mut unanswered = Vec::new();
    for replica in &location.replicas {
        let Some(address) = &replica.address else {
            continue;
        };
        match client
            .verify(address, name, verified.position.as_ref())
            .await
        {
            Ok(answer) if answer.chain_hash == verified.chain_hash => {
                let _ = writeln!(output, "  {}: agrees", replica.node);
            }
            Ok(answer) => {
                let _ = writeln!(output, "  {}: holds {}", replica.node, answer.chain_hash);
                differ.push(replica.node.to_string());
            }
            Err(status) => {
                let _ = writeln!(output, "  {}: {}", replica.node, exit::describe(&status));
                unanswered.push(replica.node.to_string());
            }
        }
    }
    if !differ.is_empty() {
        return Err(format!(
            "{output}the archive differs from {} at its latest point",
            differ.join(", ")
        )
        .into());
    }
    if unanswered.is_empty() {
        output.push_str("archive agrees with every replica\n");
    } else {
        let _ = writeln!(
            output,
            "archive agrees with every replica that answered; {} couldn't",
            unanswered.join(", ")
        );
    }
    Ok(output)
}

/// The file `archive restore` writes the archived metadata to, beside the restored database.
const ARCHIVED_METADATA_FILE: &str = "metadata.json";

/// Sets the archived description and labels on the restored database `name`, and reports what
/// the archive held (ADR-0041, point 10). The data has restored by now, so nothing here fails
/// the command: what can't be set is a warning on stderr, and the report still shows it.
///
/// The description is set without an author, so no reader trusts it, and the archived author
/// and creator are printed as the archive's claims and recorded nowhere. The expiry is never
/// restored.
async fn restore_archived_metadata(
    client: &Client,
    name: &DatabaseName,
    restored: &Restoration,
    flags: &NewMetadata,
) -> String {
    if let Some(reason) = &restored.archived_metadata_skipped {
        note(format!(
            "warning: the archived metadata was skipped: {}",
            printable(reason)
        ));
    }
    let Some(archived) = &restored.archived_metadata else {
        return restored
            .archived_metadata_skipped
            .as_ref()
            .map_or_else(String::new, |_| "archived metadata: skipped\n".to_owned());
    };
    let restore = restored_metadata(archived, flags);
    for skipped in &restore.skipped {
        note(format!("warning: skipped {}", printable(skipped)));
    }
    if !restore.change.is_empty()
        && let Err(status) = client.update_database(name, &restore.change, None).await
    {
        note(format!(
            "warning: the archived description and labels weren't restored: {}",
            exit::describe(&status)
        ));
    }
    let mut report = Vec::new();
    // Writing to a vector can't fail.
    let _ = render_archived_metadata(archived, &mut report);
    String::from_utf8_lossy(&report).into_owned()
}

/// Runs `archive restore`: builds the store `config`'s `[[archive]]` entry names, restores `from`
/// into `to`, and checkpoints it so that `data.db` holds every row.
async fn restore_offline(
    config: &Path,
    archive: &str,
    from: &DatabaseName,
    to: &Path,
    target: &RestoreTarget,
) -> Result<String, Box<dyn Error>> {
    let config = Config::load(config)?;
    let entry = config
        .archive
        .iter()
        .find(|entry| entry.name == archive)
        .ok_or_else(|| format!("the configuration has no [[archive]] named `{archive}`"))?;
    let store = build_store(entry, std::sync::Arc::new(SystemClock), Execution::Threads)?;
    let engine = SqliteEngine::new(config.engine.page_size)?;
    let restored = restore_into(
        store.as_ref(),
        &ArchiveKeys::new(&entry.prefix, from),
        from,
        std::sync::Arc::new(engine.clone()),
        target,
        to,
        Execution::Threads,
    )
    .await?;
    finish_offline(&engine, to)?;
    let metadata = match (&restored.metadata, &restored.metadata_skipped) {
        (Some(metadata), _) => {
            let file = to.join(ARCHIVED_METADATA_FILE);
            std::fs::write(&file, encode_metadata(metadata))?;
            format!(
                "the archived metadata, the archive's claims and not verified: {}\n",
                file.display()
            )
        }
        (None, Some(reason)) => {
            note(format!(
                "warning: the archived metadata was skipped: {}",
                printable(reason)
            ));
            "the archived metadata was skipped\n".to_owned()
        }
        (None, None) => String::new(),
    };
    Ok(format!(
        "restored {from} from archive {archive} into {}\n{}the directory holds a plain SQLite \
         database: {}\n{metadata}",
        to.display(),
        format_restored(
            restored.position.as_ref(),
            &restored.chain,
            &restored.snapshot,
            restored.segments,
            restored.archived_at_ms
        ),
        to.join("data.db").display(),
        from = shown(from)
    ))
}

/// The address the member `node` advertised, from the control plane's membership.
async fn member_address(client: &Client, node: &NodeId) -> Result<String, Box<dyn Error>> {
    let view = client.status().await.map_err(failed)?;
    let Some((member, _)) = view.members().find(|(member, _)| member.id() == node) else {
        let mut message = format!("no member of the fleet has the node ID '{node}'");
        // Node IDs may contain `:` and `.`, so an address is only told apart once no member
        // has it as its ID.
        if node.as_str().parse::<SocketAddr>().is_ok() || node.as_str().contains(':') {
            let _ = write!(
                message,
                "\n  hint: --node takes a node ID; to dial an address, use --address {node}"
            );
        }
        return Err(message.into());
    };
    member
        .address()
        .map(str::to_owned)
        .ok_or_else(|| format!("node '{node}' advertised no address; pass --address").into())
}

/// One line per lease the node holds when the stream opens, or one line for an event.
fn format_lease_update(update: &LeaseUpdate) -> String {
    match update {
        LeaseUpdate::Held { leases, .. } => leases
            .iter()
            .map(|(database, epoch)| format!("held {} epoch {}\n", shown(database), epoch.get()))
            .collect(),
        LeaseUpdate::Event(event) => format!("{}\n", shown(event)),
        _ => String::new(),
    }
}

/// Runs a `token` command and returns what it prints.
async fn run_token(command: TokenCommand) -> Result<String, Box<dyn Error>> {
    match command {
        TokenCommand::Issue {
            principal,
            scopes,
            ttl,
            dir,
        } => {
            let ttl = ttl.unwrap_or(DEFAULT_TOKEN_LIFETIME);
            principal.refuse_reserved()?;
            for warning in scopes.iter().filter_map(Scope::separator_warning) {
                note(format!("warning: {warning}"));
            }
            if ttl > MAX_TOKEN_LIFETIME {
                return Err(format!(
                    "a token lasts at most {} days, so its revocation stays short",
                    MAX_TOKEN_LIFETIME.as_secs() / (24 * 60 * 60)
                )
                .into());
            }
            let signer = agent::load_authority(&dir)?.token_signer()?;
            let id = agent::new_token_id()?;
            let not_before = SystemClock.now();
            let ttl_millis = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
            let grant = Grant {
                principal: principal.clone(),
                scopes: scopes.into_iter().collect(),
                id: id.clone(),
                not_before,
                expires_at: not_before.saturating_add(Millis::new(ttl_millis)),
            };
            let token = signer.issue(&grant)?;
            note(format!(
                "issued token {id} for {principal}, signed by key {}, for {}s",
                signer.key_id(),
                ttl.as_secs()
            ));
            Ok(format!("{}\n", token.expose()))
        }
        TokenCommand::Revoke {
            token_id,
            key_id,
            ttl,
            yes,
            target,
        } => {
            let (revoked, until) = match (token_id, key_id) {
                (Some(id), _) => {
                    let keep = ttl.unwrap_or(MAX_TOKEN_LIFETIME);
                    let keep = u64::try_from(keep.as_millis()).unwrap_or(u64::MAX);
                    (
                        Revoked::TokenId(id.to_string()),
                        SystemClock.now().saturating_add(Millis::new(keep)),
                    )
                }
                (None, Some(id)) => (Revoked::KeyId(id.to_string()), Timestamp::MAX),
                (None, None) => return Err("pass --token-id or --key-id".into()),
            };
            let what = match &revoked {
                Revoked::TokenId(id) => format!("token {id}"),
                Revoked::KeyId(id) => format!("signing key {id}"),
            };
            let place = target.place()?;
            match &revoked {
                Revoked::KeyId(id) if !yes => confirm::confirm(&confirm::revoke_key(id, &place))?,
                _ => note(format!("revoking {what} {}", place.on())),
            }
            target
                .client()?
                .control()
                .revoke_token(RevokeTokenRequest {
                    revocation: Some(Revocation {
                        revoked: Some(revoked),
                        until_ms: until.as_millis(),
                    }),
                })
                .await
                .map_err(failed)?;
            Ok(format!(
                "revoked {what}; every node refuses it within one heartbeat interval\n"
            ))
        }
    }
}

/// Runs a `cert` command and returns what it wrote.
fn run_cert(command: CertCommand) -> Result<String, Box<dyn Error>> {
    let validity = |ttl| Validity::new(SystemClock.now(), ttl);
    match command {
        CertCommand::Init { fleet, dir, ttl } => {
            let authority = Authority::create(
                fleet.clone(),
                validity(ttl.unwrap_or(DEFAULT_AUTHORITY_LIFETIME)),
            )?;
            authority.write(&dir)?;
            Ok(format!(
                "created the authority for fleet {fleet} in {}: ca.crt and ca.key (keep \
                 ca.key off every node)\n",
                dir.display()
            ))
        }
        CertCommand::Node {
            id,
            addresses,
            dir,
            ttl,
        } => {
            let authority = agent::load_authority(&dir)?;
            let issued = authority.issue_node(
                &id,
                &addresses,
                validity(ttl.unwrap_or(DEFAULT_NODE_LIFETIME)),
            )?;
            let stem = format!("node.{id}");
            issued.write(&dir, &stem)?;
            Ok(format!(
                "issued {} for {} in {}: {stem}.crt and {stem}.key\n",
                issued.subject(),
                addresses.join(", "),
                dir.display()
            ))
        }
        CertCommand::Client {
            principal,
            dir,
            out,
            ttl,
        } => {
            principal.refuse_reserved()?;
            let authority = agent::load_authority(&dir)?;
            let issued = authority
                .issue_client(&principal, validity(ttl.unwrap_or(DEFAULT_CLIENT_LIFETIME)))?;
            let stem = format!("client.{principal}");
            let Some(out) = out else {
                issued.write(&dir, &stem)?;
                return Ok(format!(
                    "issued {} in {}: {stem}.crt and {stem}.key\n",
                    issued.subject(),
                    dir.display()
                ));
            };
            if let Some(warning) = agent::prepare_out_dir(&out)? {
                note(warning);
            }
            // The copy of `ca.crt` first, so a directory it refuses holds nothing new.
            agent::write_new_private(&out.join(AUTHORITY_CERT_FILE), authority.certificate_pem())?;
            issued.write(&out, &stem)?;
            Ok(format!(
                "issued {} in {}: {stem}.crt, {stem}.key, and a copy of ca.crt\n",
                issued.subject(),
                out.display()
            ))
        }
    }
}

impl SqlArgs {
    async fn into_parts(self) -> Result<(Client, DatabaseName, Statement), Box<dyn Error>> {
        let mut client = self.target.client()?;
        let entry = match (self.node, self.address) {
            (Some(node), _) => Some(member_address(&client, &node).await?),
            (None, address) => address,
        };
        if let Some(entry) = entry {
            client = client.with_entry(entry);
        }
        Ok((
            client,
            self.name,
            Statement::with_params(self.sql, self.params),
        ))
    }
}

/// Runs `db verify`: asks every replica with an address for its chain hash at `at`, or at the
/// latest position they all hold, and reports each answer, whether they agree, and, with
/// `expect`, whether the log still hashes to a hash recorded earlier.
async fn verify(
    client: &Client,
    name: &DatabaseName,
    at: Option<Position>,
    expect: Option<ChainHash>,
) -> Result<VerifyReport, Box<dyn Error>> {
    let location = client.locate(name).await.map_err(failed)?;
    let replicas: Vec<(NodeId, String)> = location
        .replicas
        .iter()
        .filter_map(|replica| Some((replica.node.clone(), replica.address.clone()?)))
        .collect();
    if replicas.is_empty() {
        return Err(format!("no replica of {} has an address", shown(name)).into());
    }
    let ask = |at: Option<Position>| {
        let replicas = &replicas;
        async move {
            let mut answers = Vec::with_capacity(replicas.len());
            for (node, address) in replicas {
                let answer = client
                    .verify(address, name, at.as_ref())
                    .await
                    .map_err(|status| exit::describe(&status));
                answers.push((node.clone(), answer));
            }
            answers
        }
    };
    // The latest position every replica holds: the lowest the control plane has heard each
    // report, so a replica is held to what it claimed; failing that, the lowest they answer
    // with now, asking the ones past it again there.
    let reported = location
        .replicas
        .iter()
        .filter(|replica| replica.address.is_some())
        .map(|replica| replica.position.clone())
        .min()
        .flatten();
    let mut answers = ask(at.clone().or_else(|| reported.clone())).await;
    let position = match at.or(reported) {
        Some(at) => Some(at),
        None => {
            let common = answers
                .iter()
                .filter_map(|(_, answer)| answer.as_ref().ok())
                .map(|verification| verification.position.clone())
                .min()
                .flatten();
            let behind = answers.iter().any(|(_, answer)| {
                answer
                    .as_ref()
                    .is_ok_and(|verification| verification.position != common)
            });
            if behind {
                answers = ask(common.clone()).await;
            }
            common
        }
    };
    let first = answers
        .iter()
        .find_map(|(_, answer)| answer.as_ref().ok())
        .map(|verification| verification.chain_hash);
    let agree = first.is_some()
        && answers.iter().all(|(_, answer)| {
            answer
                .as_ref()
                .is_ok_and(|verification| Some(&verification.chain_hash) == first.as_ref())
        });
    Ok(VerifyReport {
        database: name.to_string(),
        position: position.as_ref().map(ToString::to_string),
        replicas: answers
            .into_iter()
            .map(|(node, answer)| match answer {
                Ok(verification) => ReplicaHash {
                    node: node.to_string(),
                    chain_hash: Some(verification.chain_hash.to_string()),
                    error: None,
                },
                Err(reason) => ReplicaHash {
                    node: node.to_string(),
                    chain_hash: None,
                    error: Some(reason),
                },
            })
            .collect(),
        unchanged: expect
            .as_ref()
            .filter(|_| agree)
            .map(|expect| first.as_ref() == Some(expect)),
        expected: expect.as_ref().map(ToString::to_string),
        chain_hash: first.as_ref().map(ToString::to_string),
        agree,
    })
}

/// The position `report` verified at, as text output prints it.
fn verified_at(report: &VerifyReport) -> &str {
    report.position.as_deref().unwrap_or("empty log")
}

/// What `db verify` printed for people: the position, each replica's answer, and the outcome.
fn verify_text(report: &VerifyReport) -> String {
    let mut output = format!(
        "database: {}\nposition: {}\n",
        printable(&report.database),
        verified_at(report)
    );
    for replica in &report.replicas {
        let answer = replica
            .chain_hash
            .as_deref()
            .or(replica.error.as_deref())
            .unwrap_or("no answer");
        let _ = writeln!(output, "  {}: {answer}", replica.node);
    }
    if let (true, Some(hash)) = (report.agree, &report.chain_hash) {
        let count = report.replicas.len();
        let _ = writeln!(
            output,
            "chain hash: {hash}\nall {count} replica{} agree",
            plural(u64::try_from(count).unwrap_or(u64::MAX))
        );
        if report.unchanged == Some(true) {
            let _ = writeln!(output, "nothing changed since {}", verified_at(report));
        }
    }
    output
}

/// Why `report` fails the command, if it does: no replica answered, the replicas disagree (the
/// first that differs is named), or the log no longer hashes to the expected hash.
fn verify_problem(report: &VerifyReport) -> Option<String> {
    let name = printable(&report.database);
    let Some(hash) = &report.chain_hash else {
        return Some(format!("no replica of {name} could answer"));
    };
    if !report.agree {
        let differs = report.replicas.iter().find_map(|replica| {
            match (&replica.chain_hash, &replica.error) {
                (Some(held), _) if held != hash => Some((&replica.node, format!("holds {held}"))),
                (None, Some(error)) => Some((&replica.node, error.clone())),
                _ => None,
            }
        });
        let (node, reason) = differs?;
        return Some(format!(
            "replicas of {name} disagree at {}: {node} {reason}",
            verified_at(report)
        ));
    }
    match (&report.expected, report.unchanged) {
        (Some(expected), Some(false)) => Some(format!(
            "the log at {} hashes to {hash}, not the expected {expected}",
            verified_at(report)
        )),
        _ => None,
    }
}

/// The databases whose only replica is on `node`, which a drain leaves unavailable until the
/// node returns (`docs/cli.md`, section 7).
async fn only_replica_of(client: &Client, node: &NodeId) -> Result<Vec<String>, Box<dyn Error>> {
    let listing = client
        .list_databases_where(&ListFilter::default())
        .await
        .map_err(failed)?;
    Ok(listing
        .databases
        .iter()
        .filter(|location| {
            location.replicas.len() == 1
                && location
                    .replicas
                    .first()
                    .is_some_and(|replica| &replica.node == node)
        })
        .map(|location| shown(&location.database))
        .collect())
}

/// The orphans `orphan list` reports, each with the node that keeps it, in node and then name
/// order: one node's, if `node` or `address` names it, or every member's otherwise. A member that
/// can't be asked is named on stderr and skipped.
async fn list_orphans(
    node: Option<&NodeId>,
    address: Option<String>,
    target: &Target,
) -> Result<Vec<(String, Orphan)>, Box<dyn Error>> {
    let network = target.network()?;
    let nodes: Vec<(String, String)> = match (node, address) {
        (Some(node), _) => vec![(
            node.to_string(),
            member_address(&target.client()?, node).await?,
        )],
        (None, Some(address)) => vec![(address.clone(), address)],
        (None, None) => {
            let view = target.client()?.status().await.map_err(failed)?;
            view.members()
                .filter_map(|(member, _)| {
                    Some((member.id().to_string(), member.address()?.to_owned()))
                })
                .collect()
        }
    };
    let asked_one = nodes.len() == 1;
    let mut orphans = Vec::new();
    for (label, address) in nodes {
        let client = Client::new(address.clone(), network.clone());
        match client.node_status(&address).await {
            Ok(status) => {
                let label = if asked_one {
                    status.node.to_string()
                } else {
                    label
                };
                orphans.extend(
                    status
                        .orphans
                        .into_iter()
                        .map(|orphan| (label.clone(), orphan)),
                );
            }
            Err(status) if asked_one => return Err(failed(status)),
            Err(status) => note(format!(
                "warning: can't ask {label}: {}",
                printable(status.message())
            )),
        }
    }
    Ok(orphans)
}

/// `orphan list`'s table: one row per orphan, under `NODE`, `DATABASE`, `EPOCH`, and `BYTES`.
fn format_orphans(orphans: &[(String, Orphan)]) -> String {
    if orphans.is_empty() {
        return "no orphans\n".to_owned();
    }
    let rows: Vec<[String; 4]> = orphans
        .iter()
        .map(|(node, orphan)| {
            [
                node.clone(),
                shown(&orphan.database),
                orphan.epoch.get().to_string(),
                orphan.bytes.to_string(),
            ]
        })
        .collect();
    table(["NODE", "DATABASE", "EPOCH", "BYTES"], &rows)
}

fn plural(count: u64) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Integer(integer) => integer.to_string(),
        Value::Real(real) => real.to_string(),
        Value::Text(text) => text.clone(),
        Value::Blob(blob) => {
            let mut hex = String::with_capacity(3 + 2 * blob.len());
            hex.push_str("x'");
            for byte in blob {
                // Writing to a `String` can't fail.
                let _ = write!(hex, "{byte:02x}");
            }
            hex.push('\'');
            hex
        }
    }
}

/// The rows as tab-separated lines, with the column names first.
fn format_rows(rows: &Rows) -> String {
    let mut output = rows.columns.join("\t");
    output.push('\n');
    for row in &rows.rows {
        let values: Vec<String> = row.iter().map(format_value).collect();
        output.push_str(&values.join("\t"));
        output.push('\n');
    }
    output
}

fn format_replica(replica: &ReplicaInfo) -> String {
    match &replica.address {
        Some(address) => format!("{} ({address})", replica.node),
        None => replica.node.to_string(),
    }
}

fn format_location(location: &Location) -> String {
    let primary = location
        .primary_replica()
        .map(format_replica)
        .or_else(|| location.primary.as_ref().map(ToString::to_string))
        .unwrap_or_else(|| "none".to_owned());
    let followers: Vec<String> = location.followers().map(format_replica).collect();
    let mut output = format!(
        "database: {}\nengine: {}\nepoch: {}\nreplication factor: {}\nacks: {}\n\
         min replicas: {}\nprimary: {primary}\nfollowers: {}\npositions:\n",
        shown(&location.database),
        location.engine,
        location.epoch.get(),
        location.replication_factor.get(),
        location.durability.acks(),
        location.durability.min_replicas(),
        if followers.is_empty() {
            "none".to_owned()
        } else {
            followers.join(", ")
        },
    );
    for replica in &location.replicas {
        let position = replica
            .position
            .as_ref()
            .map_or_else(|| "unknown".to_owned(), ToString::to_string);
        let _ = writeln!(
            output,
            "  {}: {position} ({})",
            replica.node,
            liveness(replica.liveness)
        );
    }
    output
}

/// The databases as a table sorted by name, then how many each node is primary for.
fn format_databases(databases: &[Location]) -> String {
    let mut rows: Vec<[String; 8]> = databases
        .iter()
        .map(|location| {
            let replicas: Vec<String> = location
                .replicas
                .iter()
                .map(|replica| replica.node.to_string())
                .collect();
            [
                shown(&location.database),
                location.engine.to_string(),
                location.epoch.get().to_string(),
                location.replicas.len().to_string(),
                location.durability.acks().to_string(),
                location.durability.min_replicas().to_string(),
                location
                    .primary
                    .as_ref()
                    .map_or_else(|| "none".to_owned(), ToString::to_string),
                replicas.join(","),
            ]
        })
        .collect();
    rows.sort();
    let header = [
        "NAME", "ENGINE", "EPOCH", "RF", "ACKS", "MIN", "PRIMARY", "REPLICAS",
    ];
    let mut widths: Vec<usize> = header.iter().map(|title| title.len()).collect();
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let mut output = String::new();
    let line = |output: &mut String, cells: &[&str]| {
        let mut columns = Vec::with_capacity(cells.len());
        for (index, cell) in cells.iter().enumerate() {
            let padding = widths[index].saturating_sub(cell.chars().count());
            // The last column isn't padded, so lines have no trailing spaces.
            if index + 1 == cells.len() {
                columns.push((*cell).to_owned());
            } else {
                columns.push(format!("{cell}{}", " ".repeat(padding)));
            }
        }
        output.push_str(columns.join("  ").trim_end());
        output.push('\n');
    };
    line(&mut output, &header);
    for row in &rows {
        let cells: Vec<&str> = row.iter().map(String::as_str).collect();
        line(&mut output, &cells);
    }
    // Every node in a replica set is counted, so a node with no primaries shows as 0.
    let mut primaries: BTreeMap<&NodeId, usize> = BTreeMap::new();
    for location in databases {
        for replica in &location.replicas {
            primaries.entry(&replica.node).or_default();
        }
        if let Some(primary) = &location.primary {
            *primaries.entry(primary).or_default() += 1;
        }
    }
    let summary: Vec<String> = primaries
        .iter()
        .map(|(node, count)| format!("{node}={count}"))
        .collect();
    let _ = writeln!(
        output,
        "{} database{}; primaries per node: {}",
        databases.len(),
        plural(u64::try_from(databases.len()).unwrap_or(u64::MAX)),
        if summary.is_empty() {
            "none".to_owned()
        } else {
            summary.join(" ")
        }
    );
    output
}

/// What a drain is about to move: the databases the node is primary for.
fn format_drain(id: &NodeId, primaries: &[DatabaseName]) -> String {
    let mut output = format!(
        "draining {id}: {} database{} to hand over",
        primaries.len(),
        plural(u64::try_from(primaries.len()).unwrap_or(u64::MAX))
    );
    if primaries.is_empty() {
        output.push('\n');
        return output;
    }
    output.push_str(":\n");
    for database in primaries {
        let _ = writeln!(output, "  {}", shown(database));
    }
    output
}

/// `status --node`'s text: the node, its epoch and ring, then what it serves, keeps and sends.
fn format_node_status(status: &NodeStatusReport) -> String {
    let mut output = format!(
        "node: {}\nmembership epoch: {}\nring fingerprint: {}\ndatabases:\n",
        printable(&status.node),
        status.membership_epoch,
        status.ring_fingerprint.as_deref().unwrap_or("none"),
    );
    if status.databases.is_empty() {
        output.push_str("  none\n");
    }
    for database in &status.databases {
        let _ = writeln!(
            output,
            "  {} {} epoch={} position={}",
            printable(&database.database),
            database.role,
            database.lease_epoch,
            database.position.as_deref().unwrap_or("unknown"),
        );
    }
    if !status.orphans.is_empty() {
        output.push_str("orphans:\n");
    }
    for orphan in &status.orphans {
        let _ = writeln!(
            output,
            "  {} epoch={} bytes={} (closed; `fleetring orphan remove` deletes it)",
            printable(&orphan.database),
            orphan.epoch,
            orphan.bytes,
        );
    }
    if !status.sinks.is_empty() {
        output.push_str("change sinks:\n");
    }
    for sink in &status.sinks {
        let _ = write!(
            output,
            "  {} {} position={} ({}) acknowledged={} backlog={} failures={} resets={}",
            sink.sink,
            printable(&sink.database),
            sink.position.as_deref().unwrap_or("none"),
            sink.mark,
            sink.acknowledged.as_deref().unwrap_or("none"),
            sink.backlog,
            sink.failures,
            sink.resets
        );
        if let Some(error) = &sink.last_error {
            let _ = write!(output, " last_error={error:?}");
        }
        output.push('\n');
    }
    output
}

fn liveness(liveness: Liveness) -> &'static str {
    liveness_name(liveness)
}

/// `status`'s text: dev mode, the epoch and ring, then one line per member.
fn format_status(status: &FleetStatus) -> String {
    let mut output = String::new();
    if status.dev_mode {
        output.push_str("dev mode: one node on loopback, for development only\n");
    }
    let _ = write!(
        output,
        "membership epoch: {}\nring fingerprint: {}\nmembers:\n",
        status.membership_epoch,
        status.ring_fingerprint.as_deref().unwrap_or("none"),
    );
    for member in &status.members {
        let _ = write!(
            output,
            "  {} {} weight={}",
            member.node, member.liveness, member.weight
        );
        if let Some(zone) = &member.zone {
            let _ = write!(output, " zone={zone}");
        }
        if let Some(host) = &member.host {
            let _ = write!(output, " host={host}");
        }
        if let Some(address) = &member.address {
            let _ = write!(output, " address={address}");
        }
        // A member that reports no software runs 0.3.0 (ADR-0038).
        let _ = write!(output, " version={}", member.release);
        output.push('\n');
    }
    output
}

/// What `db create` and `db restore` ask for: `replication_factor` replicas, or the fleet's
/// default when the flag is absent.
fn database_spec(replication_factor: Option<u8>) -> Result<DatabaseSpec, Box<dyn Error>> {
    match replication_factor {
        None => Ok(DatabaseSpec::default()),
        Some(factor) => ReplicationFactor::new(factor)
            .map(DatabaseSpec::new)
            .ok_or_else(|| "the replication factor must be at least 1".into()),
    }
}

/// Warns on stderr when a new database's replica set is smaller than its replication factor,
/// because the fleet has fewer data nodes. The creation stands: placement fills the replica set
/// as nodes join (ADR-0039).
fn warn_if_under_replicated(location: &Location) {
    if let Some(warning) = under_replicated(location) {
        note(format!("warning: {warning}"));
    }
}

/// The warning [`warn_if_under_replicated`] prints, if any.
fn under_replicated(location: &Location) -> Option<String> {
    let factor = location.replication_factor.get();
    let replicas = location.replicas.len();
    if replicas >= usize::from(factor) {
        return None;
    }
    let name = shown(&location.database);
    let floor = usize::from(location.durability.min_replicas());
    let nodes = if replicas == 1 { "node" } else { "nodes" };
    let consequence = if replicas < floor {
        let missing = floor - replicas;
        let verb = if missing == 1 { "joins" } else { "join" };
        format!(
            "its writes fail until {missing} more {verb}, or create it with a smaller \
             --replication-factor"
        )
    } else {
        let copies = if replicas == 1 { "replica" } else { "replicas" };
        format!("it has {replicas} {copies} until more join")
    };
    Some(format!(
        "{name} has a replication factor of {factor}, but the fleet has {replicas} data {nodes}: \
         {consequence}"
    ))
}

/// A name a principal chose, such as a database's, as text output prints it: control and
/// invisible characters escaped, so a name can't reorder, hide, or forge what is printed around
/// it (ADR-0041, amendment 5).
fn shown(name: &impl std::fmt::Display) -> String {
    printable(&name.to_string())
}

/// How many rows `db show` counts in each table before it says "more than" (ADR-0041, point 6).
const SCHEMA_COUNT_LIMIT: u64 = 10_000;

/// A failed call as one readable line, `<kind of failure>: <the server's message>`, with the
/// next step for the common failures; its gRPC code chooses the exit code.
fn failed(status: tonic::Status) -> Box<dyn Error> {
    Failed::new(&status).into()
}

/// The next step after a refusal of a creation or update with metadata for want of the
/// `metadata` or `expiry` fleet feature (ADR-0041, point 3), or `None` for any other status. A
/// fleet with a member that lacks the feature needs that member upgraded; a fleet that no member
/// has joined yet needs a data node first (ADR-0041, amendment 3).
fn metadata_hint(status: &tonic::Status) -> Option<&'static str> {
    if status.code() != tonic::Code::FailedPrecondition {
        return None;
    }
    let message = status.message();
    let gated = ["metadata", "expiry"]
        .iter()
        .any(|feature| message.contains(&format!("can't use '{feature}' yet")));
    if !gated {
        None
    } else if message.contains("yet: no member has joined") {
        Some(
            "start a data node first, or create it without --description, --label, and \
             --expires-in",
        )
    } else {
        Some(
            "create it without --description, --label, and --expires-in, or upgrade every \
             member first",
        )
    }
}

/// A failed creation or metadata update as [`failed`] says it, with the next step when the
/// fleet can't use the `metadata` feature yet.
fn metadata_failed(status: tonic::Status) -> Box<dyn Error> {
    match metadata_hint(&status) {
        Some(hint) => Failed::new(&status).with_hint(hint).into(),
        None => failed(status),
    }
}

/// A failed metadata update as [`failed`] says it; a conditional update whose version no
/// longer holds says how to read the metadata again.
fn updated_failed(status: tonic::Status, if_version: Option<u64>) -> Box<dyn Error> {
    match if_version {
        Some(version) if status.code() == tonic::Code::Aborted => format!(
            "the database's metadata changed since version {version}; read it again with \
             fleetring db show"
        )
        .into(),
        _ => metadata_failed(status),
    }
}

/// A database's labels as `key=value` pairs, or `none`.
fn format_labels(metadata: &MetadataView) -> String {
    if metadata.labels.is_empty() {
        return "none".to_owned();
    }
    metadata
        .labels
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// What a creation recorded, after its placement: the labels, the creator, the expiry, and the
/// version; `db show` prints the description.
fn format_metadata_summary(metadata: &MetadataView) -> String {
    format!(
        "labels: {}\ncreator: {}\nexpires: {}\nversion: {}\n",
        format_labels(metadata),
        metadata.created_by.as_deref().unwrap_or("unknown"),
        expiry_text(&metadata_report(metadata, SystemClock.now())),
        metadata.version
    )
}

/// The name the binary was run as, set once at the start of `main`.
static PROGRAM: OnceLock<ProgramName> = OnceLock::new();

/// The name errors start with: the one the binary was run as (ADR-0043, point 1.3).
fn program() -> &'static str {
    PROGRAM
        .get()
        .copied()
        .unwrap_or(ProgramName::Fleetring)
        .as_str()
}

/// `text` with the commands it names written as the binary was run: `fleet db list` under `fleet`
/// (ADR-0043, amendment of 2026-11-07).
fn hinted(text: &str) -> std::borrow::Cow<'_, str> {
    let name = PROGRAM.get().copied().unwrap_or(ProgramName::Fleetring);
    let root = <Cli as clap::CommandFactory>::command();
    name.in_hints(text, |word| root.find_subcommand(word).is_some())
}

/// Prints an error and its causes, one per line.
fn report(error: &dyn Error) {
    eprintln!("{}: {}", program(), hinted(&error.to_string()));
    let mut source = error.source();
    while let Some(cause) = source {
        eprintln!("  caused by: {}", hinted(&cause.to_string()));
        source = cause.source();
    }
}

/// How the process ends once the daemon has stopped.
#[derive(Debug)]
enum Stopped {
    /// It exits.
    Exit,
    /// It executes this binary again, restarting in place.
    Restart {
        /// The binary the daemon started from.
        exe: PathBuf,
        /// How many times the process had restarted in place before.
        restarts: u32,
    },
}

/// The signals that stop the daemon, listened for from the moment this exists: SIGHUP's default
/// action ends a process, so the listener must be in place before anyone learns the process ID
/// from the process file.
struct StopSignals {
    #[cfg(unix)]
    interrupt: Option<tokio::signal::unix::Signal>,
    #[cfg(unix)]
    terminate: Option<tokio::signal::unix::Signal>,
    #[cfg(unix)]
    hangup: Option<tokio::signal::unix::Signal>,
}

impl StopSignals {
    fn listen() -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let listen = |kind, name| match signal(kind) {
                Ok(signal) => Some(signal),
                Err(error) => {
                    tracing::error!(%error, "can't listen for {name}");
                    None
                }
            };
            Self {
                interrupt: listen(SignalKind::interrupt(), "SIGINT"),
                terminate: listen(SignalKind::terminate(), "SIGTERM"),
                hangup: listen(SignalKind::hangup(), "SIGHUP"),
            }
        }
        #[cfg(not(unix))]
        Self {}
    }

    /// Completes with [`Stop::AsConfigured`] on Ctrl-C or, on Unix, SIGTERM, and with
    /// [`Stop::Restart`] on SIGHUP, which restarts the daemon in place (ADR-0038).
    async fn next(self) -> Stop {
        #[cfg(unix)]
        {
            let Self {
                interrupt,
                terminate,
                hangup,
            } = self;
            let received = |signal: Option<tokio::signal::unix::Signal>| async move {
                match signal {
                    Some(mut signal) => {
                        signal.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                () = received(interrupt) => Stop::AsConfigured,
                () = received(terminate) => Stop::AsConfigured,
                () = received(hangup) => {
                    tracing::info!("SIGHUP: restarting in place");
                    Stop::Restart
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = self;
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(%error, "can't listen for Ctrl-C");
                std::future::pending::<()>().await;
            }
            Stop::AsConfigured
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::error::ErrorKind;

    use super::*;

    #[test]
    fn log_format_is_accepted_before_and_after_a_subcommand() {
        for words in [
            &[
                "fleetringd",
                "--log-format",
                "json",
                "status",
                "--control",
                "127.0.0.1:1",
            ][..],
            &[
                "fleetringd",
                "status",
                "--control",
                "127.0.0.1:1",
                "--log-format",
                "json",
            ],
            &[
                "fleetringd",
                "--log-format",
                "json",
                "serve",
                "--config",
                "fleetring.toml",
            ],
            &[
                "fleetringd",
                "serve",
                "--config",
                "fleetring.toml",
                "--log-format",
                "json",
            ],
        ] {
            let cli = Cli::try_parse_from(words).expect("the flag parses");
            assert_eq!(cli.log_format, Some(LogFormat::Json), "{words:?}");
        }
        let cli = Cli::try_parse_from(["fleetringd", "serve", "--config", "fleetring.toml"])
            .expect("no flag parses");
        assert_eq!(cli.log_format, None);
    }

    /// Parses `words` as `main` does, as the binary named by `words[0]`.
    fn parse_as_main(words: &[&str]) -> Result<(Cli, Invocation), clap::Error> {
        let raw: Vec<OsString> = words.iter().map(OsString::from).collect();
        let name = ProgramName::from_arg0(raw.first().map(OsString::as_os_str));
        parse(raw, name)
    }

    #[test]
    fn bare_daemon_flags_run_serve_under_either_name() {
        for program in [
            "fleetring",
            "fleetringd",
            "/usr/local/bin/fleetringd.previous",
        ] {
            for words in [
                &[program, "--config", "f.toml", "--node-id", "n1"][..],
                &[
                    program,
                    "--log-format",
                    "json",
                    "--config=f.toml",
                    "--node-id=n1",
                ],
                &[program, "--node-id", "n1", "--config", "f.toml"],
            ] {
                let (cli, invocation) = parse_as_main(words).expect("the 0.6 form parses");
                assert_eq!(invocation, Invocation::ServeImplied, "{words:?}");
                let Command::Serve(args) = cli.command else {
                    panic!("not serve: {:?}", cli.command);
                };
                assert_eq!(args.config, PathBuf::from("f.toml"), "{words:?}");
                assert_eq!(args.node_id.map(|id| id.to_string()).as_deref(), Some("n1"));
            }
            let (cli, invocation) =
                parse_as_main(&[program, "serve", "--config", "f.toml"]).expect("serve parses");
            assert_eq!(invocation, Invocation::AsWritten);
            assert!(
                matches!(cli.command, Command::Serve(_)),
                "{:?}",
                cli.command
            );
        }
    }

    #[test]
    fn daemon_options_are_refused_next_to_another_command() {
        for words in [
            &["fleetringd", "--config", "fleetring.toml", "status"][..],
            &["fleetringd", "--control", "127.0.0.1:7400", "status"],
            &[
                "fleetringd",
                "--insecure-plaintext",
                "db",
                "list",
                "--control",
                "127.0.0.1:1",
            ],
            &["fleetring", "serve", "--config", "fleetring.toml", "status"],
            &["fleetring", "serve"],
        ] {
            let refused = parse_as_main(words).expect_err("a daemon option and a command");
            assert_eq!(refused.exit_code(), 2, "{words:?}");
        }
        for words in [
            &["fleetringd", "status", "--control", "127.0.0.1:7400"][..],
            &[
                "fleetringd",
                "--log-format",
                "text",
                "status",
                "--control",
                "127.0.0.1:7400",
            ],
            &[
                "fleetringd",
                "--config",
                "fleetring.toml",
                "--log-format",
                "json",
            ],
        ] {
            assert!(parse_as_main(words).is_ok(), "{words:?}");
        }
    }

    #[test]
    fn no_command_prints_help_and_is_a_usage_error() {
        let refused = parse_as_main(&["fleetring"]).expect_err("a command is required");
        assert_eq!(
            refused.kind(),
            ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
        assert_eq!(refused.exit_code(), 2);
    }

    #[test]
    fn the_version_and_usage_name_the_program_not_its_file() {
        let version = env!("CARGO_PKG_VERSION");
        for (program, name) in [
            ("/usr/bin/fleetring", "fleetring"),
            ("/usr/bin/fleetringd", "fleetringd"),
            ("/usr/bin/fleetringd.previous", "fleetringd"),
            ("/usr/bin/.fleetringd.new", "fleetringd"),
            ("/usr/bin/fleetring.previous", "fleetring"),
        ] {
            let printed = parse_as_main(&[program, "--version"]).expect_err("--version exits");
            assert_eq!(printed.kind(), ErrorKind::DisplayVersion);
            assert_eq!(
                printed.to_string(),
                format!("{name} {version}\n"),
                "{program}"
            );
            let usage = parse_as_main(&[program, "--bogus"]).expect_err("an unknown flag");
            let text = usage.to_string();
            assert!(
                text.contains(&format!("Usage: {name} ")),
                "{program}: {text}"
            );
            assert!(
                !text.contains(".previous") && !text.contains(".new"),
                "{text}"
            );
            let nested = parse_as_main(&[program, "db", "list", "--bogus"]).expect_err("unknown");
            assert!(
                nested
                    .to_string()
                    .contains(&format!("Usage: {name} db list")),
                "{program}: {nested}"
            );
        }
    }

    #[test]
    fn dev_takes_a_directory_and_a_port_and_no_daemon_options() {
        let cli = Cli::try_parse_from(["fleetringd", "dev", "--data-dir", "d", "--port", "7410"])
            .expect("dev parses");
        let Command::Dev(args) = cli.command else {
            panic!("not dev: {:?}", cli.command);
        };
        assert_eq!(args.data_dir, Some(PathBuf::from("d")));
        assert_eq!(args.port, Some(7410));
        assert!(parse_as_main(&["fleetringd", "--listen", "0.0.0.0:7400", "dev"]).is_err());
        assert!(Cli::try_parse_from(["fleetringd", "dev", "--listen", "0.0.0.0:7400"]).is_err());
    }

    #[test]
    fn db_create_parses_labels_and_reads_a_description_from_stdin_when_dash() {
        let metadata = |words: &[&str]| {
            let cli = Cli::try_parse_from(words).expect("db create parses");
            let Command::Db(DbCommand::Create { metadata, .. }) = cli.command else {
                panic!("not db create: {:?}", cli.command);
            };
            metadata
        };
        let control = ["--control", "127.0.0.1:7400"];
        let words = [
            "fleetringd",
            "db",
            "create",
            "notes",
            "--description",
            "-",
            "--label",
            "purpose=memory",
            "--label",
            "agent=42",
        ];
        let read = metadata(&[&words[..], &control].concat())
            .read(&b"Memory for agent 42.\nTables: facts.\n\n"[..])
            .expect("valid metadata");
        assert_eq!(
            read.description.as_ref().map(Description::as_str),
            Some("Memory for agent 42.\nTables: facts.")
        );
        let labels: Vec<String> = read
            .labels
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        assert_eq!(labels, ["agent=42", "purpose=memory"]);

        let inline = metadata(
            &[
                &[
                    "fleetringd",
                    "db",
                    "create",
                    "notes",
                    "--description",
                    "inline",
                ][..],
                &control,
            ]
            .concat(),
        )
        .read(&b"never read"[..])
        .expect("valid");
        assert_eq!(
            inline.description.as_ref().map(Description::as_str),
            Some("inline")
        );
        let plain = metadata(&[&["fleetringd", "db", "create", "notes"][..], &control].concat())
            .read(&b""[..])
            .expect("nothing to read");
        assert!(plain.is_empty());

        let escape = metadata(
            &[
                &["fleetringd", "db", "create", "notes", "--description", "-"][..],
                &control,
            ]
            .concat(),
        )
        .read(&b"\x1b[2J"[..])
        .expect_err("an escape sequence");
        assert!(escape.to_string().contains("U+001B"), "{escape}");
        for bad in ["-bad=x", "fleetring.dev/x=y", "purpose"] {
            assert!(
                Cli::try_parse_from(
                    [
                        &["fleetringd", "db", "create", "notes", "--label", bad][..],
                        &control
                    ]
                    .concat()
                )
                .is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn a_refusal_for_want_of_metadata_says_what_to_do_next() {
        let unsupported = tonic::Status::failed_precondition(
            "the fleet can't use 'metadata' yet: node 'old' runs fleetringd 0.4.0, which doesn't \
             support it; upgrade every member first",
        );
        let hinted = metadata_failed(unsupported).to_string();
        assert!(
            hinted.ends_with(
                "upgrade every member first\n  hint: create it without --description, --label, \
                 and --expires-in, or upgrade every member first"
            ),
            "{hinted}"
        );
        // A fleet with metadata but a member without expiry refuses an expiry the same way.
        let no_expiry = tonic::Status::failed_precondition(
            "the fleet can't use 'expiry' yet: node 'old' runs fleetringd 0.5.0, which doesn't \
             support it; upgrade every member first",
        );
        assert!(
            metadata_failed(no_expiry)
                .to_string()
                .ends_with("and --expires-in, or upgrade every member first")
        );
        let empty = tonic::Status::failed_precondition(
            "the fleet can't use 'metadata' yet: no member has joined",
        );
        let hinted = metadata_failed(empty).to_string();
        assert!(
            hinted.ends_with(
                "no member has joined\n  hint: start a data node first, or create it without \
                 --description, --label, and --expires-in"
            ),
            "{hinted}"
        );
        let other = tonic::Status::failed_precondition("database notes has no primary yet");
        assert!(!metadata_failed(other).to_string().contains("hint:"));
        let denied = tonic::Status::permission_denied("can't use 'metadata' yet");
        assert_eq!(metadata_hint(&denied), None);
    }

    #[test]
    fn names_are_printed_with_invisible_characters_escaped() {
        let name: DatabaseName = "evil\u{202E}gnp\u{200B}.db\u{2028}x"
            .parse()
            .expect("a name the parser accepts");
        assert_eq!(shown(&name), "evil\\u{202E}gnp\\u{200B}.db\\u{2028}x");
        let status = tonic::Status::not_found("database 'a\u{202E}b' doesn't exist");
        let printed = failed(status).to_string();
        assert!(
            printed.ends_with("database 'a\\u{202E}b' doesn't exist"),
            "{printed}"
        );
    }

    #[test]
    fn orphan_remove_needs_a_node_and_orphan_list_prints_a_table_or_json() {
        let control = ["--control", "127.0.0.1:7400"];
        let remove = ["fleetringd", "orphan", "remove", "notes"];
        assert!(Cli::try_parse_from([&remove[..], &control[..]].concat()).is_err());
        let cli = Cli::try_parse_from([&remove[..], &["--node", "data-1"], &control[..]].concat())
            .expect("orphan remove parses");
        let Command::Orphan(OrphanCommand::Remove {
            name, node, yes, ..
        }) = cli.command
        else {
            panic!("not orphan remove: {:?}", cli.command);
        };
        assert_eq!(name.as_str(), "notes");
        assert_eq!(node.map(|node| node.to_string()).as_deref(), Some("data-1"));
        assert!(!yes);
        assert!(
            Cli::try_parse_from(["fleetringd", "orphan", "list", "--json", "--control", "x:1"])
                .is_ok()
        );

        assert_eq!(format_orphans(&[]), "no orphans\n");
        assert_eq!(
            serde_json::to_value(OrphanList {
                orphans: Vec::new()
            })
            .expect("serializes"),
            serde_json::json!({ "orphans": [] })
        );
    }

    #[test]
    fn db_show_takes_json_as_a_flag_or_an_output_format() {
        let format = |words: &[&str]| {
            let cli = Cli::try_parse_from(words).expect("db show parses");
            let Command::Db(DbCommand::Show { output, .. }) = cli.command else {
                panic!("not db show: {:?}", cli.command);
            };
            output.format()
        };
        let show = [
            "fleetringd",
            "db",
            "show",
            "notes",
            "--control",
            "127.0.0.1:7400",
        ];
        assert_eq!(format(&show), OutputFormat::Table);
        assert_eq!(
            format(&[&show[..], &["--json"]].concat()),
            OutputFormat::Json
        );
        assert_eq!(
            format(&[&show[..], &["-o", "json"]].concat()),
            OutputFormat::Json
        );
        assert!(Cli::try_parse_from([&show[..], &["-o", "yaml"]].concat()).is_err());
    }

    #[test]
    fn db_show_counts_ten_thousand_rows_unless_told_otherwise() {
        let parsed = |extra: &[&str]| {
            let words = [
                &[
                    "fleetringd",
                    "db",
                    "show",
                    "notes",
                    "--control",
                    "127.0.0.1:7400",
                ][..],
                extra,
            ]
            .concat();
            let cli = Cli::try_parse_from(words).expect("db show parses");
            let Command::Db(DbCommand::Show {
                no_schema,
                count_limit,
                all,
                ..
            }) = cli.command
            else {
                panic!("not db show: {:?}", cli.command);
            };
            (no_schema, count_limit, all)
        };
        assert_eq!(parsed(&[]), (false, 10_000, false));
        assert_eq!(
            parsed(&["--no-schema", "--count-limit", "0", "--all"]),
            (true, 0, true)
        );
        assert_eq!(parsed(&["--count-limit", "250"]), (false, 250, false));
        assert_eq!(
            parsed(&["--count-limit", "1000000"]),
            (false, 1_000_000, false)
        );
        let over = Cli::try_parse_from([
            "fleetringd",
            "db",
            "show",
            "notes",
            "--control",
            "127.0.0.1:7400",
            "--count-limit",
            "1000001",
        ]);
        assert!(over.is_err(), "more than a request can count is refused");
    }

    #[test]
    fn only_a_primary_that_cant_answer_now_leaves_db_show_without_a_schema() {
        use tonic::Status;

        let older = Status::unimplemented("the primary runs an older fleetringd");
        assert_eq!(
            schema_unavailable(&older).as_deref(),
            Some("the primary runs an older fleetringd")
        );
        for status in [
            Status::unavailable("database notes is failing over to epoch 3; retry"),
            Status::failed_precondition("database notes has no primary yet"),
        ] {
            assert!(schema_unavailable(&status).is_some(), "{status}");
        }
        for status in [
            Status::permission_denied("Schema needs read on 'notes'"),
            Status::not_found("database notes doesn't exist"),
            Status::internal("storage failed"),
        ] {
            assert_eq!(schema_unavailable(&status), None, "{:?}", status.code());
        }
    }

    #[test]
    fn db_create_and_restore_take_an_expiry_in_one_form() {
        let control = ["--control", "127.0.0.1:7400"];
        let create = |flags: &[&str]| {
            Cli::try_parse_from(
                [
                    &["fleetringd", "db", "create", "scratch"][..],
                    flags,
                    &control,
                ]
                .concat(),
            )
        };
        let expiry = |flags: &[&str]| {
            let cli = create(flags).expect("db create parses");
            let Command::Db(DbCommand::Create { metadata, .. }) = cli.command else {
                panic!("not db create: {:?}", cli.command);
            };
            metadata.read(&b""[..]).expect("valid").expiry
        };
        assert_eq!(
            expiry(&["--expires-in", "90m"]),
            Some(Expiry::In(Duration::from_secs(5_400)))
        );
        assert_eq!(
            expiry(&["--expires-at", "1970-01-01T00:01:00Z"]),
            Some(Expiry::At(Timestamp::from_millis(60_000)))
        );
        assert_eq!(expiry(&[]), None);
        assert!(
            create(&["--expires-in", "1h", "--expires-at", "2026-11-05T10:00:00Z"]).is_err(),
            "one form only"
        );
        let restore = Cli::try_parse_from(
            [
                &[
                    "fleetringd",
                    "db",
                    "restore",
                    "copy",
                    "--archive",
                    "backups",
                    "--from",
                    "notes",
                    "--expires-in",
                    "1m",
                ][..],
                &control,
            ]
            .concat(),
        )
        .expect("db restore parses");
        let Command::Db(DbCommand::Restore { metadata, .. }) = restore.command else {
            panic!("not db restore");
        };
        assert_eq!(
            metadata.read(&b""[..]).expect("valid").expiry,
            Some(Expiry::In(Duration::from_secs(60)))
        );
    }

    #[test]
    fn db_create_leaves_the_replication_factor_to_the_fleet_unless_given() {
        let factor = |words: &[&str]| {
            let cli = Cli::try_parse_from(words).expect("db create parses");
            let Command::Db(DbCommand::Create {
                replication_factor, ..
            }) = cli.command
            else {
                panic!("not db create: {:?}", cli.command);
            };
            replication_factor
        };
        let control = ["--control", "127.0.0.1:7400"];
        let unset = factor(&[&["fleetringd", "db", "create", "notes"][..], &control].concat());
        assert_eq!(unset, None);
        let two = factor(
            &[
                &[
                    "fleetringd",
                    "db",
                    "create",
                    "notes",
                    "--replication-factor",
                    "2",
                ][..],
                &control,
            ]
            .concat(),
        );
        assert_eq!(two, Some(2));

        assert_eq!(database_spec(None).ok(), Some(DatabaseSpec::default()));
        let two = ReplicationFactor::new(2).expect("nonzero");
        assert_eq!(database_spec(Some(2)).ok(), Some(DatabaseSpec::new(two)));
        let zero = database_spec(Some(0)).expect_err("a factor of 0");
        assert_eq!(
            zero.to_string(),
            "the replication factor must be at least 1"
        );
    }

    #[test]
    fn an_unknown_log_format_is_refused() {
        let refused = Cli::try_parse_from(["fleetringd", "--log-format", "yaml", "status"])
            .expect_err("yaml isn't a format");
        assert!(
            refused.to_string().contains("use text or json"),
            "{refused}"
        );
    }

    #[test]
    fn the_flag_beats_the_variable_which_beats_the_config_file() {
        let config = Config::from_toml(
            "data_dir = \"/tmp/fleetring-test\"\nlisten = \"127.0.0.1:7400\"\n\n\
             [node]\nid = \"n1\"\n\n[log]\nformat = \"json\"\n",
        )
        .expect("a config");
        assert_eq!(log_format(None, None, None), Ok(LogFormat::Text));
        assert_eq!(log_format(None, None, Some(&config)), Ok(LogFormat::Json));
        assert_eq!(
            log_format(None, Some("text"), Some(&config)),
            Ok(LogFormat::Text)
        );
        assert_eq!(
            log_format(Some(LogFormat::Json), Some("text"), None),
            Ok(LogFormat::Json)
        );
        assert_eq!(log_format(None, Some("xml"), None), Err("xml".to_owned()));
    }

    #[test]
    fn parameters_infer_their_type_unless_prefixed() {
        let cases = [
            ("42", Value::Integer(42)),
            ("-7", Value::Integer(-7)),
            ("2.5", Value::Real(2.5)),
            ("1e3", Value::Real(1000.0)),
            ("null", Value::Null),
            ("NULL", Value::Null),
            ("hello", Value::Text("hello".into())),
            ("inf", Value::Text("inf".into())),
            ("text:42", Value::Text("42".into())),
            ("text:", Value::Text(String::new())),
            ("int:9", Value::Integer(9)),
            ("real:3", Value::Real(3.0)),
            ("blob:00ff", Value::Blob(vec![0, 255])),
            ("blob:", Value::Blob(Vec::new())),
        ];
        for (text, value) in cases {
            assert_eq!(parse_param(text), Ok(value), "{text}");
        }
        for bad in ["int:x", "real:", "blob:0", "blob:zz"] {
            assert!(parse_param(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn context_flags_parse_and_conflict_as_documented() {
        let parses = |words: &[&str]| Cli::try_parse_from([&["fleetring"][..], words].concat());
        for words in [
            &["db", "list", "--context", "home"][..],
            &[
                "status",
                "--context",
                "home",
                "--tls-dir",
                "/t",
                "--as",
                "ops",
            ],
            &["status", "--token-file", "-"],
            &["-q", "status"],
            &["status", "-q"],
            &["switch"],
            &["switch", "-"],
            &["switch", "home"],
            &["context", "current"],
            &["context", "list", "--json"],
            &[
                "context",
                "add",
                "home",
                "--control",
                "a:1",
                "--control",
                "b:1",
                "--tls-dir",
                "/t",
            ],
        ] {
            assert!(
                parses(words).is_ok(),
                "{words:?}: {:?}",
                parses(words).err()
            );
        }
        for words in [
            &["status", "--token", "t", "--token-file", "f"][..],
            &["status", "--insecure-plaintext", "--tls-dir", "/t"],
            &["status", "--context", "Not A Name"],
            &["context", "add", "home"],
            &[
                "context",
                "add",
                "home",
                "--control",
                "a:1",
                "--tls-dir",
                "/t",
                "--cert",
                "c",
            ],
            &["context", "add", "home", "--control", "a:1", "--ca", "ca"],
            &["context", "use"],
        ] {
            assert!(parses(words).is_err(), "{words:?} should be refused");
        }
    }

    #[test]
    fn a_token_or_key_id_may_start_with_a_hyphen() {
        // IDs are base64url, so about one in 64 starts with `-`; clap would read it as a flag.
        for (flag, id) in [("--token-id", "-0abc_"), ("--key-id", "-0abc_")] {
            let parsed = Cli::try_parse_from(["fleetringd", "token", "revoke", flag, id]);
            assert!(parsed.is_ok(), "{flag} {id}: {:?}", parsed.err());
        }
    }

    #[test]
    fn node_takes_an_id_and_address_takes_an_address() {
        let parses = |words: &[&str]| Cli::try_parse_from([&["fleetringd"][..], words].concat());
        for words in [
            &["status", "--node", "node-2", "--control", "c:1"][..],
            &["status", "--address", "10.0.0.2:7400"],
            &["lease", "watch", "--node", "node-2", "--control", "c:1"],
            &["lease", "watch", "--address", "10.0.0.2:7400"],
            &["db", "exec", "notes", "DELETE FROM t", "--node", "node-2"],
            &[
                "db",
                "query",
                "notes",
                "SELECT 1",
                "--address",
                "10.0.0.2:7400",
            ],
            &[
                "cert",
                "node",
                "node-2",
                "--addresses",
                "10.0.0.2,n2.lan",
                "--dir",
                ".",
            ],
            &["cert", "init", "--fleet", "home", "--dir", "."],
        ] {
            assert!(
                parses(words).is_ok(),
                "{words:?}: {:?}",
                parses(words).err()
            );
        }
        for words in [
            &["status", "--node", "node-2", "--address", "10.0.0.2:7400"][..],
            &["status", "--address", "10.0.0.2:7400", "--control", "c:1"],
            &["lease", "watch"],
            &[
                "lease",
                "watch",
                "--address",
                "10.0.0.2:7400",
                "--config",
                "f.toml",
            ],
            &[
                "db",
                "exec",
                "notes",
                "x",
                "--node",
                "a",
                "--address",
                "b:1",
            ],
            // 0.2's spellings.
            &["watch-leases", "--node", "10.0.0.2:7400"],
            &[
                "cert", "node", "node-2", "--hosts", "10.0.0.2", "--dir", ".",
            ],
            &["cert", "init", "--cluster", "home", "--dir", "."],
        ] {
            assert!(parses(words).is_err(), "{words:?} should be refused");
        }
    }

    #[test]
    fn lifetimes_parse_in_days_hours_minutes_and_seconds() {
        assert_eq!(parse_ttl("30d"), Ok(Duration::from_secs(30 * 24 * 60 * 60)));
        assert_eq!(parse_ttl("2h"), Ok(Duration::from_secs(2 * 60 * 60)));
        assert_eq!(parse_ttl("5m"), Ok(Duration::from_secs(300)));
        assert_eq!(parse_ttl("45s"), Ok(Duration::from_secs(45)));
        for bad in ["", "d", "30", "30x", "0d", "-1d"] {
            assert!(parse_ttl(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn values_print_as_the_command_line_writes_them() {
        assert_eq!(format_value(&Value::Null), "NULL");
        assert_eq!(format_value(&Value::Blob(vec![1, 171])), "x'01ab'");
        assert_eq!(format_value(&Value::Real(0.5)), "0.5");
    }

    #[test]
    fn lease_updates_print_one_line_per_lease_or_event() {
        use fleetring_control::LeaseEpoch;
        use fleetringd::replication::{LeaseChange, LeaseEvent};

        let notes: DatabaseName = "notes".parse().expect("name");
        let snapshot = LeaseUpdate::Held {
            node: "a".parse().expect("id"),
            leases: BTreeMap::from([(notes.clone(), LeaseEpoch::new(2))]),
        };
        assert_eq!(format_lease_update(&snapshot), "held notes epoch 2\n");
        let lost = LeaseUpdate::Event(LeaseEvent::new(
            notes,
            LeaseEpoch::new(2),
            LeaseChange::Lost,
        ));
        assert_eq!(format_lease_update(&lost), "lost notes epoch 2\n");
    }
}
