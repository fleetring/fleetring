//! The `fleetringd` binary: the daemon, and the commands that talk to a running fleet.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write as _;
use std::io::{BufRead as _, IsTerminal as _, Write as _};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use clap::{Args, Parser, Subcommand};
use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{Acks, DatabaseName, Liveness, ReplicaRole};
use fleetring_control::{KeyId, Millis, Timestamp, TokenId};
use fleetring_proto::v1::revocation::Revoked;
use fleetring_proto::v1::{Revocation, RevokeTokenRequest};
use fleetringd::auth::{BearerToken, DEFAULT_TOKEN_LIFETIME, Grant, MAX_TOKEN_LIFETIME, Scope};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::data::{
    Client, DatabaseSpec, LeaseUpdate, Location, NodeStatus, ReplicaInfo, Verification,
};
use fleetringd::engine::{Position, Rows, Statement, Value};
use fleetringd::replication::ChainHash;
use fleetringd::tls::{
    Authority, DEFAULT_AUTHORITY_LIFETIME, DEFAULT_CLIENT_LIFETIME, DEFAULT_NODE_LIFETIME,
    FleetName, Identity, Principal, Security, Validity,
};
use fleetringd::transport::Credentials;
use fleetringd::{Config, Daemon, MembershipView, Network, NodeRole, Plaintext};
use ring::rand::{SecureRandom as _, SystemRandom};
use tracing_subscriber::EnvFilter;

/// The environment variable that names a directory with `ca.crt`, `client.crt`, and
/// `client.key`, used when `--ca`, `--cert`, and `--key` aren't given.
const TLS_DIR_VAR: &str = "FLEETRING_TLS_DIR";

/// The environment variable that holds a bearer token, used when `--token` isn't given.
const TOKEN_VAR: &str = "FLEETRING_TOKEN";

/// How many random bytes a token's ID holds.
const TOKEN_ID_BYTES: usize = 16;

/// Places, replicates, and fails over SQLite databases across machines you own.
///
/// With `--config`, runs the daemon. The subcommands talk to a running fleet.
#[derive(Debug, Parser)]
#[command(
    version,
    args_conflicts_with_subcommands = true,
    subcommand_negates_reqs = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[command(flatten)]
    daemon: DaemonArgs,
}

/// Runs the daemon.
#[derive(Debug, Args)]
struct DaemonArgs {
    /// The TOML configuration file.
    #[arg(long, required = true)]
    config: Option<PathBuf>,
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
    /// Runs without a `[tls]` table, in plaintext: nothing on the wire is authenticated or
    /// encrypted. For local experiments only.
    #[arg(long)]
    insecure_plaintext: bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Creates the fleet's certificate authority and issues node and client certificates.
    #[command(subcommand)]
    Cert(CertCommand),
    /// Issues and revokes bearer tokens: signed, expiring credentials that name a principal and
    /// carry its scopes.
    #[command(subcommand)]
    Token(TokenCommand),
    /// Creates, lists, writes, reads, locates, and drops databases.
    #[command(subcommand)]
    Db(DbCommand),
    /// Drains a node.
    #[command(subcommand)]
    Node(NodeCommand),
    /// Follows the leases data nodes hold.
    #[command(subcommand)]
    Lease(LeaseCommand),
    /// Shows the members with their liveness, the membership epoch, and the ring fingerprint, as
    /// the control plane sees them; with `--node` or `--address`, as one data node sees them.
    Status {
        /// Asks this data node instead of the control plane: its membership epoch, ring
        /// fingerprint, and the databases it serves. Its address comes from the control plane's
        /// membership, so `--control` or `--config` is needed too.
        #[arg(long, conflicts_with = "address")]
        node: Option<NodeId>,
        /// Asks the data node at this address, `host:port`, instead of the control plane. Nothing
        /// else is needed.
        #[arg(long, conflicts_with_all = ["control", "config"])]
        address: Option<String>,
        #[command(flatten)]
        target: Target,
    },
}

#[derive(Debug, Subcommand)]
enum LeaseCommand {
    /// Follows the leases one data node holds: first `held <database> epoch <n>` for each
    /// database it serves, then `gained` or `lost` lines as leases move, until the stream ends.
    Watch {
        /// The data node to follow. Its address comes from the control plane's membership, so
        /// `--control` or `--config` is needed too.
        #[arg(long, required_unless_present = "address", conflicts_with = "address")]
        node: Option<NodeId>,
        /// The address of the data node to follow, `host:port`. Nothing else is needed.
        #[arg(long, conflicts_with_all = ["control", "config"])]
        address: Option<String>,
        #[command(flatten)]
        target: Target,
    },
}

#[derive(Debug, Subcommand)]
enum TokenCommand {
    /// Issues a token signed with the authority's key and prints it. Run it where `ca.key` is,
    /// off the nodes; nodes verify the token with `ca.crt` alone.
    Issue {
        /// The principal the token names. A client sending it must connect with a certificate
        /// for the same principal, unless that principal forwards tokens.
        #[arg(long)]
        principal: Principal,
        /// A scope the token carries: `<action>:<pattern>`, with the action `read`, `write`,
        /// `admin`, or `stream` and the pattern a database name or a prefix ending in `*`; or
        /// `fleet`. Repeat it for more.
        #[arg(long = "scope", required = true)]
        scopes: Vec<Scope>,
        /// How long the token lasts: `<n>d`, `<n>h`, `<n>m`, or `<n>s`; one hour by default, 30
        /// days at most.
        #[arg(long, value_parser = parse_ttl)]
        ttl: Option<Duration>,
        /// The directory with `ca.crt` and `ca.key`.
        #[arg(long)]
        dir: PathBuf,
    },
    /// Revokes a token by its ID, or every token a signing key issued. Every node refuses it
    /// within one heartbeat interval.
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
        /// A signing key's ID (the token header's `kid`): revokes every token that key signed,
        /// for good.
        #[arg(long, allow_hyphen_values = true)]
        key_id: Option<KeyId>,
        /// How long to keep a token's revocation, at least what remains of its lifetime: 30 days
        /// (the longest a token lasts) by default.
        #[arg(long, value_parser = parse_ttl, conflicts_with = "key_id")]
        ttl: Option<Duration>,
        #[command(flatten)]
        target: Target,
    },
}

#[derive(Debug, Subcommand)]
enum CertCommand {
    /// Creates the authority: `ca.crt` and `ca.key` in the directory. Keep `ca.key` off every
    /// node; it is the one secret that issues certificates.
    Init {
        /// The fleet's name, which every certificate the authority issues names.
        #[arg(long)]
        fleet: FleetName,
        /// The directory to write into; created if missing.
        #[arg(long)]
        dir: PathBuf,
        /// How long the authority's certificate lasts: `<n>d`, `<n>h`, `<n>m`, or `<n>s`; ten
        /// years by default.
        #[arg(long, value_parser = parse_ttl)]
        ttl: Option<Duration>,
    },
    /// Issues a node certificate: `node.<id>.crt` and `node.<id>.key` in the directory, naming
    /// the node's ID and the addresses it is reached at.
    Node {
        /// The node's ID, as `[node] id` in its configuration.
        id: NodeId,
        /// The IP addresses and DNS names peers and clients dial the node at, comma-separated.
        /// Not the `[node] host` placement label.
        #[arg(long, value_delimiter = ',', required = true)]
        addresses: Vec<String>,
        /// The directory with `ca.crt` and `ca.key`, which the new files go into.
        #[arg(long)]
        dir: PathBuf,
        /// How long the certificate lasts: `<n>d`, `<n>h`, `<n>m`, or `<n>s`; 30 days by
        /// default.
        #[arg(long, value_parser = parse_ttl)]
        ttl: Option<Duration>,
    },
    /// Issues a client certificate: `client.<principal>.crt` and `client.<principal>.key` in
    /// the directory, naming the principal the client acts as.
    Client {
        /// The principal's name.
        principal: Principal,
        /// The directory with `ca.crt` and `ca.key`, which the new files go into.
        #[arg(long)]
        dir: PathBuf,
        /// How long the certificate lasts: `<n>d`, `<n>h`, `<n>m`, or `<n>s`; 7 days by
        /// default.
        #[arg(long, value_parser = parse_ttl)]
        ttl: Option<Duration>,
    },
}

#[derive(Debug, Subcommand)]
enum DbCommand {
    /// Creates a SQLite database, places it with the ring, and opens it on its primary.
    Create {
        /// The database's name.
        name: DatabaseName,
        /// How many replicas it has, the primary included.
        #[arg(long, default_value_t = 3)]
        replication_factor: u8,
        /// How many replicas must store a write before it is acknowledged: `quorum` (a
        /// majority), `all` (every replica), or `local` (the primary alone; a failover may lose
        /// acknowledged writes). The fleet's default if unset.
        #[arg(long)]
        acks: Option<Acks>,
        /// How many replicas, the primary included, must be reachable for a write to run;
        /// below it, writes are refused before anything commits. Between the write quorum and
        /// the replication factor; the fleet's default if unset, which is the write quorum
        /// unless the control node's configuration says otherwise.
        #[arg(long)]
        min_replicas: Option<u8>,
        #[command(flatten)]
        target: Target,
    },
    /// Changes a database's durability settings, which take effect at its next epoch: the
    /// current primary keeps the settings it holds the lease under.
    SetDurability {
        /// The database's name.
        name: DatabaseName,
        /// The acknowledgement mode: `quorum`, `all`, or `local`.
        #[arg(long)]
        acks: Acks,
        /// The floor on reachable replicas; the mode's write quorum if unset.
        #[arg(long)]
        min_replicas: Option<u8>,
        #[command(flatten)]
        target: Target,
    },
    /// Runs one write statement on the database's primary.
    Exec(SqlArgs),
    /// Runs one read-only query on the database's primary.
    Query(SqlArgs),
    /// Shows the database's primary, followers, epoch, durability settings, and replica
    /// positions.
    Locate {
        /// The database's name.
        name: DatabaseName,
        #[command(flatten)]
        target: Target,
    },
    /// Lists every database with its engine, epoch, replication factor, durability settings,
    /// primary, and replicas.
    List {
        /// Lists only the databases this node is primary for.
        #[arg(long)]
        node: Option<NodeId>,
        #[command(flatten)]
        target: Target,
    },
    /// Asks every replica of a database for its hash chain at one position, the latest they all
    /// hold unless `--at` names one, and reports whether they agree. Each replica recomputes
    /// the hash from its log on disk, so a copy that changed since it was written is named.
    Verify {
        /// The database's name.
        name: DatabaseName,
        /// The position to verify at, as `db locate` prints it, instead of the latest every
        /// replica holds.
        #[arg(long, value_parser = parse_position)]
        at: Option<Position>,
        /// A hash recorded earlier at that position: the command also reports whether the log
        /// still hashes to it, that is, whether nothing changed since.
        #[arg(long)]
        expect: Option<ChainHash>,
        #[command(flatten)]
        target: Target,
    },
    /// Drops a database for good: every replica deletes its copy.
    Drop {
        /// The database's name.
        name: DatabaseName,
        /// Drops without asking. Without it, the command asks first on a terminal, and refuses
        /// elsewhere.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        target: Target,
    },
}

#[derive(Debug, Subcommand)]
enum NodeCommand {
    /// Hands every database the node is primary for to a follower, without waiting for a
    /// failover. The node gets no new primaries afterwards, and stays a follower until it stops
    /// or restarts; stopping it then costs clients only a redirect.
    Drain {
        /// The node's ID.
        id: NodeId,
        #[command(flatten)]
        target: Target,
    },
}

#[derive(Debug, Args)]
struct SqlArgs {
    /// The database's name.
    name: DatabaseName,
    /// One SQL statement, with `?` for each parameter.
    sql: String,
    /// The parameters, in order: `null`, an integer, a decimal number, or text. Prefix one with
    /// `text:`, `int:`, `real:`, or `blob:` (hex digits) to choose its type.
    #[arg(allow_negative_numbers = true, value_parser = parse_param)]
    params: Vec<Value>,
    /// Sends the request to this data node instead of the primary, and follows its redirect.
    /// Its address comes from the control plane's membership.
    #[arg(long, conflicts_with = "address")]
    node: Option<NodeId>,
    /// Sends the request to the data node at this address, `host:port`, instead of the
    /// primary, and follows its redirect.
    #[arg(long)]
    address: Option<String>,
    #[command(flatten)]
    target: Target,
}

/// How a command finds the control plane. Commands talk to the control node, which knows every
/// database's placement; data nodes redirect database calls to the right primary on their own.
#[derive(Debug, Args)]
struct Target {
    /// The control node's address, `host:port`: the node that runs the control role, not any node
    /// in the fleet. A data-only node answers with the control node's address.
    #[arg(long, conflicts_with = "config")]
    control: Option<String>,
    /// A daemon's configuration file, to take the control node's address from.
    #[arg(long)]
    config: Option<PathBuf>,
    #[command(flatten)]
    tls: TlsArgs,
}

/// The identity a command connects with: a client certificate from `cert client`.
#[derive(Debug, Args)]
struct TlsArgs {
    /// The trusted authority certificates, PEM. Defaults to `ca.crt` under `FLEETRING_TLS_DIR`.
    #[arg(long)]
    ca: Option<PathBuf>,
    /// The client certificate, PEM. Defaults to `client.crt` under `FLEETRING_TLS_DIR`.
    #[arg(long)]
    cert: Option<PathBuf>,
    /// The client certificate's private key, PEM. Defaults to `client.key` under
    /// `FLEETRING_TLS_DIR`.
    #[arg(long)]
    key: Option<PathBuf>,
    /// Connects in plaintext, to a daemon started with `--insecure-plaintext`.
    #[arg(long, conflicts_with_all = ["ca", "cert", "key"])]
    insecure_plaintext: bool,
    /// A bearer token from `token issue`, sent with every request; the request then acts with
    /// the token's scopes. Defaults to `FLEETRING_TOKEN`, if set.
    #[arg(long)]
    token: Option<String>,
}

impl TlsArgs {
    fn security(&self) -> Result<Security, Box<dyn Error>> {
        if self.insecure_plaintext {
            return Ok(Security::Plaintext);
        }
        let dir = std::env::var_os(TLS_DIR_VAR).map(PathBuf::from);
        let path =
            |given: &Option<PathBuf>, name: &str, flag: &str| -> Result<PathBuf, Box<dyn Error>> {
                match (given, &dir) {
                    (Some(path), _) => Ok(path.clone()),
                    (None, Some(dir)) => Ok(dir.join(name)),
                    (None, None) => Err(format!(
                        "pass --{flag} (or set {TLS_DIR_VAR} to a directory with ca.crt, \
                     client.crt, and client.key), or --insecure-plaintext"
                    )
                    .into()),
                }
            };
        let ca = path(&self.ca, "ca.crt", "ca")?;
        let cert = path(&self.cert, "client.crt", "cert")?;
        let key = path(&self.key, "client.key", "key")?;
        Ok(Security::Tls(Identity::load(&ca, &cert, &key)?))
    }

    fn network(&self) -> Result<Network, Box<dyn Error>> {
        let network = Network::default().with_security(self.security()?);
        let token = match &self.token {
            Some(token) => Some(token.clone()),
            None => std::env::var(TOKEN_VAR)
                .ok()
                .filter(|token| !token.is_empty()),
        };
        Ok(match token {
            Some(token) => {
                network.with_credentials(Credentials::bearer(&BearerToken::new(token.trim()))?)
            }
            None => network,
        })
    }
}

impl Target {
    fn client(&self) -> Result<Client, Box<dyn Error>> {
        let control = match (&self.control, &self.config) {
            (Some(control), _) => control.clone(),
            (None, Some(path)) => Config::load(path)?
                .control_target()
                .ok_or("the configuration names no control address; pass --control")?
                .to_string(),
            (None, None) => return Err("pass --control or --config".into()),
        };
        Ok(Client::new(control, self.tls.network()?))
    }
}

impl DaemonArgs {
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

fn main() -> ExitCode {
    let cli = Cli::parse();
    // The daemon logs at info; a command prints its result and logs only warnings.
    let default_filter = if cli.command.is_some() {
        "warn"
    } else {
        "info"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| default_filter.into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            report(&error);
            return ExitCode::FAILURE;
        }
    };
    let result = match cli.command {
        Some(command) => runtime.block_on(run_command(command)),
        None => runtime.block_on(run_daemon(cli.daemon)),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            report(error.as_ref());
            ExitCode::FAILURE
        }
    }
}

async fn run_daemon(args: DaemonArgs) -> Result<(), Box<dyn Error>> {
    let path = args.config.clone().ok_or("--config is required")?;
    let config = args.apply(Config::load(&path)?);
    let daemon = Daemon::start(config).await?;
    daemon.run_until(shutdown_signal()).await;
    Ok(())
}

async fn run_command(command: Command) -> Result<(), Box<dyn Error>> {
    let output = match command {
        Command::Cert(command) => run_cert(command)?,
        Command::Token(command) => run_token(command).await?,
        Command::Status {
            node,
            address,
            target,
        } => match node_address(node.as_ref(), address, &target).await? {
            Some(address) => {
                // The node answers on its own, about itself.
                let client = Client::new(address.clone(), target.tls.network()?);
                format_node_status(&client.node_status(&address).await.map_err(failed)?)
            }
            None => format_status(&target.client()?.status().await.map_err(failed)?),
        },
        Command::Db(DbCommand::Create {
            name,
            replication_factor,
            acks,
            min_replicas,
            target,
        }) => {
            let replication_factor = ReplicationFactor::new(replication_factor)
                .ok_or("the replication factor must be at least 1")?;
            let mut spec = DatabaseSpec::new(replication_factor);
            if let Some(acks) = acks {
                spec = spec.with_acks(acks);
            }
            if let Some(min_replicas) = min_replicas {
                spec = spec.with_min_replicas(min_replicas);
            }
            let location = target
                .client()?
                .create_database_with(&name, spec)
                .await
                .map_err(failed)?;
            format!("created {name}\n{}", format_location(&location))
        }
        Command::Db(DbCommand::SetDurability {
            name,
            acks,
            min_replicas,
            target,
        }) => {
            let change = target
                .client()?
                .change_durability(&name, acks, min_replicas)
                .await
                .map_err(failed)?;
            format!(
                "durability of {name} changes to acks={} min_replicas={} at its next epoch; \
                 now acks={} min_replicas={}\n",
                change.acks, change.min_replicas, change.current_acks, change.current_min_replicas
            )
        }
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
        Command::Db(DbCommand::Query(args)) => {
            let (client, name, statement) = args.into_parts().await?;
            format_rows(&client.query(&name, statement).await.map_err(failed)?)
        }
        Command::Db(DbCommand::Locate { name, target }) => {
            format_location(&target.client()?.locate(&name).await.map_err(failed)?)
        }
        Command::Db(DbCommand::List { node, target }) => {
            let mut databases = target.client()?.list_databases().await.map_err(failed)?;
            if let Some(node) = &node {
                databases.retain(|location| location.primary.as_ref() == Some(node));
            }
            format_databases(&databases)
        }
        Command::Db(DbCommand::Verify {
            name,
            at,
            expect,
            target,
        }) => verify(&target.client()?, &name, at, expect).await?,
        Command::Db(DbCommand::Drop { name, yes, target }) => {
            if !yes {
                confirm_drop(&name)?;
            }
            let epoch = target
                .client()?
                .drop_database(&name)
                .await
                .map_err(failed)?;
            format!("dropped {name} at epoch {}\n", epoch.get())
        }
        Command::Node(NodeCommand::Drain { id, target }) => {
            let primaries = target.client()?.drain_node(&id).await.map_err(failed)?;
            format_drain(&id, &primaries)
        }
        Command::Lease(LeaseCommand::Watch {
            node,
            address,
            target,
        }) => {
            let address = node_address(node.as_ref(), address, &target)
                .await?
                .ok_or("pass --node or --address")?;
            // The node answers on its own, about the leases it holds.
            let client = Client::new(address.clone(), target.tls.network()?);
            let mut updates = client.watch_leases(&address).await.map_err(failed)?;
            while let Some(update) = updates.next().await.map_err(failed)? {
                let mut stdout = std::io::stdout().lock();
                stdout.write_all(format_lease_update(&update).as_bytes())?;
                stdout.flush()?;
            }
            String::new()
        }
    };
    print!("{output}");
    Ok(())
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
        LeaseUpdate::Snapshot { leases, .. } => leases
            .iter()
            .map(|(database, epoch)| format!("held {database} epoch {}\n", epoch.get()))
            .collect(),
        LeaseUpdate::Event(event) => format!("{event}\n"),
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
            if ttl > MAX_TOKEN_LIFETIME {
                return Err(format!(
                    "a token lasts at most {} days, so its revocation stays short",
                    MAX_TOKEN_LIFETIME.as_secs() / (24 * 60 * 60)
                )
                .into());
            }
            let signer = Authority::load(&dir)?.token_signer()?;
            let mut random = [0_u8; TOKEN_ID_BYTES];
            SystemRandom::new()
                .fill(&mut random)
                .map_err(|_| "the system's random number generator failed")?;
            let id: TokenId = URL_SAFE_NO_PAD.encode(random).parse()?;
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
            eprintln!(
                "issued token {id} for {principal}, signed by key {}, for {}s",
                signer.key_id(),
                ttl.as_secs()
            );
            Ok(format!("{}\n", token.expose()))
        }
        TokenCommand::Revoke {
            token_id,
            key_id,
            ttl,
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
            let authority = Authority::load(&dir)?;
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
            ttl,
        } => {
            let authority = Authority::load(&dir)?;
            let issued = authority
                .issue_client(&principal, validity(ttl.unwrap_or(DEFAULT_CLIENT_LIFETIME)))?;
            let stem = format!("client.{principal}");
            issued.write(&dir, &stem)?;
            Ok(format!(
                "issued {} in {}: {stem}.crt and {stem}.key\n",
                issued.subject(),
                dir.display()
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

/// One replica's answer to `db verify`.
#[derive(Debug)]
enum Answer {
    Hash(Verification),
    Failed(String),
}

/// Runs `db verify`: asks every replica with an address for its chain hash at `at`, or at the
/// latest position they all hold, and reports agreement, the first replica that differs, and,
/// with `expect`, whether the log still hashes to a hash recorded earlier.
async fn verify(
    client: &Client,
    name: &DatabaseName,
    at: Option<Position>,
    expect: Option<ChainHash>,
) -> Result<String, Box<dyn Error>> {
    let location = client.locate(name).await.map_err(failed)?;
    let replicas: Vec<(NodeId, String)> = location
        .replicas
        .iter()
        .filter_map(|replica| Some((replica.node.clone(), replica.address.clone()?)))
        .collect();
    if replicas.is_empty() {
        return Err(format!("no replica of {name} has an address").into());
    }
    let ask = |at: Option<Position>| {
        let replicas = &replicas;
        async move {
            let mut answers = Vec::with_capacity(replicas.len());
            for (node, address) in replicas {
                let answer = match client.verify(address, name, at.as_ref()).await {
                    Ok(verification) => Answer::Hash(verification),
                    Err(status) => Answer::Failed(failed(status).to_string()),
                };
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
                .filter_map(|(_, answer)| match answer {
                    Answer::Hash(verification) => Some(verification.position.clone()),
                    Answer::Failed(_) => None,
                })
                .min()
                .flatten();
            let behind = answers.iter().any(|(_, answer)| {
                matches!(answer, Answer::Hash(verification) if verification.position != common)
            });
            if behind {
                answers = ask(common.clone()).await;
            }
            common
        }
    };
    let position_text = position
        .as_ref()
        .map_or_else(|| "empty log".to_owned(), ToString::to_string);
    let mut output = format!(
        "database: {name}
position: {position_text}
"
    );
    let mut agreed: Option<&ChainHash> = None;
    let mut differs: Option<(&NodeId, String)> = None;
    for (node, answer) in &answers {
        match answer {
            Answer::Hash(verification) => {
                let _ = writeln!(output, "  {node}: {}", verification.chain_hash);
                match agreed {
                    None => agreed = Some(&verification.chain_hash),
                    Some(hash) if *hash != verification.chain_hash && differs.is_none() => {
                        differs = Some((node, format!("holds {}", verification.chain_hash)));
                    }
                    Some(_) => {}
                }
            }
            Answer::Failed(reason) => {
                let _ = writeln!(output, "  {node}: {reason}");
                if differs.is_none() {
                    differs = Some((node, reason.clone()));
                }
            }
        }
    }
    let Some(hash) = agreed else {
        return Err(format!("{output}no replica of {name} could answer").into());
    };
    if let Some((node, reason)) = differs {
        return Err(format!(
            "{output}replicas of {name} disagree at {position_text}: {node} {reason}"
        )
        .into());
    }
    let _ = writeln!(
        output,
        "chain hash: {hash}
all {} replica{} agree",
        answers.len(),
        plural(u64::try_from(answers.len()).unwrap_or(u64::MAX))
    );
    if let Some(expect) = expect {
        if expect == *hash {
            let _ = writeln!(output, "nothing changed since {position_text}");
        } else {
            return Err(format!(
                "{output}the log at {position_text} hashes to {hash}, not the expected {expect}"
            )
            .into());
        }
    }
    Ok(output)
}

/// Asks on the terminal whether to drop `name`, and refuses without a terminal.
fn confirm_drop(name: &DatabaseName) -> Result<(), Box<dyn Error>> {
    let stdin = std::io::stdin();
    if !stdin.is_terminal() {
        return Err(format!("pass --yes to drop {name} without a prompt").into());
    }
    let mut stderr = std::io::stderr();
    write!(
        stderr,
        "drop {name}? Every replica deletes its copy, for good. [y/N] "
    )?;
    stderr.flush()?;
    let mut answer = String::new();
    stdin.lock().read_line(&mut answer)?;
    let answer = answer.trim();
    if answer.eq_ignore_ascii_case("y") || answer.eq_ignore_ascii_case("yes") {
        Ok(())
    } else {
        Err(format!("not dropping {name}").into())
    }
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
        location.database,
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
                location.database.to_string(),
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
        let _ = writeln!(output, "  {database}");
    }
    output
}

fn format_node_status(status: &NodeStatus) -> String {
    let fingerprint = status
        .fingerprint
        .map_or_else(|| "none".to_owned(), |fingerprint| fingerprint.to_string());
    let mut output = format!(
        "node: {}\nmembership epoch: {}\nring fingerprint: {fingerprint}\ndatabases:\n",
        status.node,
        status.membership_epoch.get(),
    );
    if status.databases.is_empty() {
        output.push_str("  none\n");
    }
    for database in &status.databases {
        let role = match database.role {
            ReplicaRole::Primary => "primary",
            ReplicaRole::Follower => "follower",
            _ => "unknown",
        };
        let position = database
            .position
            .as_ref()
            .map_or_else(|| "unknown".to_owned(), ToString::to_string);
        let _ = writeln!(
            output,
            "  {} {role} epoch={} position={position}",
            database.database,
            database.epoch.get(),
        );
    }
    if !status.sinks.is_empty() {
        output.push_str("change sinks:\n");
    }
    for sink in &status.sinks {
        let position = sink
            .position
            .as_ref()
            .map_or_else(|| "none".to_owned(), ToString::to_string);
        let acknowledged = sink
            .acknowledged
            .as_ref()
            .map_or_else(|| "none".to_owned(), ToString::to_string);
        let _ = write!(
            output,
            "  {} {} position={position} acknowledged={acknowledged} backlog={} failures={} resets={}",
            sink.sink, sink.database, sink.backlog, sink.failures, sink.resets
        );
        if let Some(error) = &sink.last_error {
            let _ = write!(output, " last_error={error:?}");
        }
        output.push('\n');
    }
    output
}

fn liveness(liveness: Liveness) -> &'static str {
    match liveness {
        Liveness::Live => "live",
        Liveness::Down => "down",
    }
}

fn format_status(view: &MembershipView) -> String {
    let fingerprint = view
        .fingerprint()
        .map_or_else(|| "none".to_owned(), |fingerprint| fingerprint.to_string());
    let mut output = format!(
        "membership epoch: {}\nring fingerprint: {fingerprint}\nmembers:\n",
        view.epoch().get()
    );
    for (member, live) in view.members() {
        let _ = write!(
            output,
            "  {} {} weight={}",
            member.id(),
            liveness(live),
            member.weight().get()
        );
        if let Some(zone) = member.zone() {
            let _ = write!(output, " zone={zone}");
        }
        if let Some(host) = member.host() {
            let _ = write!(output, " host={host}");
        }
        if let Some(address) = member.address() {
            let _ = write!(output, " address={address}");
        }
        output.push('\n');
    }
    output
}

/// A failed call as one readable line: what kind of failure, and the server's message.
fn failed(status: tonic::Status) -> Box<dyn Error> {
    format!("{}: {}", status.code().description(), status.message()).into()
}

/// Prints an error and its causes, one per line.
fn report(error: &dyn Error) {
    eprintln!("fleetringd: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        eprintln!("  caused by: {cause}");
        source = cause.source();
    }
}

/// Completes on Ctrl-C or, on Unix, SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "can't listen for Ctrl-C");
        }
    };
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    () = ctrl_c => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::error!(%error, "can't listen for SIGTERM");
                ctrl_c.await;
            }
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let snapshot = LeaseUpdate::Snapshot {
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
