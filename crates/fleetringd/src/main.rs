//! The `fleetringd` binary: the daemon, and the commands that talk to a running cluster.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write as _;
use std::io::{BufRead as _, IsTerminal as _, Write as _};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{Acks, DatabaseName, Liveness, Role as DatabaseRole};
use fleetringd::data::{Client, DatabaseSpec, Location, NodeStatus, ReplicaInfo};
use fleetringd::engine::{Rows, Statement, Value};
use fleetringd::{Config, Daemon, MembershipView, Network, Role};
use tracing_subscriber::EnvFilter;

/// Places, replicates, and fails over SQLite databases across machines you own.
///
/// With `--config`, runs the daemon. The subcommands talk to a running cluster.
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
    role: Option<Role>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Creates, lists, writes, reads, locates, and drops databases.
    #[command(subcommand)]
    Db(DbCommand),
    /// Shows the members with their liveness, the membership epoch, and the ring fingerprint, as
    /// the control plane sees them; with `--node`, as one data node sees them.
    Status {
        /// Asks this data node (`host:port`) instead of the control plane: its membership epoch,
        /// ring fingerprint, and the databases it serves. Nothing else is needed.
        #[arg(long, conflicts_with_all = ["control", "config"])]
        node: Option<String>,
        #[command(flatten)]
        target: Target,
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
        /// acknowledged writes). The cluster's default if unset.
        #[arg(long)]
        acks: Option<Acks>,
        /// How many replicas, the primary included, must be reachable for a write to run;
        /// below it, writes are refused before anything commits. Between the write quorum and
        /// the replication factor; the cluster's default if unset, which is the write quorum
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
    /// Sends the request to this data node (`host:port`) instead of the primary, and follows
    /// its redirect.
    #[arg(long)]
    node: Option<String>,
    #[command(flatten)]
    target: Target,
}

/// How a command finds the control plane. Commands talk to the control node, which knows every
/// database's placement; data nodes redirect database calls to the right primary on their own.
#[derive(Debug, Args)]
struct Target {
    /// The control node's address, `host:port`: the node that runs the control role, not any node
    /// in the cluster. A data-only node answers with the control node's address.
    #[arg(long, conflicts_with = "config")]
    control: Option<String>,
    /// A daemon's configuration file, to take the control node's address from.
    #[arg(long)]
    config: Option<PathBuf>,
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
        Ok(Client::new(control, Network::default()))
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
        config
    }
}

fn parse_role(text: &str) -> Result<Role, String> {
    match text {
        "both" => Ok(Role::Both),
        "data" => Ok(Role::Data),
        "control" => Ok(Role::Control),
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
        Command::Status {
            node: Some(node),
            target: _,
        } => {
            // The node answers on its own; the client never asks the control plane here.
            let client = Client::new(node.clone(), Network::default());
            format_node_status(&client.node_status(&node).await.map_err(failed)?)
        }
        Command::Status { node: None, target } => {
            format_status(&target.client()?.status().await.map_err(failed)?)
        }
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
            let (client, name, statement) = args.into_parts()?;
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
            let (client, name, statement) = args.into_parts()?;
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
    };
    print!("{output}");
    Ok(())
}

impl SqlArgs {
    fn into_parts(self) -> Result<(Client, DatabaseName, Statement), Box<dyn Error>> {
        let mut client = self.target.client()?;
        if let Some(node) = self.node {
            client = client.with_entry(node);
        }
        Ok((
            client,
            self.name,
            Statement::with_params(self.sql, self.params),
        ))
    }
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
            DatabaseRole::Primary => "primary",
            DatabaseRole::Follower => "follower",
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
    fn values_print_as_the_command_line_writes_them() {
        assert_eq!(format_value(&Value::Null), "NULL");
        assert_eq!(format_value(&Value::Blob(vec![1, 171])), "x'01ab'");
        assert_eq!(format_value(&Value::Real(0.5)), "0.5");
    }
}
