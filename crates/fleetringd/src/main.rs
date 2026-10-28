//! The `fleetringd` binary: the daemon, and the commands that talk to a running cluster.

use std::error::Error;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, Liveness};
use fleetringd::data::{Client, Location, ReplicaInfo};
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
    /// Creates, writes, reads, and locates databases.
    #[command(subcommand)]
    Db(DbCommand),
    /// Shows the members with their liveness, the membership epoch, and the ring fingerprint.
    Status {
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
        #[command(flatten)]
        target: Target,
    },
    /// Runs one write statement on the database's primary.
    Exec(SqlArgs),
    /// Runs one read-only query on the database's primary.
    Query(SqlArgs),
    /// Shows the database's primary, followers, epoch, and replica positions.
    Locate {
        /// The database's name.
        name: DatabaseName,
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

/// How a command finds the control plane.
#[derive(Debug, Args)]
struct Target {
    /// The control node's address, `host:port`.
    #[arg(long, conflicts_with = "config")]
    control: Option<String>,
    /// A daemon's configuration file, to take the control address from.
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
        Command::Status { target } => {
            format_status(&target.client()?.status().await.map_err(failed)?)
        }
        Command::Db(DbCommand::Create {
            name,
            replication_factor,
            target,
        }) => {
            let replication_factor = ReplicationFactor::new(replication_factor)
                .ok_or("the replication factor must be at least 1")?;
            let location = target
                .client()?
                .create_database(&name, replication_factor)
                .await
                .map_err(failed)?;
            format!("created {name}\n{}", format_location(&location))
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
        "database: {}\nengine: {}\nepoch: {}\nprimary: {primary}\nfollowers: {}\npositions:\n",
        location.database,
        location.engine,
        location.epoch.get(),
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
