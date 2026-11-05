//! The `fleetringd` binary: the daemon, and the commands that talk to a running fleet.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write as _;
use std::io::{BufRead as _, IsTerminal as _, Read, Write as _};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use clap::error::ErrorKind;
use clap::{Args, CommandFactory as _, Parser, Subcommand};
use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{
    Acks, DatabaseName, Description, DescriptionChange, LabelKey, LabelValue, Labels, Liveness,
    MetadataChange, ReplicaRole, parse_label,
};
use fleetring_control::{KeyId, Millis, ParseReleaseError, Release, Timestamp, TokenId};
use fleetring_proto::v1::revocation::Revoked;
use fleetring_proto::v1::{Revocation, RevokeTokenRequest};
use fleetringd::auth::{BearerToken, DEFAULT_TOKEN_LIFETIME, Grant, MAX_TOKEN_LIFETIME, Scope};
use fleetringd::changes::MarkHome;
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::data::{
    ArchiveDescription, Client, DatabaseSpec, LabelRequirement, LeaseUpdate, ListFilter, Location,
    MetadataView, NewMetadata, NodeStatus, ReplicaInfo, Verification,
};
use fleetringd::dev::{self, DevError, DevMode, Prepared};
use fleetringd::durability::{
    ArchiveKeys, RestoreTarget, build_store, finish_offline, restore_into,
};
use fleetringd::engine::{Execution, Position, Rows, SqliteEngine, Statement, Value};
use fleetringd::observe::log_subscriber;
use fleetringd::replication::ChainHash;
use fleetringd::report::{DatabaseReport, printable, render_text};
use fleetringd::tls::{
    Authority, DEFAULT_AUTHORITY_LIFETIME, DEFAULT_CLIENT_LIFETIME, DEFAULT_NODE_LIFETIME,
    FleetName, Identity, Peer, Principal, Security, Validity,
};
use fleetringd::transport::Credentials;
#[cfg(unix)]
use fleetringd::upgrade::RESTARTS_VAR;
use fleetringd::upgrade::{self, LocalNode, Mode, Options, ProcessFile, Restart};
use fleetringd::{
    Config, Daemon, LogFormat, MembershipView, Network, NodeRole, ParseLogFormatError, Plaintext,
    Stop,
};
use ring::rand::{SecureRandom as _, SystemRandom};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::util::SubscriberInitExt as _;

/// The environment variable that names a directory with `ca.crt`, `client.crt`, and
/// `client.key`, and the `client.<principal>.crt` and `.key` files `--as` picks from. It supplies
/// whatever `--ca`, `--cert`, `--key`, and `--as` (and their variables) leave open.
const TLS_DIR_VAR: &str = "FLEETRING_TLS_DIR";

/// The environment variable that holds a bearer token, used when `--token` isn't given.
const TOKEN_VAR: &str = "FLEETRING_TOKEN";

/// The environment variable that names the control node's address, used when neither
/// `--control` nor `--config` is given. Dev mode's environment file sets it (ADR-0039).
const CONTROL_VAR: &str = "FLEETRING_CONTROL";

/// The environment variable that chooses the log format when `--log-format` isn't given.
const LOG_FORMAT_VAR: &str = "FLEETRING_LOG_FORMAT";

/// The exit code for a usage error, as clap's own.
const USAGE_ERROR: u8 = 2;

/// How many random bytes a token's ID holds.
const TOKEN_ID_BYTES: usize = 16;

/// Places, replicates, and fails over SQLite databases across machines you own.
///
/// With `--config`, runs the daemon. The subcommands talk to a running fleet.
#[derive(Debug, Parser)]
#[command(version, subcommand_negates_reqs = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[command(flatten)]
    daemon: DaemonArgs,
    /// Writes log lines as `text` or `json`. Overrides FLEETRING_LOG_FORMAT and the config file's
    /// `[log] format`.
    #[arg(long, global = true, value_parser = parse_log_format)]
    log_format: Option<LogFormat>,
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
    /// Runs one node for development, on loopback only, with a local authority and an admin
    /// principal that every command can act as (ADR-0039). The first run creates them, with the
    /// node's configuration, in the directory; later runs reuse them. Load `<dir>/env` in another
    /// terminal to point the commands at the node. Not for production.
    Dev(DevArgs),
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
    /// Drains a node, clears its draining mark, or removes it from the fleet.
    #[command(subcommand)]
    Node(NodeCommand),
    /// Follows the leases data nodes hold.
    #[command(subcommand)]
    Lease(LeaseCommand),
    /// Works with archives without a fleet: restores a database from one offline.
    #[command(subcommand)]
    Archive(ArchiveCommand),
    /// Installs another release of fleetringd in place of this binary, verified with cosign
    /// against the release workflow's identity and then by checksum, and keeps this one as
    /// `fleetringd.previous`. With `--config` naming a running node, first checks that the fleet
    /// allows the release, then drains the node, restarts it in place, and waits until it has
    /// rejoined and caught up.
    Upgrade(UpgradeArgs),
    /// Shows the members with their liveness, release, the membership epoch, and the ring
    /// fingerprint, as the control plane sees them; with `--node` or `--address`, as one data
    /// node sees them.
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

/// `fleetringd dev`'s flags (ADR-0039).
#[derive(Debug, Args)]
struct DevArgs {
    /// The directory that holds the authority, the certificates, the configuration, and the
    /// databases; `~/.fleetring` by default.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// The loopback port the node serves on, 7400 by default; the admin listener takes the next
    /// one. Only the first run chooses it: later runs read it from `<dir>/fleetringd.toml`, and
    /// refuse a different one.
    #[arg(long)]
    port: Option<u16>,
}

/// `fleetringd upgrade`'s flags (ADR-0038).
#[derive(Debug, Args)]
struct UpgradeArgs {
    /// The release to install, such as 0.4.0; the newest final release by default. Naming the
    /// installed release reinstalls it.
    #[arg(long, value_parser = parse_release)]
    version: Option<Release>,
    /// Reports the installed release, the newest one, and whether the fleet allows it, and
    /// changes nothing. Exits 1 if a newer release exists that the fleet doesn't allow.
    #[arg(long, conflicts_with_all = ["rollback", "no_restart"])]
    check: bool,
    /// Replaces the binary, but leaves the running node on the release it runs.
    #[arg(long)]
    no_restart: bool,
    /// Swaps the binary with the one the last upgrade replaced, `fleetringd.previous`, and
    /// restarts the node as an upgrade does. A second rollback rolls forward again.
    #[arg(long, conflicts_with_all = ["version", "base_url", "releases_url"])]
    rollback: bool,
    /// Downloads the release's files from this URL, a mirror's directory for one release as
    /// `install.sh --base-url` takes it, instead of GitHub. Needs --version.
    #[arg(long, env = "FLEETRING_BASE_URL", requires = "version")]
    base_url: Option<String>,
    /// Reads the list of releases, in the JSON GitHub's API returns, from this URL instead of
    /// GitHub's, to find the newest release on a mirror.
    #[arg(long, env = "FLEETRING_RELEASES_URL")]
    releases_url: Option<String>,
    /// How long each download, the drain, the rejoin, and the catch-up may take: `<n>m` or
    /// `<n>s`; 5m by default.
    #[arg(long, value_parser = parse_ttl)]
    timeout: Option<Duration>,
    /// Prints what this binary runs, as one JSON object (its release, protocol, control-log
    /// format, and fleet features), and changes nothing. An upgrade asks a release's binary this
    /// before it installs it, to check the fleet's features.
    #[arg(long, exclusive = true)]
    software: bool,
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
    /// Restores an archived database into a new directory, with no fleet: for disaster
    /// recovery. The directory then holds a plain SQLite database, `data.db`, that `sqlite3`
    /// opens directly. Every batch is verified against the hash chain on the way.
    Restore {
        /// A daemon's configuration file, whose `[[archive]]` entry names the store.
        #[arg(long)]
        config: PathBuf,
        /// The `[[archive]]` entry's name.
        #[arg(long)]
        archive: String,
        /// The archived database to restore.
        #[arg(long)]
        from: DatabaseName,
        /// The directory to write, which must not exist.
        #[arg(long)]
        to: PathBuf,
        #[command(flatten)]
        point: PointArgs,
    },
}

/// Which point of an archive a command restores: the latest unless one is named.
#[derive(Debug, Args)]
struct PointArgs {
    /// Restores exactly the log up to this position, a transaction boundary, as `db locate` or
    /// `db archive describe` prints positions.
    #[arg(long, value_parser = parse_position, conflicts_with = "at_time")]
    at_position: Option<Position>,
    /// Restores every segment archived at or before this time, RFC 3339, such as
    /// `2026-11-03T09:30:00Z`, by the archiving nodes' clocks. The restore may stop up to one
    /// segment interval before it, and never holds a write acknowledged after it.
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
enum DbArchiveCommand {
    /// Shows what an archive holds for a database: its snapshots, how far its segments reach,
    /// and anything a restore won't use.
    Show {
        /// The archived database.
        name: DatabaseName,
        /// The `[[archive]]` entry's name.
        #[arg(long)]
        archive: String,
        /// Asks the data node at this address, `host:port`, instead of the first live member.
        #[arg(long)]
        address: Option<String>,
        #[command(flatten)]
        target: Target,
    },
    /// Restores an archived database into a scratch copy on a data node, verifying every batch
    /// against the hash chain, and discards it; then asks each live replica for its hash at the
    /// restored position and reports whether they agree with the archive.
    Verify {
        /// The archived database.
        name: DatabaseName,
        /// The `[[archive]]` entry's name.
        #[arg(long)]
        archive: String,
        /// Asks the data node at this address, `host:port`, instead of the first live member.
        #[arg(long)]
        address: Option<String>,
        #[command(flatten)]
        point: PointArgs,
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
        /// How many replicas it has, the primary included. The fleet's default if unset: the
        /// control node's `[replication] replication_factor`, which is 3 unless its configuration
        /// says otherwise, and 1 on a dev node.
        #[arg(long)]
        replication_factor: Option<u8>,
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
        metadata: MetadataArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Shows a database in detail: where it lives, as `db locate` prints it, and its
    /// description, labels, creator, and metadata version.
    ///
    /// A description is text a principal wrote: it prints as a block, each line indented and
    /// prefixed with `| `, and one written by another principal, or by nobody that can be
    /// named, is marked untrusted.
    Show {
        /// The database's name.
        name: DatabaseName,
        #[command(flatten)]
        output: OutputArgs,
        #[command(flatten)]
        target: Target,
    },
    /// Sets or clears a database's description. The control node records you as its author.
    SetDescription {
        /// The database's name.
        name: DatabaseName,
        /// The description: what the database holds and how it was set up, at most 1 KiB of
        /// text; tab and line feed are allowed, other control and invisible characters aren't.
        /// `-` reads it from stdin, without its trailing line feeds.
        #[arg(long, required_unless_present = "clear", allow_hyphen_values = true)]
        description: Option<String>,
        /// Removes the description.
        #[arg(long, conflicts_with = "description")]
        clear: bool,
        /// Changes it only if the metadata is still at this version, as `db show` prints it;
        /// refused otherwise, so a description read first isn't overwritten by mistake.
        #[arg(long)]
        if_version: Option<u64>,
        #[command(flatten)]
        target: Target,
    },
    /// Sets and removes a database's labels. Labels it doesn't name stay as they are.
    SetLabels {
        /// The database's name.
        name: DatabaseName,
        /// A label to set, `key=value`, replacing the key's value if the database has it.
        /// Repeat it for several. At least one `--label` or `--remove` is needed.
        #[arg(
            long = "label",
            value_name = "KEY=VALUE",
            value_parser = parse_label_arg,
            required_unless_present = "remove"
        )]
        labels: Vec<(LabelKey, LabelValue)>,
        /// A label key to remove. Repeat it for several.
        #[arg(long = "remove", value_name = "KEY")]
        remove: Vec<LabelKey>,
        /// Changes them only if the metadata is still at this version, as `db show` prints it.
        #[arg(long)]
        if_version: Option<u64>,
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
        /// Lists only the databases with this label: `key=value`, or `key` for any value.
        /// Repeat it to require several.
        #[arg(long = "label", value_name = "KEY=VALUE|KEY")]
        labels: Vec<LabelRequirement>,
        /// Lists only the databases whose names start with this.
        #[arg(long)]
        prefix: Option<String>,
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
    /// Creates a new database from an archived one: the first live member of its replica set
    /// restores it, verifying every batch against the hash chain, and serves it. The archived
    /// database needs not exist any more; restoring under its own name needs it dropped first.
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
        /// How many replicas the new database has, the primary included; the fleet's default if
        /// unset, as for `db create`.
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
    /// Shows and verifies a database's archive.
    #[command(subcommand)]
    Archive(DbArchiveCommand),
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
    /// Clears a node's draining mark, so that it may take primaries again without a restart, as
    /// after a drain that turned out not to be needed. Refused until every member runs a release
    /// that supports it.
    Undrain {
        /// The node's ID.
        id: NodeId,
        #[command(flatten)]
        target: Target,
    },
    /// Removes a member that is gone for good from the fleet, as a node that leaves on shutdown
    /// does, so that its release no longer holds the fleet back. Refuses a live member unless
    /// `--force`; a live node joins again at its next heartbeat anyway. Asks first on a terminal.
    Remove {
        /// The node's ID.
        id: NodeId,
        /// Removes the member even though it is live.
        #[arg(long)]
        force: bool,
        /// Removes without asking. Without it, the command asks first on a terminal, and
        /// refuses elsewhere.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        target: Target,
    },
}

/// What `db create` and `db restore` record about the new database besides its placement.
#[derive(Debug, Args)]
struct MetadataArgs {
    /// What the database holds and how it was set up, at most 1 KiB of text; tab and line feed
    /// are allowed, other control and invisible characters aren't. `-` reads it from stdin,
    /// without its trailing line feeds. The control node records you as the creator.
    #[arg(long, allow_hyphen_values = true)]
    description: Option<String>,
    /// A label, `key=value`, to find the database by (`db list --label`). Repeat it for
    /// several, at most 32.
    #[arg(long = "label", value_name = "KEY=VALUE", value_parser = parse_label_arg)]
    labels: Vec<(LabelKey, LabelValue)>,
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
        })
    }
}

/// How a command prints its result: `-o table|json`, or `--json`.
#[derive(Debug, Args)]
struct OutputArgs {
    /// The output format: `table`, key and value lines for one item, or `json`, one object
    /// with stable snake_case fields.
    #[arg(short = 'o', long, value_enum, default_value_t = OutputFormat::Table)]
    output: OutputFormat,
    /// Short for `-o json`.
    #[arg(long, conflicts_with = "output")]
    json: bool,
}

impl OutputArgs {
    fn format(&self) -> OutputFormat {
        if self.json {
            OutputFormat::Json
        } else {
            self.output
        }
    }
}

/// An output format (`docs/cli.md`, section 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum OutputFormat {
    /// For people.
    Table,
    /// For programs.
    Json,
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
    /// in the fleet. A data-only node answers with the control node's address. Defaults to
    /// `FLEETRING_CONTROL` when neither this nor `--config` is given.
    #[arg(long, conflicts_with = "config")]
    control: Option<String>,
    /// A daemon's configuration file, to take the control node's address from.
    #[arg(long)]
    config: Option<PathBuf>,
    #[command(flatten)]
    tls: TlsArgs,
}

/// The identity a command connects with: a client certificate from `cert client`.
///
/// Each of the three files is the first of these that names it: the flag, the environment
/// variable, `--as` (or `FLEETRING_AS`) for the certificate and key, and the default under
/// `FLEETRING_TLS_DIR`.
#[derive(Debug, Args)]
struct TlsArgs {
    /// The trusted authority certificates, PEM. Defaults to `$FLEETRING_TLS_DIR/ca.crt`.
    #[arg(long, env = "FLEETRING_CA")]
    ca: Option<PathBuf>,
    /// The client certificate, PEM. Overrides `--as`. Defaults to
    /// `$FLEETRING_TLS_DIR/client.crt`.
    #[arg(long, env = "FLEETRING_CERT")]
    cert: Option<PathBuf>,
    /// The client certificate's private key, PEM. Overrides `--as`. Defaults to
    /// `$FLEETRING_TLS_DIR/client.key`.
    #[arg(long, env = "FLEETRING_KEY")]
    key: Option<PathBuf>,
    /// Acts as this principal, using `client.<principal>.crt` and `client.<principal>.key` under
    /// `FLEETRING_TLS_DIR`, as `cert client <principal>` writes them. An explicit `--cert` or
    /// `--key`, or its environment variable, wins over this for that file, and this wins over
    /// `client.crt` and `client.key`.
    #[arg(long = "as", env = "FLEETRING_AS", value_name = "PRINCIPAL")]
    principal: Option<Principal>,
    /// Connects in plaintext, to a daemon started with `--insecure-plaintext`.
    #[arg(long, conflicts_with_all = ["ca", "cert", "key", "principal"])]
    insecure_plaintext: bool,
    /// A bearer token from `token issue`, sent with every request; the request then acts with
    /// the token's scopes. Defaults to `FLEETRING_TOKEN`, if set.
    #[arg(long)]
    token: Option<String>,
}

/// The three files a command's identity is loaded from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct IdentityFiles {
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
}

impl TlsArgs {
    fn security(&self) -> Result<Security, Box<dyn Error>> {
        if self.insecure_plaintext {
            return Ok(Security::Plaintext);
        }
        let dir = std::env::var_os(TLS_DIR_VAR).map(PathBuf::from);
        let files = self.files(dir.as_deref())?;
        Ok(Security::Tls(Identity::load(
            &files.ca,
            &files.cert,
            &files.key,
        )?))
    }

    /// Picks the identity files. Each takes the first of: its flag or environment variable (clap
    /// has already merged the two), the principal's file under `dir` if `--as` names one, and
    /// the default name under `dir`.
    ///
    /// # Errors
    ///
    /// If a file has none of those and `dir` is unset; if `--as` is needed and `dir` is unset;
    /// or if `--as` is needed and the principal's certificate or key is missing from `dir`.
    fn files(&self, dir: Option<&Path>) -> Result<IdentityFiles, Box<dyn Error>> {
        let in_dir = |name: &str, flag: &str| -> Result<PathBuf, Box<dyn Error>> {
            dir.map(|dir| dir.join(name)).ok_or_else(|| {
                format!(
                    "pass --{flag} (or set {TLS_DIR_VAR} to a directory with ca.crt, \
                     client.crt, and client.key), or --insecure-plaintext"
                )
                .into()
            })
        };
        // `--as` only matters for a file nothing more explicit names.
        let named = match (&self.principal, &self.cert, &self.key) {
            (Some(principal), None, _) | (Some(principal), _, None) => {
                Some(principal_files(principal, dir)?)
            }
            _ => None,
        };
        let ca = match &self.ca {
            Some(ca) => ca.clone(),
            None => in_dir("ca.crt", "ca")?,
        };
        let cert = match (&self.cert, &named) {
            (Some(cert), _) => cert.clone(),
            (None, Some((cert, _))) => cert.clone(),
            (None, None) => in_dir("client.crt", "cert")?,
        };
        let key = match (&self.key, &named) {
            (Some(key), _) => key.clone(),
            (None, Some((_, key))) => key.clone(),
            (None, None) => in_dir("client.key", "key")?,
        };
        Ok(IdentityFiles { ca, cert, key })
    }

    /// The bearer token the command sends: `--token`, else `FLEETRING_TOKEN`, if set.
    fn token(&self) -> Option<String> {
        match &self.token {
            Some(token) => Some(token.clone()),
            None => std::env::var(TOKEN_VAR)
                .ok()
                .filter(|token| !token.is_empty()),
        }
    }

    /// The principal the command acts as, so that what others wrote can be told apart from
    /// what it wrote: the one its token names, if it sends one, else its certificate's. Read
    /// without verifying; the nodes verify. `None` in plaintext without a token, or when
    /// neither can be read.
    fn principal(&self) -> Option<String> {
        if let Some(token) = self.token() {
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

    fn network(&self) -> Result<Network, Box<dyn Error>> {
        let network = Network::default().with_security(self.security()?);
        let token = self.token();
        Ok(match token {
            Some(token) => {
                network.with_credentials(Credentials::bearer(&BearerToken::new(token.trim()))?)
            }
            None => network,
        })
    }
}

/// The certificate and key `cert client <principal>` writes under `dir`.
///
/// # Errors
///
/// If `dir` is unset, or either file is missing; the message names both files.
fn principal_files(
    principal: &Principal,
    dir: Option<&Path>,
) -> Result<(PathBuf, PathBuf), Box<dyn Error>> {
    let Some(dir) = dir else {
        return Err(format!(
            "--as {principal} needs {TLS_DIR_VAR} to find client.{principal}.crt and \
             client.{principal}.key; set it, or pass --cert and --key"
        )
        .into());
    };
    let cert = dir.join(format!("client.{principal}.crt"));
    let key = dir.join(format!("client.{principal}.key"));
    if cert.is_file() && key.is_file() {
        return Ok((cert, key));
    }
    Err(format!(
        "no client identity for principal '{principal}': looked for {} and {}\n  \
         hint: create it with `fleetringd cert client {principal} --dir {}`",
        cert.display(),
        key.display(),
        dir.display(),
    )
    .into())
}

impl Target {
    fn client(&self) -> Result<Client, Box<dyn Error>> {
        let control = match (&self.control, &self.config) {
            (Some(control), _) => control.clone(),
            (None, Some(path)) => Config::load(path)?
                .control_target()
                .ok_or("the configuration names no control address; pass --control")?
                .to_string(),
            (None, None) => std::env::var(CONTROL_VAR)
                .ok()
                .filter(|control| !control.is_empty())
                .ok_or_else(|| format!("pass --control or --config, or set {CONTROL_VAR}"))?,
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
    Command(Command),
    /// The daemon, from a configuration file or from dev mode's directory.
    Daemon {
        args: DaemonArgs,
        config: Box<Config>,
        /// Set in dev mode: the prepared directory, for the banner.
        dev: Option<Box<Prepared>>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(error) = cli.check() {
        error.exit();
    }
    let Cli {
        command,
        daemon,
        log_format: format_flag,
    } = cli;
    // The daemon reads its configuration before logging starts, since it may name the format;
    // dev mode prepares its directory first, since the configuration is in it.
    let run = match (command, daemon.config.clone()) {
        (Some(Command::Dev(args)), _) => match prepare_dev(&args) {
            Ok(prepared) => Run::Daemon {
                args: daemon,
                config: Box::new(prepared.config().clone()),
                dev: Some(Box::new(prepared)),
            },
            Err(error) => {
                report(&error);
                return ExitCode::FAILURE;
            }
        },
        (Some(command), _) => Run::Command(command),
        (None, Some(path)) => match Config::load(path) {
            Ok(config) => Run::Daemon {
                args: daemon,
                config: Box::new(config),
                dev: None,
            },
            Err(error) => {
                report(&error);
                return ExitCode::FAILURE;
            }
        },
        (None, None) => {
            eprintln!("fleetringd: --config is required");
            return ExitCode::FAILURE;
        }
    };
    let config = match &run {
        Run::Daemon { config, .. } => Some(config.as_ref()),
        Run::Command(_) => None,
    };
    let variable = std::env::var(LOG_FORMAT_VAR).ok();
    let format = match log_format(format_flag, variable.as_deref(), config) {
        Ok(format) => format,
        Err(value) => {
            eprintln!("fleetringd: invalid {LOG_FORMAT_VAR} '{value}'; use text or json");
            return ExitCode::from(USAGE_ERROR);
        }
    };
    // The daemon logs at info; a command prints its result and logs only warnings.
    let default_filter = match run {
        Run::Command(_) => "warn",
        Run::Daemon { .. } => "info",
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| default_filter.into());
    if let Err(error) = log_subscriber(format, filter, std::io::stderr).try_init() {
        report(&error);
        return ExitCode::FAILURE;
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
            .block_on(run_command(command))
            .map(|()| Stopped::Exit),
        Run::Daemon { args, config, dev } => {
            runtime.block_on(run_daemon(args, *config, dev.map(|prepared| *prepared)))
        }
    };
    // Every task, connection, and database is closed by now; the runtime's threads go too,
    // before a restart executes the binary again.
    runtime.shutdown_timeout(RUNTIME_GRACE);
    match result {
        Ok(Stopped::Restart { exe, restarts }) => restart_in_place(&exe, restarts),
        Ok(Stopped::Exit) => ExitCode::SUCCESS,
        Err(error) => {
            report(error.as_ref());
            ExitCode::FAILURE
        }
    }
}

impl Cli {
    /// Refuses the daemon's options next to a subcommand. `--log-format` is the one option both
    /// take, which is why clap's own conflict rule, which would refuse it too, isn't used.
    ///
    /// # Errors
    ///
    /// A usage error naming the conflict.
    fn check(&self) -> Result<(), clap::Error> {
        let daemon = &self.daemon;
        let given = daemon.config.is_some()
            || daemon.node_id.is_some()
            || daemon.data_dir.is_some()
            || daemon.listen.is_some()
            || daemon.advertise.is_some()
            || daemon.control.is_some()
            || daemon.role.is_some()
            || daemon.insecure_plaintext;
        if self.command.is_some() && given {
            return Err(Self::command().error(
                ErrorKind::ArgumentConflict,
                "the daemon's options can't be used with a subcommand; put a command's options \
                 after its name",
            ));
        }
        Ok(())
    }
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

/// Executes `exe`, the binary the daemon started from, with the same arguments, keeping the
/// process ID, so that systemd, a container, or a terminal sees the same process (ADR-0038).
/// Returns only if that fails.
#[cfg(unix)]
fn restart_in_place(exe: &Path, restarts: u32) -> ExitCode {
    use std::os::unix::process::CommandExt as _;
    tracing::info!(exe = %exe.display(), "restarting in place");
    let error = std::process::Command::new(exe)
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

/// Prepares dev mode's directory: creates what it lacks, and loads its configuration.
fn prepare_dev(args: &DevArgs) -> Result<Prepared, DevError> {
    let dir = match &args.data_dir {
        Some(dir) => dir.clone(),
        None => dev::default_dir()?,
    };
    dev::prepare(&dir, args.port, SystemClock.now())
}

async fn run_daemon(
    args: DaemonArgs,
    config: Config,
    dev: Option<Prepared>,
) -> Result<Stopped, Box<dyn Error>> {
    let config = args.apply(config);
    // Read before an upgrade can replace the file: the path a restart executes again.
    let exe = std::env::current_exe()?;
    let data_dir = config.data_dir.clone();
    // Listened for before the daemon logs its start, so that no signal sent once it has started
    // finds the default action, which ends the process.
    let signals = StopSignals::listen();
    let daemon = Daemon::start(config).await?;
    if let Some(prepared) = &dev {
        eprint!(
            "\n{}\n",
            prepared.banner(daemon.listen_address(), daemon.admin_address())
        );
    }
    let process = ProcessFile::this_process(exe.clone());
    if let Err(error) = process.write(&data_dir) {
        tracing::warn!(%error, "can't write the process file; `fleetringd upgrade` can't restart this node in place");
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
        Command::Dev(_) => return Err("fleetringd dev runs the daemon, not a command".into()),
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
            // A plain creation keeps the call every release knows (ADR-0041, point 2).
            let location = if metadata.is_empty() {
                client.create_database_with(&name, spec).await
            } else {
                client
                    .create_database_with_metadata(&name, spec, &metadata)
                    .await
            }
            .map_err(metadata_failed)?;
            warn_if_under_replicated(&location);
            let mut output = format!("created {}\n{}", shown(&name), format_location(&location));
            if let Some(metadata) = &location.metadata {
                output.push_str(&format_metadata_summary(metadata));
            }
            output
        }
        Command::Db(DbCommand::Show {
            name,
            output,
            target,
        }) => {
            let location = target.client()?.locate(&name).await.map_err(failed)?;
            let report = DatabaseReport::from_location(&location, SystemClock.now());
            match output.format() {
                OutputFormat::Json => format!("{}\n", serde_json::to_string_pretty(&report)?),
                OutputFormat::Table => {
                    let mut text = Vec::new();
                    render_text(&report, target.tls.principal().as_deref(), &mut text)?;
                    String::from_utf8(text)?
                }
            }
        }
        Command::Db(DbCommand::SetDescription {
            name,
            description,
            clear,
            if_version,
            target,
        }) => {
            let description = match description {
                Some(text) if !clear => {
                    DescriptionChange::Set(description_arg(&text, std::io::stdin().lock())?)
                }
                _ => DescriptionChange::Clear,
            };
            let change = MetadataChange {
                description,
                ..MetadataChange::default()
            };
            let metadata = target
                .client()?
                .update_database(&name, &change, if_version)
                .await
                .map_err(|status| updated_failed(status, if_version))?;
            let what = if metadata.description.is_some() {
                "set"
            } else {
                "cleared"
            };
            format!(
                "description of {name} {what}; metadata version {}\n",
                metadata.version,
                name = shown(&name)
            )
        }
        Command::Db(DbCommand::SetLabels {
            name,
            labels,
            remove,
            if_version,
            target,
        }) => {
            let change = MetadataChange {
                set_labels: Labels::try_from_pairs(labels)?,
                remove_labels: remove.into_iter().collect(),
                ..MetadataChange::default()
            };
            let metadata = target
                .client()?
                .update_database(&name, &change, if_version)
                .await
                .map_err(|status| updated_failed(status, if_version))?;
            format!(
                "labels of {name}: {}; metadata version {}\n",
                format_labels(&metadata),
                metadata.version,
                name = shown(&name)
            )
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
                change.acks,
                change.min_replicas,
                change.current_acks,
                change.current_min_replicas,
                name = shown(&name)
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
        Command::Db(DbCommand::List {
            node,
            labels,
            prefix,
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
            format_databases(&databases)
        }
        Command::Db(DbCommand::Verify {
            name,
            at,
            expect,
            target,
        }) => verify(&target.client()?, &name, at, expect).await?,
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
            let restored = target
                .client()?
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
            warn_if_under_replicated(&restored.location);
            format!(
                "restored {name} from {from} in archive {archive}\n{}{}",
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
            )
        }
        Command::Db(DbCommand::Archive(DbArchiveCommand::Show {
            name,
            archive,
            address,
            target,
        })) => {
            let client = target.client()?;
            let address = archive_node(&client, address).await?;
            let description = client
                .describe_archive(&address, &archive, &name)
                .await
                .map_err(failed)?;
            format_archive(&archive, &description)
        }
        Command::Db(DbCommand::Archive(DbArchiveCommand::Verify {
            name,
            archive,
            address,
            point,
            target,
        })) => verify_archive(&target.client()?, &name, &archive, address, &point.target()).await?,
        Command::Archive(ArchiveCommand::Restore {
            config,
            archive,
            from,
            to,
            point,
        }) => restore_offline(&config, &archive, &from, &to, &point.target()).await?,
        Command::Db(DbCommand::Drop { name, yes, target }) => {
            if !yes {
                confirm_drop(&name)?;
            }
            let epoch = target
                .client()?
                .drop_database(&name)
                .await
                .map_err(failed)?;
            format!("dropped {} at epoch {}\n", shown(&name), epoch.get())
        }
        Command::Node(NodeCommand::Drain { id, target }) => {
            let primaries = target.client()?.drain_node(&id).await.map_err(failed)?;
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
                confirm_remove(&id)?;
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
            })
        }
        None => None,
    };
    let fleet = if args.target.control.is_some() || args.target.config.is_some() {
        Some(args.target.client()?)
    } else {
        None
    };
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
    })
    .await?;
    if outcome.blocked {
        print!("{}", outcome.report);
        return Err("the fleet doesn't allow the newest release yet".into());
    }
    Ok(outcome.report)
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

/// `db archive describe`'s report.
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
            "  {}: position {} epoch {} taken {} ({} bytes)",
            snapshot.id,
            snapshot
                .position
                .as_ref()
                .map_or_else(|| "begin".to_owned(), ToString::to_string),
            snapshot.epoch.get(),
            format_time(snapshot.taken_at_ms),
            snapshot.bytes
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

/// Runs `db archive verify`: a scratch restore on a data node, then each live replica's hash at
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
                let _ = writeln!(output, "  {}: {}", replica.node, failed(status));
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
    Ok(format!(
        "restored {from} from archive {archive} into {}\n{}the directory holds a plain SQLite \
         database: {}\n",
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
            for warning in scopes.iter().filter_map(Scope::separator_warning) {
                eprintln!("warning: {warning}");
            }
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
        return Err(format!("no replica of {} has an address", shown(name)).into());
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
    let mut output = format!("database: {}\nposition: {position_text}\n", shown(name));
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
        return Err(format!("{output}no replica of {} could answer", shown(name)).into());
    };
    if let Some((node, reason)) = differs {
        return Err(format!(
            "{output}replicas of {name} disagree at {position_text}: {node} {reason}",
            name = shown(name)
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
        return Err(format!("pass --yes to drop {} without a prompt", shown(name)).into());
    }
    let mut stderr = std::io::stderr();
    write!(
        stderr,
        "drop {}? Every replica deletes its copy, for good. [y/N] ",
        shown(name)
    )?;
    stderr.flush()?;
    let mut answer = String::new();
    stdin.lock().read_line(&mut answer)?;
    let answer = answer.trim();
    if answer.eq_ignore_ascii_case("y") || answer.eq_ignore_ascii_case("yes") {
        Ok(())
    } else {
        Err(format!("not dropping {}", shown(name)).into())
    }
}

fn confirm_remove(id: &NodeId) -> Result<(), Box<dyn Error>> {
    let stdin = std::io::stdin();
    if !stdin.is_terminal() {
        return Err(format!("pass --yes to remove {id} without a prompt").into());
    }
    let mut stderr = std::io::stderr();
    write!(
        stderr,
        "remove {id} from the fleet? Its replicas are placed elsewhere. [y/N] "
    )?;
    stderr.flush()?;
    let mut answer = String::new();
    stdin.lock().read_line(&mut answer)?;
    let answer = answer.trim();
    if answer.eq_ignore_ascii_case("y") || answer.eq_ignore_ascii_case("yes") {
        Ok(())
    } else {
        Err(format!("not removing {id}").into())
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
            shown(&database.database),
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
        let kept = match sink.home {
            Some(MarkHome::Replicated) => "replicated",
            _ => "local",
        };
        let _ = write!(
            output,
            "  {} {} position={position} ({kept}) acknowledged={acknowledged} backlog={} failures={} resets={}",
            sink.sink,
            shown(&sink.database),
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
    match liveness {
        Liveness::Live => "live",
        Liveness::Down => "down",
    }
}

fn format_status(view: &MembershipView) -> String {
    let fingerprint = view
        .fingerprint()
        .map_or_else(|| "none".to_owned(), |fingerprint| fingerprint.to_string());
    let mut output = String::new();
    if view.dev_mode() == DevMode::On {
        output.push_str("dev mode: one node on loopback, for development only\n");
    }
    let _ = write!(
        output,
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
        // A member that reports no software runs 0.3.0 (ADR-0038).
        let _ = write!(output, " version={}", member.running().release());
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
        eprintln!("warning: {warning}");
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

/// A failed call as one readable line: what kind of failure, and the server's message.
fn failed(status: tonic::Status) -> Box<dyn Error> {
    // The message may name a database, whose name a principal chose.
    format!(
        "{}: {}",
        status.code().description(),
        printable(status.message())
    )
    .into()
}

/// The next step after a refusal of a creation or update with metadata for want of the
/// `metadata` fleet feature (ADR-0041, point 3), or `None` for any other status. A fleet with a
/// member that lacks the feature needs that member upgraded; a fleet that no member has joined
/// yet needs a data node first (ADR-0041, amendment 3).
fn metadata_hint(status: &tonic::Status) -> Option<&'static str> {
    if status.code() != tonic::Code::FailedPrecondition {
        return None;
    }
    let message = status.message();
    if message.contains("can't use 'metadata' yet: no member has joined") {
        Some("start a data node first, or create it without --description and --label")
    } else if message.contains("can't use 'metadata' yet") {
        Some("create it without --description and --label, or upgrade every member first")
    } else {
        None
    }
}

/// A failed creation or metadata update as [`failed`] says it, with the next step when the
/// fleet can't use the `metadata` feature yet.
fn metadata_failed(status: tonic::Status) -> Box<dyn Error> {
    match metadata_hint(&status) {
        Some(hint) => format!("{}\n  hint: {hint}", failed(status)).into(),
        None => failed(status),
    }
}

/// A failed metadata update as [`failed`] says it; a conditional update whose version no
/// longer holds says how to read the metadata again.
fn updated_failed(status: tonic::Status, if_version: Option<u64>) -> Box<dyn Error> {
    match if_version {
        Some(version) if status.code() == tonic::Code::Aborted => format!(
            "the database's metadata changed since version {version}; read it again with \
             fleetringd db show"
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

/// What a creation recorded, after its placement: the labels, the creator, and the version;
/// `db show` prints the description.
fn format_metadata_summary(metadata: &MetadataView) -> String {
    format!(
        "labels: {}\ncreator: {}\nversion: {}\n",
        format_labels(metadata),
        metadata.created_by.as_deref().unwrap_or("unknown"),
        metadata.version
    )
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
                "--config",
                "fleetring.toml",
            ],
        ] {
            let cli = Cli::try_parse_from(words).expect("the flag parses");
            assert_eq!(cli.log_format, Some(LogFormat::Json), "{words:?}");
        }
        let cli = Cli::try_parse_from(["fleetringd", "--config", "fleetring.toml"])
            .expect("no flag parses");
        assert_eq!(cli.log_format, None);
    }

    #[test]
    fn daemon_options_are_refused_next_to_a_subcommand() {
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
        ] {
            let cli = Cli::try_parse_from(words).expect("clap parses it");
            let refused = cli.check().expect_err("a daemon option and a subcommand");
            assert_eq!(refused.kind(), ErrorKind::ArgumentConflict, "{words:?}");
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
            let cli = Cli::try_parse_from(words).expect("clap parses it");
            assert!(cli.check().is_ok(), "{words:?}");
        }
    }

    #[test]
    fn dev_takes_a_directory_and_a_port_and_no_daemon_options() {
        let cli = Cli::try_parse_from(["fleetringd", "dev", "--data-dir", "d", "--port", "7410"])
            .expect("dev parses");
        assert!(cli.check().is_ok());
        let Some(Command::Dev(args)) = cli.command else {
            panic!("not dev: {:?}", cli.command);
        };
        assert_eq!(args.data_dir, Some(PathBuf::from("d")));
        assert_eq!(args.port, Some(7410));
        let cli = Cli::try_parse_from(["fleetringd", "--listen", "0.0.0.0:7400", "dev"])
            .expect("clap parses it");
        let refused = cli.check().expect_err("a daemon option beside dev");
        assert_eq!(refused.kind(), ErrorKind::ArgumentConflict);
        assert!(Cli::try_parse_from(["fleetringd", "dev", "--listen", "0.0.0.0:7400"]).is_err());
    }

    #[test]
    fn db_create_parses_labels_and_reads_a_description_from_stdin_when_dash() {
        let metadata = |words: &[&str]| {
            let cli = Cli::try_parse_from(words).expect("db create parses");
            let Some(Command::Db(DbCommand::Create { metadata, .. })) = cli.command else {
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
                "upgrade every member first\n  hint: create it without --description and \
                 --label, or upgrade every member first"
            ),
            "{hinted}"
        );
        let empty = tonic::Status::failed_precondition(
            "the fleet can't use 'metadata' yet: no member has joined",
        );
        let hinted = metadata_failed(empty).to_string();
        assert!(
            hinted.ends_with(
                "no member has joined\n  hint: start a data node first, or create it without \
                 --description and --label"
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
    fn db_show_takes_json_as_a_flag_or_an_output_format() {
        let format = |words: &[&str]| {
            let cli = Cli::try_parse_from(words).expect("db show parses");
            let Some(Command::Db(DbCommand::Show { output, .. })) = cli.command else {
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
        assert!(
            Cli::try_parse_from(
                [
                    &["fleetringd", "db", "set-description", "notes"][..],
                    &show[4..]
                ]
                .concat()
            )
            .is_err(),
            "set-description needs --description or --clear"
        );
        let set_labels = ["fleetringd", "db", "set-labels", "notes"];
        assert!(
            Cli::try_parse_from([&set_labels[..], &show[4..]].concat()).is_err(),
            "set-labels needs a label or a removal"
        );
        assert!(
            Cli::try_parse_from([&set_labels[..], &["--remove", "agent"], &show[4..]].concat())
                .is_ok()
        );
    }

    #[test]
    fn db_create_leaves_the_replication_factor_to_the_fleet_unless_given() {
        let factor = |words: &[&str]| {
            let cli = Cli::try_parse_from(words).expect("db create parses");
            let Some(Command::Db(DbCommand::Create {
                replication_factor, ..
            })) = cli.command
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

    /// Identity arguments as the command line, after clap has merged each flag with its variable.
    fn tls_args(cert: Option<&str>, key: Option<&str>, principal: Option<&str>) -> TlsArgs {
        TlsArgs {
            ca: None,
            cert: cert.map(PathBuf::from),
            key: key.map(PathBuf::from),
            principal: principal.map(|name| name.parse().expect("a principal")),
            insecure_plaintext: false,
            token: None,
        }
    }

    fn files(ca: &str, cert: &str, key: &str) -> IdentityFiles {
        IdentityFiles {
            ca: PathBuf::from(ca),
            cert: PathBuf::from(cert),
            key: PathBuf::from(key),
        }
    }

    #[test]
    fn the_identity_files_come_from_a_flag_then_as_then_the_directory() {
        let dir = tempfile::tempdir().expect("a directory");
        let root = dir.path();
        for name in ["client.app.crt", "client.app.key"] {
            std::fs::write(root.join(name), "").expect("a file");
        }
        let at = |name: &str| root.join(name).to_str().expect("a UTF-8 path").to_owned();

        // The directory's defaults.
        assert_eq!(
            tls_args(None, None, None).files(Some(root)).expect("files"),
            files(&at("ca.crt"), &at("client.crt"), &at("client.key")),
        );
        // `--as` replaces the certificate and the key, not the authority.
        assert_eq!(
            tls_args(None, None, Some("app"))
                .files(Some(root))
                .expect("files"),
            files(&at("ca.crt"), &at("client.app.crt"), &at("client.app.key")),
        );
        // An explicit file beats `--as`, one file at a time.
        assert_eq!(
            tls_args(Some("c.crt"), None, Some("app"))
                .files(Some(root))
                .expect("files"),
            files(&at("ca.crt"), "c.crt", &at("client.app.key")),
        );
        assert_eq!(
            tls_args(None, Some("c.key"), Some("app"))
                .files(Some(root))
                .expect("files"),
            files(&at("ca.crt"), &at("client.app.crt"), "c.key"),
        );
        // With both named, `--as` isn't consulted: its files needn't exist, nor the directory.
        let mut explicit = tls_args(Some("c.crt"), Some("c.key"), Some("nobody"));
        explicit.ca = Some(PathBuf::from("ca.pem"));
        assert_eq!(
            explicit.files(None).expect("files"),
            files("ca.pem", "c.crt", "c.key"),
        );
    }

    #[test]
    fn an_unknown_principal_names_the_files_it_looked_for() {
        let dir = tempfile::tempdir().expect("a directory");
        let root = dir.path();
        // A certificate without its key is as unknown as neither.
        std::fs::write(root.join("client.half.crt"), "").expect("a file");
        for name in ["nobody", "half"] {
            let error = tls_args(None, None, Some(name))
                .files(Some(root))
                .expect_err("unknown principal")
                .to_string();
            let cert = root.join(format!("client.{name}.crt"));
            let key = root.join(format!("client.{name}.key"));
            assert!(
                error.starts_with(&format!(
                    "no client identity for principal '{name}': looked for {} and {}",
                    cert.display(),
                    key.display()
                )),
                "{error}"
            );
            assert!(
                error.contains(&format!(
                    "hint: create it with `fleetringd cert client {name} --dir"
                )),
                "{error}"
            );
        }
    }

    #[test]
    fn identity_files_need_a_directory_unless_every_file_is_named() {
        let error = tls_args(None, None, Some("app"))
            .files(None)
            .expect_err("no directory")
            .to_string();
        assert!(
            error.starts_with("--as app needs FLEETRING_TLS_DIR to find client.app.crt"),
            "{error}"
        );
        let error = tls_args(None, None, None)
            .files(None)
            .expect_err("no directory")
            .to_string();
        assert!(
            error.starts_with("pass --ca (or set FLEETRING_TLS_DIR"),
            "{error}"
        );
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
