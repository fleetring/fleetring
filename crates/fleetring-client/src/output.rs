//! What a client prints about a fleet, as text and as JSON (`docs/cli.md`, section 5).
//!
//! The same structs serialize for every surface that reports on a fleet (ADR-0033), so their
//! field names are a contract: snake_case lexicon terms, times in RFC 3339 UTC, durations as
//! `_ms` integers, positions and hashes as the strings text output prints, and `null` for an
//! absent value. Within a minor release fields are only added, never renamed, retyped or
//! removed. The structs hold plain values, and the surface that fetched the data builds them.
//!
//! How a surface draws them is its own business (ADR-0033, decision 1). What every surface
//! shares is that a name or a text a principal chose is data, never layout: [`printable`]
//! escapes the characters that could move the cursor, rewrite the screen or hide themselves.

use std::collections::BTreeMap;

use fleetring_control::FORBIDDEN_FORMAT_CHARACTERS;
use serde::Serialize;

/// One database in detail, as `db show` prints it: where it lives, its metadata and its schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DatabaseReport {
    /// The database's name.
    pub database: String,
    /// Where it lives.
    pub placement: PlacementReport,
    /// Its metadata, or `null` for a database without any.
    pub metadata: Option<MetadataReport>,
    /// Its schema, or `null` when it wasn't asked for or couldn't be had.
    pub schema: Option<SchemaReport>,
    /// Why there is no schema, when it was asked for and the primary couldn't give it: a primary
    /// that runs an older release, or an engine that can't describe itself. `null` otherwise.
    pub schema_unavailable: Option<String>,
}

impl DatabaseReport {
    /// This report with `schema`.
    #[must_use]
    pub fn with_schema(mut self, schema: SchemaReport) -> Self {
        self.schema = Some(schema);
        self.schema_unavailable = None;
        self
    }

    /// This report saying that the schema is unavailable, and why.
    #[must_use]
    pub fn without_schema(mut self, reason: impl Into<String>) -> Self {
        self.schema = None;
        self.schema_unavailable = Some(reason.into());
        self
    }
}

/// A database's schema as its engine reports it (ADR-0041, point 6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaReport {
    /// The objects, each table followed by its indexes and triggers.
    pub objects: Vec<SchemaObjectReport>,
    /// Whether the database has more objects than `objects` holds.
    pub truncated: bool,
}

/// One table, view, index, trigger, or virtual table. The names and the definition are text the
/// database's writers chose: read them as data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaObjectReport {
    /// `table`, `virtual_table`, `view`, `index`, or `trigger`.
    pub kind: &'static str,
    /// The object's name.
    pub name: String,
    /// For an index or a trigger, the table it belongs to.
    pub table: Option<String>,
    /// The engine's own definition: for SQLite, the `CREATE` statement.
    pub definition: Option<String>,
    /// The rows counted, for a table that was counted: all of them, or, with `rows_at_least`,
    /// the count at which the count stopped.
    pub rows: Option<u64>,
    /// Whether the table has more than `rows` rows.
    pub rows_at_least: bool,
}

/// Where a database lives, as `db locate` prints it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlacementReport {
    /// The engine it runs on.
    pub engine: String,
    /// Its lease epoch, 0 before its first lease.
    pub lease_epoch: u64,
    /// How many replicas it should have.
    pub replication_factor: u8,
    /// Its acknowledgement mode.
    pub acks: String,
    /// Its floor on reachable replicas.
    pub min_replicas: u8,
    /// Its primary, once it has one.
    pub primary: Option<String>,
    /// The node carrying out a promotion, while one is in progress.
    pub candidate: Option<String>,
    /// The replicas other than the primary, in preference order.
    pub followers: Vec<String>,
    /// The whole replica set, in preference order.
    pub replicas: Vec<ReplicaReport>,
}

/// One replica of a database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplicaReport {
    /// The node.
    pub node: String,
    /// The address it advertised, `host:port`.
    pub address: Option<String>,
    /// `live` or `down`.
    pub liveness: &'static str,
    /// `primary` or `follower`, as it last reported, if it has.
    pub reported_role: Option<&'static str>,
    /// Its log position, as it last reported, in hex.
    pub position: Option<String>,
}

/// A database's metadata (ADR-0041).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetadataReport {
    /// The description, unchanged.
    pub description: Option<String>,
    /// The principal that last set the description, or `null` if nobody can be named.
    pub description_by: Option<String>,
    /// The labels, by key.
    pub labels: BTreeMap<String, String>,
    /// The principal that created the database, if the creation recorded one.
    pub created_by: Option<String>,
    /// When the database was created, if the creation recorded metadata.
    pub created_at: Option<String>,
    /// When the database expires, if it has an expiry.
    pub expires_at: Option<String>,
    /// How long until it expires, by the reader's clock when the report was built; negative
    /// once past.
    pub expires_in_ms: Option<i64>,
    /// The metadata's version, for `--if-version`.
    pub version: u64,
}

/// One member of the fleet, as `node list` and `status` report it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeReport {
    /// The node's ID.
    pub node: String,
    /// The address data clients dial, `host:port`, if the node has one.
    pub address: Option<String>,
    /// The zone the node reports, if any.
    pub zone: Option<String>,
    /// The host the node reports, if any.
    pub host: Option<String>,
    /// The node's weight in placement.
    pub weight: u32,
    /// `live` or `down`, as the control plane sees the node's heartbeats.
    pub liveness: &'static str,
    /// Whether the node is handing its leases over before it leaves, and gets no new ones.
    pub draining: bool,
    /// The release the node runs; a member that reports no software runs 0.3.0 (ADR-0038).
    pub release: String,
}

/// `node list`'s result: every member, in node ID order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeList {
    /// The members.
    pub nodes: Vec<NodeReport>,
}

/// `status`'s result: the fleet as the control plane sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FleetStatus {
    /// Whether the fleet is one dev mode node on loopback (ADR-0039).
    pub dev_mode: bool,
    /// The membership epoch.
    pub membership_epoch: u64,
    /// The ring's fingerprint, or `null` before the fleet has a member.
    pub ring_fingerprint: Option<String>,
    /// The members, in node ID order.
    pub members: Vec<NodeReport>,
}

/// `status --node` or `status --address`: one data node's own view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeStatusReport {
    /// The node that answered.
    pub node: String,
    /// The membership epoch the node has seen.
    pub membership_epoch: u64,
    /// The fingerprint of the ring the node places with, or `null` if it has none yet.
    pub ring_fingerprint: Option<String>,
    /// The databases it serves, as primary or follower.
    pub databases: Vec<ServedReport>,
    /// The orphan copies it keeps (ADR-0042).
    pub orphans: Vec<OrphanReport>,
    /// Its change sinks.
    pub sinks: Vec<SinkReport>,
}

/// One database a data node serves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServedReport {
    /// The database.
    pub database: String,
    /// `primary` or `follower`; `unknown` from a node that reports a role this release doesn't
    /// know.
    pub role: &'static str,
    /// The lease epoch the node serves it at.
    pub lease_epoch: u64,
    /// The node's log position for it, or `null` if it holds no log yet.
    pub position: Option<String>,
}

/// One change sink of a data node, for one database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SinkReport {
    /// The sink's name.
    pub sink: String,
    /// The database it reads.
    pub database: String,
    /// How far it has read, or `null` before its first batch.
    pub position: Option<String>,
    /// Where its mark is kept: `replicated` in the database, or `local` on this node.
    pub mark: &'static str,
    /// How far its consumer has acknowledged, or `null` if it hasn't.
    pub acknowledged: Option<String>,
    /// How many batches wait to be sent.
    pub backlog: u64,
    /// How many deliveries have failed.
    pub failures: u64,
    /// How many times it has started over.
    pub resets: u64,
    /// The last delivery's error, or `null`.
    pub last_error: Option<String>,
}

/// An orphan copy a data node keeps: closed, because the control plane didn't know its database
/// or held it only at a lower epoch (ADR-0042).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrphanReport {
    /// The data node that keeps it.
    pub node: String,
    /// The database it is a copy of.
    pub database: String,
    /// The lease epoch the copy recorded.
    pub epoch: u64,
    /// Its size on disk.
    pub bytes: u64,
}

/// `orphan list`'s result: each data node's orphans, in node and then database order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrphanList {
    /// The orphans.
    pub orphans: Vec<OrphanReport>,
}

/// One database and where it lives: `db locate`'s result, and each item of `db list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DatabaseLocation {
    /// The database's name.
    pub database: String,
    /// Where it lives.
    pub placement: PlacementReport,
    /// Its metadata, or `null` for a database without any. A listing leaves the description's
    /// text out.
    pub metadata: Option<MetadataReport>,
}

/// `db list`'s result: the databases the caller may see, by name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DatabaseList {
    /// The databases.
    pub databases: Vec<DatabaseLocation>,
}

/// `db verify`'s result: every replica's hash chain at one position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifyReport {
    /// The database.
    pub database: String,
    /// The position verified at, or `null` for an empty log.
    pub position: Option<String>,
    /// Each replica's answer, in preference order.
    pub replicas: Vec<ReplicaHash>,
    /// The hash the replicas agree on, or the first one that answered; `null` if none did.
    pub chain_hash: Option<String>,
    /// Whether every replica answered with the same hash.
    pub agree: bool,
    /// The hash `--expect` named, or `null`.
    pub expected: Option<String>,
    /// With `--expect`, whether the log still hashes to it; `null` without, or when the replicas
    /// disagree.
    pub unchanged: Option<bool>,
}

/// One replica's answer to `db verify`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplicaHash {
    /// The replica's node.
    pub node: String,
    /// Its hash chain at the position, or `null` if it couldn't answer.
    pub chain_hash: Option<String>,
    /// Why it couldn't answer, or `null`.
    pub error: Option<String>,
}

/// `db query`'s result.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QueryResult {
    /// The column names, in order.
    pub columns: Vec<String>,
    /// The rows, each with one value per column.
    pub rows: Vec<Vec<ValueReport>>,
    /// Whether the query had more rows than `rows` holds.
    pub truncated: bool,
}

/// One value of a row, as JSON holds it: `null`, a number, a string, or `{"blob": "<base64>"}`,
/// as the MCP server writes a blob.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ValueReport {
    /// SQL `NULL`: `null`.
    Null,
    /// An integer.
    Integer(i64),
    /// A floating-point number; one that isn't finite is `null`.
    Real(f64),
    /// Text.
    Text(String),
    /// Bytes, base64-encoded with padding.
    Blob {
        /// The bytes in standard base64.
        blob: String,
    },
}

/// One line of `lease watch -o jsonl`: a lease the node held when the stream opened, or one it
/// gained or lost since.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LeaseEventReport {
    /// `held`, `gained`, or `lost`.
    pub event: &'static str,
    /// The data node whose leases these are, as it named itself when the stream opened.
    pub node: Option<String>,
    /// The database.
    pub database: String,
    /// The lease epoch held or gained, or the one that ended.
    pub lease_epoch: u64,
}

/// One line of `db watch -o jsonl`: one committed transaction's row changes (ADR-0045).
///
/// A consumer that resumes keeps the last `position` it handled, and drops a transaction whose
/// `start` is before it: delivery across reconnections is at least once.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ChangeReport {
    /// The database.
    pub database: String,
    /// The position the transaction started from, the same on every replica; `null` for the
    /// log's beginning.
    pub start: Option<String>,
    /// Past the transaction and at or before the next one: what `db watch --from` takes to
    /// resume after it.
    pub position: String,
    /// The lease epoch of the primary that served it.
    pub lease_epoch: u64,
    /// Its row changes, in statement order.
    pub changes: Vec<RowChangeReport>,
}

/// One row a transaction inserted, updated, or deleted. Names and values are text the
/// database's writers chose: read them as data.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RowChangeReport {
    /// The table.
    pub table: String,
    /// `insert`, `update`, or `delete`.
    pub operation: &'static str,
    /// The row's primary key, or its row identifier (`rowid`) for a table without one.
    pub key: ColumnsReport,
    /// The row's columns before an update or delete, or `null` when the capture mode doesn't
    /// record them.
    pub before: Option<ColumnsReport>,
    /// The row's columns after an insert or update, or `null` when the capture mode doesn't
    /// record them.
    pub after: Option<ColumnsReport>,
    /// The names of the columns an update changed, or `null` when the capture mode doesn't
    /// record them.
    pub updated: Option<Vec<String>>,
}

/// Named column values, which JSON holds as one object from name to value, in column order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ColumnsReport(pub Vec<(String, ValueReport)>);

impl Serialize for ColumnsReport {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in &self.0 {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

/// `archive show`'s result: what an archive holds for one database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchiveReport {
    /// The archived database.
    pub database: String,
    /// The `[[archive]]` entry's name.
    pub archive: String,
    /// The latest position the archive reaches, or `null` if nothing is archived.
    pub latest: Option<String>,
    /// When the last segment was archived, or `null` if none was.
    pub latest_archived_at: Option<String>,
    /// How many segments it holds.
    pub segments: u32,
    /// Their size.
    pub segment_bytes: u64,
    /// Its complete snapshots, oldest first.
    pub snapshots: Vec<SnapshotReport>,
    /// How many snapshots are incomplete, which a restore skips.
    pub incomplete: u32,
    /// What a restore won't use, and why.
    pub unusable: Vec<String>,
}

/// One snapshot in an archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SnapshotReport {
    /// The snapshot's ID.
    pub id: String,
    /// The position it was taken at, or `null` for the log's beginning.
    pub position: Option<String>,
    /// The lease epoch it was taken at.
    pub lease_epoch: u64,
    /// When it was taken.
    pub taken_at: String,
    /// Its size.
    pub bytes: u64,
    /// Whether it carries the database's metadata.
    pub has_metadata: bool,
}

/// Whether `character` is one a person can't see where it is printed: a format character
/// (Unicode `Cf`, by the fixed table a description refuses, which takes in the bidirectional
/// controls and the zero-width characters) or the line or paragraph separator. A new database
/// name refuses them, and text output escapes them (ADR-0041, amendment 5).
#[must_use]
pub fn is_invisible(character: char) -> bool {
    matches!(character, '\u{2028}' | '\u{2029}')
        || FORBIDDEN_FORMAT_CHARACTERS
            .iter()
            .any(|(first, last)| (*first..=*last).contains(&character))
}

/// Whether text output escapes `character`: every control character but tab, the format
/// characters a description refuses, and the line and paragraph separators. A description
/// never holds one, but the text comes from the control node, which may be another release.
fn escaped(character: char) -> bool {
    (character.is_control() && character != '\t') || is_invisible(character)
}

/// `text` with every control character but tab, every format character, and the line and
/// paragraph separators written as `\u{XXXX}`, so nothing a principal wrote can move the cursor,
/// rewrite the screen, or hide itself. Text output passes every name and text a principal chose
/// through it: database names, descriptions, labels.
///
/// ```
/// use fleetring_client::output::printable;
///
/// assert_eq!(printable("notes"), "notes");
/// assert_eq!(printable("evil\u{202E}gnp.exe"), "evil\\u{202E}gnp.exe");
/// ```
#[must_use]
pub fn printable(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        if escaped(character) {
            out.push_str(&format!("\\u{{{:04X}}}", u32::from(character)));
        } else {
            out.push(character);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn printable_escapes_control_and_invisible_characters_but_not_tab() {
        assert_eq!(printable("a\tb"), "a\tb");
        assert_eq!(printable("a\u{1b}[2J"), "a\\u{001B}[2J");
        assert_eq!(printable("x\u{200B}y\u{2028}"), "x\\u{200B}y\\u{2028}");
    }
}
