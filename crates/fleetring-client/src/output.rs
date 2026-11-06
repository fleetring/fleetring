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
