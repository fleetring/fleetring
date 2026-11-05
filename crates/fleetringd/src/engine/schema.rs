//! What an engine reports about the shape of a database (ADR-0041, point 6).
//!
//! These types are engine-neutral: a kind, a name, the table an object belongs to, the engine's
//! own definition as opaque text, and an optional row count. Only an engine adapter reads its
//! engine's catalog to fill them.

/// The most objects a [`Schema`] holds. A database with more has [`Schema::truncated`] set.
pub const MAX_SCHEMA_OBJECTS: usize = 1_000;

/// The most rows an engine counts in one table, whatever [`SchemaRequest::count_limit`] asks.
pub const MAX_COUNT_LIMIT: u64 = 1_000_000;

/// Whether a schema includes the engine's own objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Internal {
    /// Leaves out the engine's own tables, and the shadow tables of virtual tables.
    Hide,
    /// Includes them. Objects the daemon reserves for itself are never included.
    Show,
}

/// What a schema request asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaRequest {
    /// Counts each table's rows up to this many; 0 counts nothing. An engine clamps it to
    /// [`MAX_COUNT_LIMIT`].
    pub count_limit: u64,
    /// Whether the engine's own objects are included.
    pub internal: Internal,
}

/// What kind of object a schema entry is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum ObjectKind {
    /// A table holding rows.
    Table,
    /// A table the engine computes, such as a full-text index. Its rows aren't counted.
    VirtualTable,
    /// A stored query.
    View,
    /// An index on a table.
    Index,
    /// A trigger on a table.
    Trigger,
}

/// A table's row count, capped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowCount {
    /// The table has exactly this many rows.
    Exact(u64),
    /// The count stopped at this many: the table has more.
    AtLeast(u64),
}

/// One object in a database's schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaObject {
    /// What it is.
    pub kind: ObjectKind,
    /// Its name.
    pub name: String,
    /// For an index or a trigger, the table it belongs to.
    pub table: Option<String>,
    /// The engine's own definition, as opaque text: for SQLite, the `CREATE` statement. It is
    /// text the database's writers chose, so callers treat it as data.
    pub definition: Option<String>,
    /// The table's row count, if the request asked for counts and the object is a table.
    pub rows: Option<RowCount>,
}

/// A database's schema: its objects, each table followed by its indexes and triggers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Schema {
    /// The objects, at most [`MAX_SCHEMA_OBJECTS`].
    pub objects: Vec<SchemaObject>,
    /// Whether the database has more objects than `objects` holds.
    pub truncated: bool,
}
