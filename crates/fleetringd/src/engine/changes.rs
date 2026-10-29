//! Row changes, as an engine decodes them for a change stream (FR-039).
//!
//! A [`RowChange`] is engine-neutral: a table name, an operation, the row's key, and its column
//! values as [`Value`]s. How an engine captures them is its own business; the SQLite adapter
//! records them with its pre-update hook.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::engine::Value;

/// What happened to one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum RowOperation {
    /// The row was inserted.
    Insert,
    /// The row was updated.
    Update,
    /// The row was deleted.
    Delete,
}

impl fmt::Display for RowOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        })
    }
}

/// One named column value of a row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    /// The column's name.
    pub name: String,
    /// Its value.
    pub value: Value,
}

impl Column {
    /// A column `name` holding `value`.
    #[must_use]
    pub fn new(name: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// One row inserted, updated, or deleted by a committed transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowChange {
    /// The table the row belongs to.
    pub table: String,
    /// What happened to the row.
    pub operation: RowOperation,
    /// The row's primary key: the key columns of the row before an update or delete, or of the
    /// inserted row. A table without a declared key uses the engine's row identifier.
    pub key: Vec<Column>,
    /// The row's columns: after an insert or update, before a delete.
    pub values: Vec<Column>,
}

/// Whether an engine can decode the row changes of its committed transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum LogicalSupport {
    /// The engine records row changes and decodes them for a position range.
    Supported,
    /// The engine only ships opaque payloads; a logical change stream can't be built on it.
    Unsupported,
}
