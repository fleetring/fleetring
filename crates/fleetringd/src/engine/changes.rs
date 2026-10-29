//! Row changes, as an engine decodes them for a change stream (FR-039, ADR-0024).
//!
//! A [`RowChange`] is engine-neutral: a table name, an operation, the row's key, and whatever
//! column values the database's [`CaptureMode`] records, as [`Value`]s. How an engine captures
//! them is its own business; the SQLite adapter records them in a change table inside the
//! database, so that they replicate with the data.

use std::fmt;
use std::str::FromStr;

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
///
/// Which values come with the key depends on the [`CaptureMode`] the database records in:
///
/// | Mode | Insert | Update | Delete |
/// |---|---|---|---|
/// | [`Id`](CaptureMode::Id) | key | key | key |
/// | [`After`](CaptureMode::After) | key, `after` | key, `after` | key |
/// | [`Full`](CaptureMode::Full) | key, `after` | key, `before`, `after`, `updated` | key, `before` |
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowChange {
    /// The table the row belongs to.
    pub table: String,
    /// What happened to the row.
    pub operation: RowOperation,
    /// The row's primary key: the key columns of the row before an update or delete, or of the
    /// inserted row. A table without a declared key uses the engine's row identifier.
    pub key: Vec<Column>,
    /// The row's columns before an update or delete, when the mode records them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<Vec<Column>>,
    /// The row's columns after an insert or update, when the mode records them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Vec<Column>>,
    /// The names of the columns an update changed, when the mode records them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated: Option<Vec<String>>,
}

impl RowChange {
    /// A change of `operation` to the row of `table` keyed by `key`, with no values yet.
    #[must_use]
    pub fn new(
        table: impl Into<String>,
        operation: RowOperation,
        key: impl IntoIterator<Item = Column>,
    ) -> Self {
        Self {
            table: table.into(),
            operation,
            key: key.into_iter().collect(),
            before: None,
            after: None,
            updated: None,
        }
    }

    /// The same change, with the row's columns before it.
    #[must_use]
    pub fn with_before(mut self, before: impl IntoIterator<Item = Column>) -> Self {
        self.before = Some(before.into_iter().collect());
        self
    }

    /// The same change, with the row's columns after it.
    #[must_use]
    pub fn with_after(mut self, after: impl IntoIterator<Item = Column>) -> Self {
        self.after = Some(after.into_iter().collect());
        self
    }

    /// The same change, with the names of the columns it changed.
    #[must_use]
    pub fn with_updated(mut self, updated: impl IntoIterator<Item = String>) -> Self {
        self.updated = Some(updated.into_iter().collect());
        self
    }

    /// The row's columns as the change leaves it: `after` for an insert or update, `before` for a
    /// delete, or nothing when the mode didn't record them.
    #[must_use]
    pub fn values(&self) -> &[Column] {
        match self.operation {
            RowOperation::Insert | RowOperation::Update => self.after.as_deref(),
            RowOperation::Delete => self.before.as_deref(),
        }
        .unwrap_or_default()
    }
}

/// How much of each changed row a database records for its logical change streams (ADR-0024).
///
/// The modes are ordered from the least to the most recorded, so the strongest of several
/// requests is their maximum.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum CaptureMode {
    /// The key alone.
    Id,
    /// The key and, after an insert or update, the row's new values. The default.
    #[default]
    After,
    /// The key, the row's values before an update or delete, its values after an insert or
    /// update, and the names of the columns an update changed.
    Full,
}

impl CaptureMode {
    /// The mode's name as configuration spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::After => "after",
            Self::Full => "full",
        }
    }
}

impl fmt::Display for CaptureMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The error returned for text that names no [`CaptureMode`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a capture mode is `id`, `after`, or `full`")]
pub struct ParseCaptureModeError;

impl FromStr for CaptureMode {
    type Err = ParseCaptureModeError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "id" => Ok(Self::Id),
            "after" => Ok(Self::After),
            "full" => Ok(Self::Full),
            _ => Err(ParseCaptureModeError),
        }
    }
}

/// Whether an engine can decode the row changes of its committed transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum LogicalSupport {
    /// The engine records row changes in every [`CaptureMode`] and decodes them for a position
    /// range.
    Supported,
    /// The engine only ships opaque payloads; a logical change stream can't be built on it.
    Unsupported,
}
