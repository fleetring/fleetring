//! A change as JSON, for the file and webhook sinks and for whoever reads them back.

use std::fmt;

use fleetring_control::{DatabaseName, LeaseEpoch};
use serde::{Deserialize, Serialize};

use crate::changes::{Change, ChangeKind};
use crate::engine::{Payload, Position, RowChange};

/// One change as the built-in sinks write it: one JSON object per acknowledged batch.
///
/// ```json
/// {"database":"ledger","epoch":3,"start":"…","position":"…","kind":"logical","rows":[…]}
/// {"database":"ledger","epoch":3,"start":"…","position":"…","kind":"physical","payload":"…"}
/// ```
///
/// Positions and payloads are lowercase hex. `start` is `null` for a batch read from the
/// log's beginning. A logical record's `rows` are [`RowChange`]s in commit order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeRecord {
    /// The database.
    pub database: String,
    /// The epoch of the primary that acknowledged the batch.
    pub epoch: u64,
    /// The position the batch continues from, in hex, or `None` for the log's beginning.
    pub start: Option<String>,
    /// The position just past the batch, in hex.
    pub position: String,
    /// The batch's content.
    #[serde(flatten)]
    pub content: RecordContent,
}

/// The content of a [`ChangeRecord`], tagged by `kind`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum RecordContent {
    /// The opaque payload, in hex.
    Physical {
        /// The payload bytes as lowercase hex.
        payload: String,
    },
    /// The row changes, in commit order.
    Logical {
        /// The rows.
        rows: Vec<RowChange>,
    },
}

impl From<&Change> for ChangeRecord {
    fn from(change: &Change) -> Self {
        Self {
            database: change.database.to_string(),
            epoch: change.epoch.get(),
            start: change.start.as_ref().map(ToString::to_string),
            position: change.position.to_string(),
            content: match &change.kind {
                ChangeKind::Physical(payload) => RecordContent::Physical {
                    payload: hex(payload.as_bytes()),
                },
                ChangeKind::Logical(rows) => RecordContent::Logical { rows: rows.clone() },
            },
        }
    }
}

impl From<Change> for ChangeRecord {
    fn from(change: Change) -> Self {
        Self::from(&change)
    }
}

/// The error returned when a [`ChangeRecord`] can't be turned back into a [`Change`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseRecordError {
    /// The database name is invalid.
    #[error("invalid database name: {reason}")]
    Database {
        /// Why.
        reason: String,
    },
    /// A hex field is malformed.
    #[error("field `{field}` is not hex")]
    Hex {
        /// The field.
        field: &'static str,
    },
}

impl TryFrom<ChangeRecord> for Change {
    type Error = ParseRecordError;

    fn try_from(record: ChangeRecord) -> Result<Self, Self::Error> {
        let database: DatabaseName = record.database.parse().map_err(
            |error: fleetring_control::ParseDatabaseNameError| ParseRecordError::Database {
                reason: error.to_string(),
            },
        )?;
        let start = record
            .start
            .as_deref()
            .map(|text| unhex(text).map(Position::from))
            .transpose()
            .map_err(|_| ParseRecordError::Hex { field: "start" })?;
        let position = unhex(&record.position)
            .map(Position::from)
            .map_err(|_| ParseRecordError::Hex { field: "position" })?;
        let kind = match record.content {
            RecordContent::Physical { payload } => ChangeKind::Physical(Payload::from(
                unhex(&payload).map_err(|_| ParseRecordError::Hex { field: "payload" })?,
            )),
            RecordContent::Logical { rows } => ChangeKind::Logical(rows),
        };
        Ok(Self {
            database,
            epoch: LeaseEpoch::new(record.epoch),
            start,
            position,
            kind,
        })
    }
}

/// Lowercase hex of `bytes`.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing to a `String` can't fail.
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The bytes `text` spells in hex.
///
/// # Errors
///
/// [`NotHex`] if `text` has an odd length or a non-hex digit.
pub fn unhex(text: &str) -> Result<Vec<u8>, NotHex> {
    if !text.len().is_multiple_of(2) {
        return Err(NotHex);
    }
    (0..text.len())
        .step_by(2)
        .map(|at| {
            text.get(at..at + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or(NotHex)
        })
        .collect()
}

/// The error returned for text that isn't hex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotHex;

impl fmt::Display for NotHex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("not hex")
    }
}

impl std::error::Error for NotHex {}
