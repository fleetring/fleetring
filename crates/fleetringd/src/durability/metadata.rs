//! Where an archive gets a database's metadata (ADR-0041, point 10).

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use fleetring_control::DatabaseName;
use fleetring_proto::v1::{DatabaseMetadata, LocateRequest};

use super::format::{ArchivedMetadata, METADATA_FORMAT};
use crate::control::ControlClient;

/// The future a [`MetadataSource`] call returns: the metadata, `None` for a database that has
/// none, or why it can't be had.
pub type MetadataFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ArchivedMetadata>, String>> + Send + 'a>>;

/// Where the archive gets a database's metadata when it completes a snapshot.
pub trait MetadataSource: fmt::Debug + Send + Sync {
    /// The metadata of `database` as of now, `None` if it has none.
    ///
    /// # Errors
    ///
    /// Why the metadata can't be had. The archive then writes the snapshot without it, and the
    /// next snapshot carries it.
    fn metadata<'a>(&'a self, database: &'a DatabaseName) -> MetadataFuture<'a>;
}

/// A source with no metadata: for configurations without a control plane, and for tests.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoMetadata;

impl MetadataSource for NoMetadata {
    fn metadata<'a>(&'a self, _database: &'a DatabaseName) -> MetadataFuture<'a> {
        Box::pin(async { Ok(None) })
    }
}

/// The metadata the control plane holds, read with `Locate`, a call nodes may make.
#[derive(Debug, Clone)]
pub struct ControlMetadata {
    control: ControlClient,
}

impl ControlMetadata {
    /// A source that asks the control plane `control` reaches.
    #[must_use]
    pub fn new(control: ControlClient) -> Self {
        Self { control }
    }
}

impl MetadataSource for ControlMetadata {
    fn metadata<'a>(&'a self, database: &'a DatabaseName) -> MetadataFuture<'a> {
        Box::pin(async move {
            let response = self
                .control
                .locate(LocateRequest {
                    database: database.to_string(),
                })
                .await
                .map_err(|status| status.message().to_owned())?;
            Ok(response
                .placement
                .and_then(|placement| placement.metadata)
                .map(|metadata| archived(database, metadata)))
        })
    }
}

/// The metadata object for `database` that the control plane's `metadata` makes.
fn archived(database: &DatabaseName, metadata: DatabaseMetadata) -> ArchivedMetadata {
    ArchivedMetadata {
        format: METADATA_FORMAT,
        database: database.to_string(),
        description: (!metadata.description.is_empty()).then_some(metadata.description),
        description_by: metadata.description_by,
        labels: metadata
            .labels
            .into_iter()
            .map(|label| (label.key, label.value))
            .collect::<BTreeMap<_, _>>(),
        created_by: metadata.created_by,
        created_at_ms: metadata.created_at_ms,
        expires_at_ms: metadata.expires_at_ms,
        version: metadata.version,
    }
}

#[cfg(test)]
mod tests {
    use fleetring_proto::v1::Label;

    use super::*;

    #[test]
    fn a_placements_metadata_becomes_an_archived_object() {
        let database: DatabaseName = "agent-7-notes".parse().expect("name");
        let metadata = archived(
            &database,
            DatabaseMetadata {
                description: "Notes.".to_owned(),
                description_by: Some("agent-7".to_owned()),
                labels: vec![Label {
                    key: "purpose".to_owned(),
                    value: "memory".to_owned(),
                }],
                created_by: Some("ops".to_owned()),
                created_at_ms: Some(10),
                expires_at_ms: None,
                version: 3,
            },
        );
        assert_eq!(metadata.database, "agent-7-notes");
        assert_eq!(metadata.description.as_deref(), Some("Notes."));
        assert_eq!(
            metadata.labels.get("purpose").map(String::as_str),
            Some("memory")
        );
        assert_eq!(metadata.version, 3);
        let none = archived(&database, DatabaseMetadata::default());
        assert_eq!(none.description, None, "an empty description is none");
    }
}
