//! Conversions between the protocol's messages and the daemon's types.

use std::fmt::Display;
use std::str::FromStr;

use std::collections::{BTreeMap, BTreeSet};

use fleetring::{Epoch, Fingerprint, NodeId, ReplicationFactor};
use fleetring_control::{
    Acks, DatabaseName, Description, DescriptionChange, Durability, Engine, LabelKey, LabelValue,
    Labels, LeaseEpoch, Liveness, Metadata, MetadataChange, ReplicaRole, Revocations, Revoked,
    Timestamp,
};
use fleetring_proto::v1::update_database_request::Description as DescriptionUpdate;
use fleetring_proto::v1::{self as proto, lease_update::Update, value::Value as Kind};
use tonic::Status;

use crate::changes::{MarkHome, SinkStatus};
use crate::data::client::{LeaseUpdate, Location, NodeStatus, ReplicaInfo, ServedDatabase};
use crate::data::metadata::{Descriptions, MetadataView, NewMetadata};
use crate::engine::{Position, Rows, Statement, Value};
use crate::replication::{LeaseChange, LeaseEvent};

/// Parses `text` as `field`, or answers `INVALID_ARGUMENT`.
pub(crate) fn parse<T>(field: &str, text: &str) -> Result<T, Status>
where
    T: FromStr,
    T::Err: Display,
{
    text.parse()
        .map_err(|error| Status::invalid_argument(format!("invalid {field}: {error}")))
}

/// Parses `text`, which a peer sent in a reply, as `field`, or answers `INTERNAL`.
fn parse_reply<T>(field: &str, text: &str) -> Result<T, Status>
where
    T: FromStr,
    T::Err: Display,
{
    text.parse()
        .map_err(|error| Status::internal(format!("the reply has an invalid {field}: {error}")))
}

pub(crate) fn value_to_proto(value: Value) -> proto::Value {
    let kind = match value {
        Value::Null => Kind::Null(true),
        Value::Integer(integer) => Kind::Integer(integer),
        Value::Real(real) => Kind::Real(real),
        Value::Text(text) => Kind::Text(text),
        Value::Blob(blob) => Kind::Blob(blob),
    };
    proto::Value { value: Some(kind) }
}

pub(crate) fn value_from_proto(value: proto::Value) -> Option<Value> {
    Some(match value.value? {
        Kind::Null(_) => Value::Null,
        Kind::Integer(integer) => Value::Integer(integer),
        Kind::Real(real) => Value::Real(real),
        Kind::Text(text) => Value::Text(text),
        Kind::Blob(blob) => Value::Blob(blob),
    })
}

/// The statement a request carries, or `INVALID_ARGUMENT` if a parameter has no value.
pub(crate) fn statement(sql: String, params: Vec<proto::Value>) -> Result<Statement, Status> {
    let params = params
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            value_from_proto(value).ok_or_else(|| {
                Status::invalid_argument(format!("parameter {} has no value", index + 1))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Statement::with_params(sql, params))
}

pub(crate) fn params_to_proto(statement: &Statement) -> Vec<proto::Value> {
    statement
        .params()
        .iter()
        .cloned()
        .map(value_to_proto)
        .collect()
}

pub(crate) fn rows_to_proto(rows: Rows) -> proto::QueryResponse {
    proto::QueryResponse {
        columns: rows.columns,
        rows: rows
            .rows
            .into_iter()
            .map(|row| proto::Row {
                values: row.into_iter().map(value_to_proto).collect(),
            })
            .collect(),
    }
}

pub(crate) fn rows_from_proto(response: proto::QueryResponse) -> Result<Rows, Status> {
    let rows = response
        .rows
        .into_iter()
        .map(|row| {
            row.values
                .into_iter()
                .map(|value| {
                    value_from_proto(value)
                        .ok_or_else(|| Status::internal("the reply has a value without a kind"))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Rows {
        columns: response.columns,
        rows,
    })
}

/// The acknowledgement mode `acks` names, or `None` if it is unspecified.
pub(crate) fn acks_from_proto(acks: i32) -> Option<Acks> {
    match proto::Acks::try_from(acks) {
        Ok(proto::Acks::Quorum) => Some(Acks::Quorum),
        Ok(proto::Acks::All) => Some(Acks::All),
        Ok(proto::Acks::Local) => Some(Acks::Local),
        Ok(proto::Acks::Unspecified) | Err(_) => None,
    }
}

pub(crate) fn acks_to_proto(acks: Acks) -> proto::Acks {
    match acks {
        Acks::Quorum => proto::Acks::Quorum,
        Acks::All => proto::Acks::All,
        Acks::Local => proto::Acks::Local,
        _ => proto::Acks::Unspecified,
    }
}

/// The durability settings `placement` carries for a database with `replication_factor`
/// replicas: [`Acks::Quorum`] with the floor at W from a control node that predates the fields,
/// and the mode's W for a floor the control node left at 0.
pub(crate) fn durability_of(
    placement: &proto::Placement,
    replication_factor: ReplicationFactor,
) -> Durability {
    let acks = acks_from_proto(placement.acks).unwrap_or_default();
    u8::try_from(placement.min_replicas)
        .ok()
        .filter(|min_replicas| *min_replicas > 0)
        .and_then(|min_replicas| {
            Durability::with_min_replicas(acks, min_replicas, replication_factor).ok()
        })
        .unwrap_or_else(|| Durability::new(acks, replication_factor))
}

/// The protocol form of `metadata`, with its description text or without it.
pub(crate) fn metadata_to_proto(
    metadata: &Metadata,
    descriptions: Descriptions,
) -> proto::DatabaseMetadata {
    let description = match descriptions {
        Descriptions::Include => metadata
            .description()
            .map(|text| text.as_str().to_owned())
            .unwrap_or_default(),
        Descriptions::Omit => String::new(),
    };
    proto::DatabaseMetadata {
        description,
        description_by: metadata.description_by().map(ToString::to_string),
        labels: labels_to_proto(metadata.labels()),
        created_by: metadata.created_by().map(ToString::to_string),
        created_at_ms: metadata.created_at().map(Timestamp::as_millis),
        expires_at_ms: metadata.expires_at().map(Timestamp::as_millis),
        version: metadata.version(),
    }
}

/// The metadata a reply carries, as the client reports it.
pub(crate) fn metadata_view(metadata: proto::DatabaseMetadata) -> MetadataView {
    let mut labels: Vec<(String, String)> = metadata
        .labels
        .into_iter()
        .map(|label| (label.key, label.value))
        .collect();
    labels.sort();
    MetadataView {
        description: (!metadata.description.is_empty()).then_some(metadata.description),
        description_by: metadata.description_by,
        labels,
        created_by: metadata.created_by,
        created_at: metadata.created_at_ms.map(Timestamp::from_millis),
        expires_at: metadata.expires_at_ms.map(Timestamp::from_millis),
        version: metadata.version,
    }
}

/// The labels a request carries, or `INVALID_ARGUMENT` naming the label at fault. A repeated
/// key keeps its last value.
pub(crate) fn labels_from_proto(labels: &[proto::Label]) -> Result<Labels, Status> {
    let pairs = labels
        .iter()
        .map(|label| {
            let invalid = |error: &dyn Display| {
                Status::invalid_argument(format!(
                    "invalid label '{}={}': {error}",
                    label.key, label.value
                ))
            };
            let key: LabelKey = label.key.parse().map_err(|error| invalid(&error))?;
            let value: LabelValue = label.value.parse().map_err(|error| invalid(&error))?;
            Ok((key, value))
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Labels::try_from_pairs(pairs).map_err(|error| Status::invalid_argument(error.to_string()))
}

/// `labels` as the protocol sends them, sorted by key.
pub(crate) fn labels_to_proto(labels: &Labels) -> Vec<proto::Label> {
    labels
        .iter()
        .map(|(key, value)| proto::Label {
            key: key.to_string(),
            value: value.to_string(),
        })
        .collect()
}

/// The description a request carries: `None` for the empty text, which is no description.
pub(crate) fn description_from_proto(text: &str) -> Result<Option<Description>, Status> {
    if text.is_empty() {
        return Ok(None);
    }
    text.parse()
        .map(Some)
        .map_err(|error| Status::invalid_argument(format!("invalid description: {error}")))
}

/// A description an update sets, which mustn't be empty: clearing one is its own change.
fn set_description(text: &str) -> Result<Description, Status> {
    description_from_proto(text)?.ok_or_else(|| {
        Status::invalid_argument("the description is empty; clear it with clear_description")
    })
}

/// The patch an update asks for.
///
/// # Errors
///
/// `INVALID_ARGUMENT` for an invalid description, label, or label key; a key both set and
/// removed; or a patch that changes nothing.
pub(crate) fn change_from_proto(
    request: &proto::UpdateDatabaseRequest,
) -> Result<MetadataChange, Status> {
    let description = match &request.description {
        None | Some(DescriptionUpdate::ClearDescription(false)) => DescriptionChange::Keep,
        Some(DescriptionUpdate::SetDescription(text)) => {
            DescriptionChange::Set(set_description(text)?)
        }
        Some(DescriptionUpdate::SetDescriptionWithoutAuthor(text)) => {
            DescriptionChange::SetWithoutAuthor(set_description(text)?)
        }
        Some(DescriptionUpdate::ClearDescription(true)) => DescriptionChange::Clear,
    };
    let set_labels = labels_from_proto(&request.set_labels)?;
    let remove_labels = request
        .remove_labels
        .iter()
        .map(|key| {
            key.parse::<LabelKey>().map_err(|error| {
                Status::invalid_argument(format!("invalid label key '{key}': {error}"))
            })
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if let Some(key) = remove_labels
        .iter()
        .find(|key| set_labels.get(key).is_some())
    {
        return Err(Status::invalid_argument(format!(
            "the label '{key}' is both set and removed"
        )));
    }
    let change = MetadataChange {
        description,
        set_labels,
        remove_labels,
        ..MetadataChange::default()
    };
    if change.is_empty() {
        return Err(Status::invalid_argument(
            "the update changes nothing: set or clear the description, or set or remove a label",
        ));
    }
    Ok(change)
}

/// The update request that applies `change` to `database`, at `if_version` if given.
///
/// # Errors
///
/// `INVALID_ARGUMENT` for a change the protocol can't carry yet: one to the expiry.
pub(crate) fn change_to_proto(
    database: &DatabaseName,
    change: &MetadataChange,
    if_version: Option<u64>,
) -> Result<proto::UpdateDatabaseRequest, Status> {
    if change.touches_expiry() {
        return Err(Status::invalid_argument(
            "this release can't change a database's expiry",
        ));
    }
    let description = match &change.description {
        DescriptionChange::Set(text) => {
            Some(DescriptionUpdate::SetDescription(text.as_str().to_owned()))
        }
        DescriptionChange::SetWithoutAuthor(text) => Some(
            DescriptionUpdate::SetDescriptionWithoutAuthor(text.as_str().to_owned()),
        ),
        DescriptionChange::Clear => Some(DescriptionUpdate::ClearDescription(true)),
        _ => None,
    };
    Ok(proto::UpdateDatabaseRequest {
        database: database.to_string(),
        description,
        set_labels: labels_to_proto(&change.set_labels),
        remove_labels: change
            .remove_labels
            .iter()
            .map(ToString::to_string)
            .collect(),
        if_version,
    })
}

/// The metadata a creation asks for, as the protocol sends it.
pub(crate) fn new_metadata_to_proto(metadata: &NewMetadata) -> proto::MetadataSpec {
    proto::MetadataSpec {
        description: metadata
            .description
            .as_ref()
            .map(|text| text.as_str().to_owned())
            .unwrap_or_default(),
        labels: labels_to_proto(&metadata.labels),
    }
}

/// The location a placement describes.
pub(crate) fn location(placement: proto::Placement) -> Result<Location, Status> {
    let engine = match proto::Engine::try_from(placement.engine) {
        // SQLite is the only engine so far, and an unset engine means it.
        Ok(proto::Engine::Sqlite | proto::Engine::Unspecified) => Engine::Sqlite,
        Err(_) => {
            return Err(Status::internal(format!(
                "the reply has an unknown engine {}",
                placement.engine
            )));
        }
    };
    let primary: Option<NodeId> = placement
        .primary
        .as_deref()
        .map(|primary| parse_reply("primary", primary))
        .transpose()?;
    let candidate: Option<NodeId> = placement
        .candidate
        .as_deref()
        .map(|candidate| parse_reply("candidate", candidate))
        .transpose()?;
    let replication_factor = u8::try_from(placement.replication_factor)
        .ok()
        .and_then(ReplicationFactor::new)
        .or_else(|| {
            u8::try_from(placement.replicas.len())
                .ok()
                .and_then(ReplicationFactor::new)
        })
        .unwrap_or(ReplicationFactor::ONE);
    let durability = durability_of(&placement, replication_factor);
    let replicas = placement
        .replicas
        .into_iter()
        .map(|replica| {
            let reported_role = match proto::ReplicaRole::try_from(replica.reported_role) {
                Ok(proto::ReplicaRole::Primary) => Some(ReplicaRole::Primary),
                Ok(proto::ReplicaRole::Follower) => Some(ReplicaRole::Follower),
                _ => None,
            };
            Ok(ReplicaInfo {
                node: parse_reply("node id", &replica.node_id)?,
                address: replica.address,
                liveness: if replica.live {
                    Liveness::Live
                } else {
                    Liveness::Down
                },
                reported_role,
                position: replica.position.map(Position::from),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(Location {
        database: parse_reply::<DatabaseName>("database name", &placement.database)?,
        engine,
        epoch: LeaseEpoch::new(placement.epoch),
        primary,
        candidate,
        replicas,
        replication_factor,
        durability,
        metadata: placement.metadata.map(metadata_view),
    })
}

/// The status a node reported.
pub(crate) fn node_status(response: proto::NodeStatusResponse) -> Result<NodeStatus, Status> {
    let databases = response
        .databases
        .into_iter()
        .map(|status| {
            let role = match proto::ReplicaRole::try_from(status.role) {
                Ok(proto::ReplicaRole::Primary) => ReplicaRole::Primary,
                Ok(proto::ReplicaRole::Follower) => ReplicaRole::Follower,
                _ => {
                    return Err(Status::internal(format!(
                        "the reply gives database {} no role",
                        status.database
                    )));
                }
            };
            Ok(ServedDatabase {
                database: parse_reply("database name", &status.database)?,
                role,
                epoch: LeaseEpoch::new(status.epoch),
                // An empty position is an empty log, or none reported yet.
                position: (!status.position.is_empty()).then(|| Position::from(status.position)),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    let held = |bytes: Vec<u8>| (!bytes.is_empty()).then(|| Position::from(bytes));
    let sinks = response
        .sinks
        .into_iter()
        .map(|status| {
            Ok(SinkStatus {
                sink: status.sink,
                database: parse_reply("database name", &status.database)?,
                position: held(status.position),
                home: Some(if status.replicated {
                    MarkHome::Replicated
                } else {
                    MarkHome::Local
                }),
                acknowledged: held(status.acknowledged),
                backlog: status.backlog,
                failures: status.failures,
                resets: status.resets,
                last_error: (!status.last_error.is_empty()).then_some(status.last_error),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(NodeStatus {
        node: parse_reply("node id", &response.node_id)?,
        membership_epoch: Epoch::new(response.membership_epoch),
        fingerprint: response.ring_fingerprint.map(Fingerprint::new),
        databases,
        sinks,
    })
}

/// A denylist entry as the protocol carries it.
pub(crate) fn revocation_to_proto(revoked: &Revoked, until: Timestamp) -> proto::Revocation {
    let revoked = match revoked {
        Revoked::Token(id) => proto::revocation::Revoked::TokenId(id.to_string()),
        Revoked::Key(id) => proto::revocation::Revoked::KeyId(id.to_string()),
    };
    proto::Revocation {
        revoked: Some(revoked),
        until_ms: until.as_millis(),
    }
}

/// A denylist entry from the protocol, or `INVALID_ARGUMENT` if it names nothing valid.
pub(crate) fn revocation_from_proto(
    revocation: proto::Revocation,
) -> Result<(Revoked, Timestamp), Status> {
    let revoked = match revocation.revoked {
        Some(proto::revocation::Revoked::TokenId(id)) => Revoked::Token(parse("token id", &id)?),
        Some(proto::revocation::Revoked::KeyId(id)) => Revoked::Key(parse("key id", &id)?),
        None => {
            return Err(Status::invalid_argument(
                "a revocation names a token or a key",
            ));
        }
    };
    Ok((revoked, Timestamp::from_millis(revocation.until_ms)))
}

/// The denylist as the protocol carries it, in order.
pub(crate) fn revocations_to_proto(revocations: &Revocations) -> Vec<proto::Revocation> {
    revocations
        .iter()
        .map(|(revoked, until)| revocation_to_proto(revoked, until))
        .collect()
}

/// The denylist a heartbeat reply carries. An entry the node can't read is skipped with a
/// warning: the rest still apply, and a token whose ID can't be parsed is refused anyway.
pub(crate) fn revocations_from_proto(revocations: Vec<proto::Revocation>) -> Revocations {
    revocations
        .into_iter()
        .filter_map(|revocation| match revocation_from_proto(revocation) {
            Ok(entry) => Some(entry),
            Err(status) => {
                tracing::warn!(
                    reason = status.message(),
                    "skipped an unreadable revocation"
                );
                None
            }
        })
        .collect()
}

/// The first message of a `WatchLeases` stream: every lease `node` holds, in name order.
pub(crate) fn held_leases_to_proto(
    node: &NodeId,
    held: &BTreeMap<DatabaseName, LeaseEpoch>,
) -> proto::LeaseUpdate {
    let leases = held
        .iter()
        .map(|(database, epoch)| proto::HeldLease {
            database: database.to_string(),
            epoch: epoch.get(),
        })
        .collect();
    proto::LeaseUpdate {
        update: Some(Update::Held(proto::HeldLeases {
            node_id: node.to_string(),
            leases,
        })),
    }
}

pub(crate) fn lease_event_to_proto(event: &LeaseEvent) -> proto::LeaseUpdate {
    let kind = match event.change {
        LeaseChange::Gained => proto::LeaseEventKind::Gained,
        LeaseChange::Lost => proto::LeaseEventKind::Lost,
    };
    proto::LeaseUpdate {
        update: Some(Update::Event(proto::LeaseEvent {
            database: event.database.to_string(),
            epoch: event.epoch.get(),
            kind: kind.into(),
        })),
    }
}

/// The update a `WatchLeases` message carries, or `INTERNAL` if it carries none or names an
/// unknown kind of event.
pub(crate) fn lease_update_from_proto(update: proto::LeaseUpdate) -> Result<LeaseUpdate, Status> {
    match update.update {
        Some(Update::Held(snapshot)) => {
            let leases = snapshot
                .leases
                .into_iter()
                .map(|lease| {
                    let database: DatabaseName = parse_reply("database name", &lease.database)?;
                    Ok((database, LeaseEpoch::new(lease.epoch)))
                })
                .collect::<Result<BTreeMap<_, _>, Status>>()?;
            Ok(LeaseUpdate::Held {
                node: parse_reply("node id", &snapshot.node_id)?,
                leases,
            })
        }
        Some(Update::Event(event)) => {
            let change = match proto::LeaseEventKind::try_from(event.kind) {
                Ok(proto::LeaseEventKind::Gained) => LeaseChange::Gained,
                Ok(proto::LeaseEventKind::Lost) => LeaseChange::Lost,
                Ok(proto::LeaseEventKind::Unspecified) | Err(_) => {
                    return Err(Status::internal(format!(
                        "the reply has a lease event of unknown kind {}",
                        event.kind
                    )));
                }
            };
            Ok(LeaseUpdate::Event(LeaseEvent::new(
                parse_reply("database name", &event.database)?,
                LeaseEpoch::new(event.epoch),
                change,
            )))
        }
        None => Err(Status::internal(
            "the reply has a lease update without a kind",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label(key: &str, value: &str) -> proto::Label {
        proto::Label {
            key: key.to_owned(),
            value: value.to_owned(),
        }
    }

    #[test]
    fn labels_round_trip_sorted() {
        let sent = [
            label("zone", "b"),
            label("app.example.com/tier", ""),
            label("a", "1"),
        ];
        let labels = labels_from_proto(&sent).expect("valid labels");
        let keys: Vec<String> = labels_to_proto(&labels)
            .into_iter()
            .map(|label| label.key)
            .collect();
        assert_eq!(keys, ["a", "app.example.com/tier", "zone"]);
        assert_eq!(
            labels_from_proto(&labels_to_proto(&labels)).expect("valid"),
            labels
        );
        for bad in [
            label("-bad", "x"),
            label("fleetring.dev/x", "y"),
            label("a", "b c"),
        ] {
            let refused = labels_from_proto(std::slice::from_ref(&bad)).expect_err("invalid");
            assert_eq!(refused.code(), tonic::Code::InvalidArgument);
            assert!(
                refused
                    .message()
                    .contains(&format!("'{}={}'", bad.key, bad.value)),
                "{refused:?}"
            );
        }
    }

    #[test]
    fn an_update_that_sets_and_removes_one_key_or_changes_nothing_is_refused() {
        let database: DatabaseName = "notes".parse().expect("name");
        let request = |set: Vec<proto::Label>, remove: &[&str]| proto::UpdateDatabaseRequest {
            database: database.to_string(),
            set_labels: set,
            remove_labels: remove.iter().map(|key| (*key).to_owned()).collect(),
            ..proto::UpdateDatabaseRequest::default()
        };
        let both = change_from_proto(&request(vec![label("a", "1")], &["a"])).expect_err("both");
        assert!(both.message().contains("both set and removed"), "{both:?}");
        let nothing = change_from_proto(&request(Vec::new(), &[])).expect_err("empty");
        assert_eq!(nothing.code(), tonic::Code::InvalidArgument);
        let empty_text = proto::UpdateDatabaseRequest {
            description: Some(DescriptionUpdate::SetDescription(String::new())),
            ..request(Vec::new(), &[])
        };
        assert!(change_from_proto(&empty_text).is_err());

        let change = change_from_proto(&proto::UpdateDatabaseRequest {
            description: Some(DescriptionUpdate::SetDescriptionWithoutAuthor(
                "restored".to_owned(),
            )),
            ..request(vec![label("b", "2")], &["a"])
        })
        .expect("a valid patch");
        assert!(matches!(
            change.description,
            DescriptionChange::SetWithoutAuthor(_)
        ));
        let sent = change_to_proto(&database, &change, Some(3)).expect("no expiry");
        assert_eq!(change_from_proto(&sent).expect("valid"), change);
        assert_eq!(sent.if_version, Some(3));
    }

    #[test]
    fn lease_updates_round_trip_and_an_unknown_kind_is_refused() {
        let node: NodeId = "a".parse().expect("id");
        let database: DatabaseName = "db".parse().expect("name");
        let held = BTreeMap::from([(database.clone(), LeaseEpoch::new(3))]);
        let snapshot =
            lease_update_from_proto(held_leases_to_proto(&node, &held)).expect("a valid snapshot");
        assert_eq!(snapshot, LeaseUpdate::Held { node, leases: held });
        let event = LeaseEvent::new(database, LeaseEpoch::new(4), LeaseChange::Lost);
        assert_eq!(
            lease_update_from_proto(lease_event_to_proto(&event)).expect("a valid event"),
            LeaseUpdate::Event(event)
        );
        let unknown = proto::LeaseUpdate {
            update: Some(Update::Event(proto::LeaseEvent {
                database: "db".into(),
                epoch: 1,
                kind: 7,
            })),
        };
        assert_eq!(
            lease_update_from_proto(unknown)
                .expect_err("unknown kind")
                .code(),
            tonic::Code::Internal
        );
        assert_eq!(
            lease_update_from_proto(proto::LeaseUpdate { update: None })
                .expect_err("no kind")
                .code(),
            tonic::Code::Internal
        );
    }

    #[test]
    fn values_round_trip_and_a_missing_value_is_refused() {
        let values = vec![
            Value::Null,
            Value::Integer(-7),
            Value::Real(1.5),
            Value::Text("hi".into()),
            Value::Blob(vec![0, 255]),
        ];
        let wire: Vec<proto::Value> = values.iter().cloned().map(value_to_proto).collect();
        let statement = statement("SELECT ?".into(), wire).expect("valid parameters");
        assert_eq!(statement.params(), values.as_slice());

        let refused = super::statement("SELECT ?".into(), vec![proto::Value { value: None }])
            .expect_err("a parameter without a value");
        assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    }
}
