//! Conversions between the protocol's messages and the daemon's types.

use std::fmt::Display;
use std::str::FromStr;

use std::collections::BTreeMap;

use fleetring::{Epoch, Fingerprint, NodeId, ReplicationFactor};
use fleetring_control::{
    Acks, DatabaseName, Durability, Engine, LeaseEpoch, Liveness, ReplicaRole, Revocations,
    Revoked, Timestamp,
};
use fleetring_proto::v1::{self as proto, lease_update::Update, value::Value as Kind};
use tonic::Status;

use crate::changes::SinkStatus;
use crate::data::client::{LeaseUpdate, Location, NodeStatus, ReplicaInfo, ServedDatabase};
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
pub(crate) fn lease_snapshot_to_proto(
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
        update: Some(Update::Snapshot(proto::LeaseSnapshot {
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
        Some(Update::Snapshot(snapshot)) => {
            let leases = snapshot
                .leases
                .into_iter()
                .map(|lease| {
                    let database: DatabaseName = parse_reply("database name", &lease.database)?;
                    Ok((database, LeaseEpoch::new(lease.epoch)))
                })
                .collect::<Result<BTreeMap<_, _>, Status>>()?;
            Ok(LeaseUpdate::Snapshot {
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

    #[test]
    fn lease_updates_round_trip_and_an_unknown_kind_is_refused() {
        let node: NodeId = "a".parse().expect("id");
        let database: DatabaseName = "db".parse().expect("name");
        let held = BTreeMap::from([(database.clone(), LeaseEpoch::new(3))]);
        let snapshot = lease_update_from_proto(lease_snapshot_to_proto(&node, &held))
            .expect("a valid snapshot");
        assert_eq!(snapshot, LeaseUpdate::Snapshot { node, leases: held });
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
