//! Writes `fields-0.9.0.txt`: every JSON field path the 0.9 line's `State` and `Action` have
//! (ADR-0040 point 5's field rule).
//!
//! `scripts/write-control-fixtures.sh` copies this file into a worktree at `fleetringd-v0.9.0`
//! as `crates/fleetring-control/tests/fields_writer.rs` and runs its ignored `write` there, so the
//! paths are 0.9.0's. `fleetring-control`'s `fields` test includes the same file as a module and
//! computes the paths of this build with the same [`field_paths`], so both lists come from one
//! computation. Like the fixture, the file is frozen: a sample added here later would show an old
//! field as new.
//!
//! The paths come from the 0.9.0 control fixture's commands (`commands.json`): every command's
//! action, and the state after every command, replayed with `apply`. Hand-built actions add the
//! variants and optional fields the fixture's log lacks. A path is dotted, `[]` marks an array's
//! items, and `*` the keys of a map.

use std::collections::BTreeSet;

use fleetring::NodeId;
use fleetring_control::delegation::{
    AncestorTarget, CallerToken, CanonicalRequest, ChainLink, DelegationId, TokenClaims, TokenKind,
};
use fleetring_control::fork::{
    ChainHash, CopyLossReason, CopyLost, ForkExpiry, ForkFailure, ForkIdentity, ForkIntent,
    ForkLimits, OperationId, PlacementCaughtUp, ResetReport, RetryCause, SeedFailure,
    SeedFailureReason, StagedCopy,
};
use fleetring_control::{
    Acks, Action, Command, ControlConfig, DatabaseName, DescriptionChange, Durability, Engine,
    ExpiryChange, Feature, HostedReplica, Incarnation, IncarnationRule, LeaseEpoch, Member,
    MetadataChange, MetadataSpec, Millis, Position, PrincipalName, Release, ReplicaRole,
    ReplicasCaughtUp, Revoked, Software, State, Timestamp, UnknownReports, apply,
};
use serde_json::Value;

/// The objects whose keys are data, not field names: they become `*`. Paths are given with
/// their own map keys already replaced.
const MAPS: [&str; 11] = [
    "state.members",
    "state.members.*.reports",
    "state.databases",
    "state.databases.*.metadata.labels",
    "state.fork_operations",
    "state.delegations",
    "state.delegation_operations",
    "state.tombstones.by_name",
    "action.create_database.metadata.labels",
    "action.update_database.change.set_labels",
    "action.fork_database.intent.labels",
];

/// Adds every path under `value`, which sits at `path`, to `paths`.
fn walk(path: &str, value: &Value, paths: &mut BTreeSet<String>) {
    paths.insert(path.to_owned());
    match value {
        Value::Object(fields) => {
            let is_map = MAPS.contains(&path);
            for (key, child) in fields {
                let key = if is_map { "*" } else { key.as_str() };
                walk(&format!("{path}.{key}"), child, paths);
            }
        }
        Value::Array(items) => {
            for item in items {
                walk(&format!("{path}[]"), item, paths);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

/// Every field path of the states that replaying `commands` passes through, of their actions,
/// and of [`samples`].
pub(crate) fn field_paths(commands: &[Command]) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    let mut state = State::new(ControlConfig::new());
    for command in commands {
        let _outcome = apply(&mut state, command.clone());
        walk(
            "state",
            &serde_json::to_value(&state).expect("a state encodes"),
            &mut paths,
        );
        walk(
            "action",
            &serde_json::to_value(&command.action).expect("an action encodes"),
            &mut paths,
        );
    }
    for action in samples() {
        walk(
            "action",
            &serde_json::to_value(&action).expect("an action encodes"),
            &mut paths,
        );
    }
    paths
}

fn id(name: &str) -> NodeId {
    name.parse().expect("a node id")
}

fn db(name: &str) -> DatabaseName {
    name.parse().expect("a database name")
}

fn principal(name: &str) -> PrincipalName {
    name.parse().expect("a principal name")
}

fn epoch(value: u64) -> LeaseEpoch {
    LeaseEpoch::new(value)
}

fn fork() -> ForkIdentity {
    ForkIdentity::new(
        db("sample-fork"),
        Incarnation::new(7),
        OperationId::from_bytes([1; 16]),
    )
}

fn labels() -> fleetring_control::Labels {
    let (key, value) = fleetring_control::parse_label("team=platform").expect("a label");
    fleetring_control::Labels::try_from_pairs([(key, value)]).expect("labels")
}

fn caller() -> CallerToken {
    CallerToken {
        claims: TokenClaims {
            fleet: "acme".into(),
            principal: principal("agent-1.child"),
            id: "token-1".parse().expect("a token id"),
            issued_at: Some(Timestamp::from_millis(1)),
            not_before: Timestamp::from_millis(1),
            expires_at: Timestamp::from_millis(2),
            scopes: "admin:agent-1.child-*"
                .parse::<fleetring_control::scope::Scope>()
                .map(|scope| [scope].into_iter().collect())
                .expect("a scope"),
            kind: Some(TokenKind::Agent),
            owner: Some(principal("sam")),
            parent: Some(principal("agent-1")),
            chain: vec![ChainLink {
                id: "token-0".parse().expect("a token id"),
                kid: "issuer-a".parse().expect("a key id"),
            }],
        },
        kid: "issuer-a".parse().expect("a key id"),
    }
}

/// Every action variant the fixture's log lacks, and the variants it has with every optional
/// field set.
fn samples() -> Vec<Action> {
    let software = Feature::ALL.into_iter().fold(
        Software::new(Release::new(0, 9, 0), 1),
        Software::with_feature,
    );
    let replica = HostedReplica::new(db("sample"), ReplicaRole::Primary, Position::new([1]))
        .with_log_epoch(epoch(1))
        .with_size_bytes(1)
        .with_staged(StagedCopy::new(
            fork(),
            1,
            Position::new([1]),
            ChainHash::new([1]),
            Incarnation::new(3),
        ))
        .with_copy_lost(CopyLost::new(fork(), 1, CopyLossReason::Missing))
        .with_seed_failure(SeedFailure::new(
            fork(),
            1,
            SeedFailureReason::TransferFailed,
        ))
        .with_reset(ResetReport::committed(fork(), epoch(1), Position::new([1])))
        .with_placement_caught_up(PlacementCaughtUp::new(
            fork(),
            epoch(1),
            fleetring::Epoch::new(1),
        ))
        .with_replicas_caught_up(ReplicasCaughtUp::new(
            Incarnation::new(3),
            epoch(1),
            fleetring::Epoch::new(1),
        ));
    let durability = Durability::new(
        Acks::All,
        fleetring::ReplicationFactor::new(3).expect("nonzero"),
    );
    let mut intent = ForkIntent::new(
        db("sample-fork"),
        db("sample"),
        fleetring::ReplicationFactor::new(3).expect("nonzero"),
    );
    intent.expect_source_incarnation = Some(Incarnation::new(3));
    intent.durability = Some(durability);
    intent.description = Some("a fork".parse().expect("a description"));
    intent.labels = labels();
    intent.expiry = ForkExpiry::After(Millis::new(1));
    intent.requested_by = Some(principal("sam"));
    vec![
        Action::Join {
            member: Member::new(id("n1"))
                .with_zone("a".parse().expect("a zone"))
                .with_host("h1".parse().expect("a host"))
                .with_weight(fleetring::Weight::new(2).expect("nonzero"))
                .with_address("n1.internal:7400")
                .with_software(software.clone()),
        },
        Action::Heartbeat {
            id: id("n1"),
            replicas: vec![replica],
            unknown_reports: UnknownReports::Skip,
            software: Some(software),
            disk_free_bytes: Some(1),
        },
        Action::CreateDatabase {
            name: db("sample"),
            replication_factor: fleetring::ReplicationFactor::new(3).expect("nonzero"),
            engine: Engine::Sqlite,
            durability: Some(durability),
            metadata: Some(MetadataSpec {
                description: Some("a database".parse().expect("a description")),
                labels: labels(),
                expires_at: Some(Timestamp::from_millis(1)),
                created_by: Some(principal("sam")),
            }),
            incarnation: Incarnation::new(3),
            incarnation_rule: IncarnationRule::Always,
        },
        Action::UpdateDatabase {
            name: db("sample"),
            change: MetadataChange {
                description: DescriptionChange::Set("a database".parse().expect("a description")),
                set_labels: labels(),
                remove_labels: BTreeSet::from(["old".parse().expect("a label key")]),
                expiry: ExpiryChange::Set(Timestamp::from_millis(1)),
            },
            by: Some(principal("sam")),
            if_version: Some(1),
        },
        Action::ChangeDurability {
            database: db("sample"),
            durability,
        },
        Action::StartPromotion {
            database: db("sample"),
        },
        Action::RecordFenced {
            database: db("sample"),
            node: id("n1"),
            epoch: epoch(1),
            position: Position::new([1]),
        },
        Action::CompletePromotion {
            database: db("sample"),
            epoch: epoch(2),
        },
        Action::TransferLease {
            database: db("sample"),
            target: Some(id("n2")),
        },
        Action::RevokeToken {
            revoked: Revoked::Key("issuer-a".parse().expect("a key id")),
            until: Timestamp::from_millis(1),
        },
        Action::RevokeToken {
            revoked: Revoked::Principal {
                fleet: Some("acme".parse().expect("a fleet")),
                principal: principal("agent-1"),
            },
            until: Timestamp::from_millis(1),
        },
        Action::ForkDatabase {
            operation: OperationId::from_bytes([1; 16]),
            intent,
            incarnation: Incarnation::new(7),
            resolved_expiry: Some(Timestamp::from_millis(1)),
            limits: ForkLimits {
                max_attempts: 3,
                deadline: Millis::new(1),
                attempt_timeout: Millis::new(1),
                seeder_timeout: Millis::new(1),
                operation_retention: Millis::new(1),
                admission_min_free_bytes: Some(1),
            },
        },
        Action::RetryForkSeed {
            fork: fork(),
            attempt: 1,
            cause: RetryCause::SeederDown,
            exclude_seeder: Some(id("n1")),
        },
        Action::RecordForkDiskRefusal {
            fork: fork(),
            attempt: 1,
            node: id("n1"),
        },
        Action::DropForkHolder {
            fork: fork(),
            attempt: 1,
            holder: id("n1"),
            cause: RetryCause::HolderLost,
        },
        Action::FailFork {
            fork: fork(),
            attempt: 1,
            reason: ForkFailure::RetriesExhausted {
                last: RetryCause::AttemptTimedOut,
            },
        },
        Action::RecordDelegation {
            operation: OperationId::from_bytes([2; 16]),
            requested_by: principal("agent-1.child"),
            request: CanonicalRequest {
                segment: "c1".into(),
                scopes: Some(caller().claims.scopes),
                lifetime: Some(Millis::new(1)),
                not_before: Some(Timestamp::from_millis(1)),
                caller_token: "token-1".parse().expect("a token id"),
            },
            caller: caller(),
            id: DelegationId::from_bytes([3; 16]),
            scopes: caller().claims.scopes,
            not_before: Timestamp::from_millis(1),
            expires_at: Timestamp::from_millis(2),
        },
        Action::RevokeAsAncestor {
            target: AncestorTarget::PrincipalAndToken {
                principal: "agent-1.child".parse().expect("an agent name"),
                token: "token-1".parse().expect("a token id"),
            },
            caller: caller(),
        },
    ]
}

/// Writes `$FIXTURE_OUT/fields-0.9.0.txt` from the commands in `$FIXTURE_COMMANDS`.
#[test]
#[ignore = "writes fields-0.9.0.txt; scripts/write-control-fixtures.sh runs it at the tag"]
fn write() {
    let commands = std::env::var("FIXTURE_COMMANDS").expect("FIXTURE_COMMANDS names a file");
    let out = std::env::var("FIXTURE_OUT").expect("FIXTURE_OUT names a directory");
    let text = std::fs::read_to_string(commands).expect("the commands can be read");
    let commands: Vec<Command> = serde_json::from_str(&text).expect("the commands decode");
    let mut lines = String::new();
    for path in field_paths(&commands) {
        lines.push_str(&path);
        lines.push('\n');
    }
    std::fs::write(std::path::Path::new(&out).join("fields-0.9.0.txt"), lines)
        .expect("fields-0.9.0.txt can be written");
}
