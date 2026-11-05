//! Replaying a command log, from scratch or from a snapshot, gives the same states and outcomes.

use std::collections::BTreeMap;

use fleetring_control::{Action, Applied, Command, Feature, Outcome, State, apply};

use crate::common::{SoftwareMix, config, fixed_ops, record, record_with};

/// Long enough to cover joins, leaves, leases, and several promotions per database.
const LOG_LEN: usize = 4_000;

/// Replays `log` from scratch and checks it reaches `final_state` with the same `outcomes`.
fn check_replay_from_scratch(log: &[Command], outcomes: &[Outcome], final_state: &State) {
    let mut replayed = State::new(config());
    let replayed_outcomes: Vec<_> = log
        .iter()
        .map(|command| apply(&mut replayed, command.clone()))
        .collect();

    assert_eq!(replayed_outcomes, outcomes);
    assert_eq!(replayed, *final_state);
}

/// Snapshots the state at several points of `log`, restores each snapshot, and replays the rest
/// of the log from it.
fn check_replay_from_snapshots(log: &[Command], outcomes: &[Outcome], final_state: &State) {
    for split in [1, log.len() / 3, log.len() / 2, log.len() - 1] {
        let mut state = State::new(config());
        for command in &log[..split] {
            let _ = apply(&mut state, command.clone());
        }
        let snapshot = serde_json::to_string(&state).expect("state serializes");
        let mut restored: State = serde_json::from_str(&snapshot).expect("snapshot restores");
        assert_eq!(restored, state, "snapshot at {split} restores exactly");

        let rest: Vec<_> = log[split..]
            .iter()
            .map(|command| apply(&mut restored, command.clone()))
            .collect();
        assert_eq!(
            rest,
            outcomes[split..],
            "outcomes after the snapshot at {split}"
        );
        assert_eq!(
            restored, *final_state,
            "final state after the snapshot at {split}"
        );
        assert_eq!(
            serde_json::to_string(&restored).expect("state serializes"),
            serde_json::to_string(final_state).expect("state serializes"),
        );
    }
}

/// Serializes `log` and its `outcomes`, decodes them, and replays the decoded log.
fn check_replay_through_serialization(log: &[Command], outcomes: &[Outcome], final_state: &State) {
    let encoded = serde_json::to_string(log).expect("log serializes");
    let decoded: Vec<Command> = serde_json::from_str(&encoded).expect("log deserializes");
    assert_eq!(decoded, log);

    let encoded = serde_json::to_string(outcomes).expect("outcomes serialize");
    let decoded_outcomes: Vec<Outcome> =
        serde_json::from_str(&encoded).expect("outcomes deserialize");
    assert_eq!(decoded_outcomes, outcomes);

    let mut replayed = State::new(config());
    for command in decoded {
        let _ = apply(&mut replayed, command);
    }
    assert_eq!(replayed, *final_state);
}

#[test]
fn replaying_the_log_gives_identical_states_and_outcomes() {
    let (log, outcomes, final_state) = record(&fixed_ops(0x5EED, LOG_LEN));
    check_replay_from_scratch(&log, &outcomes, &final_state);
}

#[test]
fn replaying_from_a_midway_snapshot_gives_identical_states_and_outcomes() {
    let (log, outcomes, final_state) = record(&fixed_ops(0xC0FFEE, LOG_LEN));
    check_replay_from_snapshots(&log, &outcomes, &final_state);
}

#[test]
fn the_command_log_survives_serialization() {
    let (log, outcomes, final_state) = record(&fixed_ops(7, 500));
    check_replay_through_serialization(&log, &outcomes, &final_state);
}

/// Metadata, its updates, and expiries replay to the same states and outcomes: `apply` decides
/// them by the command's time alone.
#[test]
fn replaying_a_log_with_metadata_and_expiry_gives_identical_states() {
    let (log, outcomes, final_state) =
        record_with(&fixed_ops(0xA6E7, LOG_LEN), SoftwareMix::Current);
    check_replay_from_scratch(&log, &outcomes, &final_state);
    check_replay_from_snapshots(&log, &outcomes, &final_state);
    check_replay_through_serialization(&log, &outcomes, &final_state);
    assert!(final_state.is_enabled(Feature::Metadata));
    assert!(final_state.is_enabled(Feature::Expiry));
}

/// `command` as a control node of the previous release line decodes it from the log: its types
/// have no `metadata` fields, so they are dropped.
fn without_metadata(command: &Command) -> Command {
    let mut value = serde_json::to_value(command).expect("a command serializes");
    if let Some(bodies) = value
        .get_mut("action")
        .and_then(serde_json::Value::as_object_mut)
    {
        for body in bodies
            .values_mut()
            .filter_map(serde_json::Value::as_object_mut)
        {
            body.remove("metadata");
        }
    }
    serde_json::from_value(value).expect("a command without metadata decodes")
}

/// The dropped-field rule (ADR-0040, point 5; ADR-0041, point 3): every new field is set only by
/// commands that need `metadata`. A control node of the previous line, after a rollback, replays
/// the log up to the first such command, dropping the fields it doesn't know, and reaches
/// exactly the state this build had there; no database record holds metadata yet.
#[test]
fn a_log_replayed_without_metadata_matches_until_metadata_is_first_used() {
    let (log, _, _) = record_with(&fixed_ops(0xA6E7, LOG_LEN), SoftwareMix::Current);
    let first = log
        .iter()
        .position(|command| command.action.features().contains(&Feature::Metadata))
        .expect("the log uses metadata");
    assert!(first > 0, "the log starts with a metadata command");

    let mut current = State::new(config());
    let mut previous = State::new(config());
    for command in &log[..first] {
        let stripped = without_metadata(command);
        assert_eq!(&stripped, command, "{command:?} carries metadata");
        let outcome = apply(&mut current, command.clone());
        assert_eq!(apply(&mut previous, stripped), outcome);
    }
    assert_eq!(previous, current);
    let databases: Vec<_> = current.databases().collect();
    assert!(!databases.is_empty(), "no database to compare");
    assert!(databases.iter().all(|db| db.metadata().is_none()));
    let snapshot = serde_json::to_value(&current).expect("state serializes");
    for record in snapshot["databases"]
        .as_object()
        .expect("databases")
        .values()
    {
        assert!(record.get("metadata").is_none(), "{record}");
    }

    // The first metadata command is one the previous line can't act on: a creation that would
    // lose its metadata if decoded without it, or an action it can't decode at all.
    let changed = match &log[first].action {
        Action::CreateDatabase { .. } => without_metadata(&log[first]) != log[first],
        Action::UpdateDatabase { .. } | Action::ExpireDatabase { .. } => true,
        other => panic!("{other:?} doesn't need metadata"),
    };
    assert!(changed);
}

/// Guards the generator: a replay test over a log that never reaches promotions proves little.
#[test]
fn the_generated_log_exercises_every_kind_of_change() {
    let (_, outcomes, _) = record(&fixed_ops(0x5EED, LOG_LEN));
    let counts = count_outcomes(&outcomes, None);
    for kind in [
        "joined",
        "left",
        "heartbeat",
        "created",
        "dropped",
        "granted",
        "renewed",
        "started",
        "fenced",
        "completed",
        "draining",
        "revoked",
        "rejected",
    ] {
        assert!(
            counts.get(kind).copied().unwrap_or(0) > 0,
            "no '{kind}' outcomes in {counts:?}"
        );
    }

    let ops = fixed_ops(0xA6E7, LOG_LEN);
    let (log, outcomes, _) = record_with(&ops, SoftwareMix::Current);
    let counts = count_outcomes(&outcomes, Some(&log));
    for kind in [
        "created",
        "created with metadata",
        "updated",
        "expired",
        "dropped",
        "granted",
        "completed",
        "metadata conflict",
        "expiry passed",
        "not expired",
    ] {
        assert!(
            counts.get(kind).copied().unwrap_or(0) > 0,
            "no '{kind}' outcomes among current members in {counts:?}"
        );
    }
}

/// Counts outcomes by kind. Given the `log`, it also counts creations that recorded metadata,
/// by replaying the log to see each created database.
fn count_outcomes(outcomes: &[Outcome], log: Option<&[Command]>) -> BTreeMap<&'static str, usize> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    let mut state = State::new(config());
    for (index, outcome) in outcomes.iter().enumerate() {
        if let Some(log) = log {
            let _ = apply(&mut state, log[index].clone());
        }
        let kind = match outcome {
            Ok(Applied::MemberJoined { .. }) => "joined",
            Ok(Applied::MemberLeft { .. }) => "left",
            Ok(Applied::HeartbeatRecorded { .. }) => "heartbeat",
            Ok(Applied::DatabaseCreated { database }) => {
                let with_metadata = state
                    .database(database)
                    .is_some_and(|db| db.metadata().is_some());
                if with_metadata {
                    *counts.entry("created with metadata").or_default() += 1;
                }
                "created"
            }
            Ok(Applied::DatabaseDropped { .. }) => "dropped",
            Ok(Applied::LeaseGranted { .. }) => "granted",
            Ok(Applied::LeaseRenewed { .. }) => "renewed",
            Ok(Applied::PromotionStarted { .. }) => "started",
            Ok(Applied::FenceRecorded { .. }) => "fenced",
            Ok(Applied::PromotionCompleted { .. }) => "completed",
            Ok(Applied::MemberDraining { .. }) => "draining",
            Ok(Applied::TokenRevoked { .. }) => "revoked",
            Ok(Applied::DatabaseUpdated { .. }) => "updated",
            Ok(Applied::DatabaseExpired { .. }) => "expired",
            Ok(_) => "other",
            Err(fleetring_control::Rejection::MetadataConflict { .. }) => "metadata conflict",
            Err(fleetring_control::Rejection::ExpiryPassed { .. }) => "expiry passed",
            Err(fleetring_control::Rejection::NotExpired { .. }) => "not expired",
            Err(_) => "rejected",
        };
        *counts.entry(kind).or_default() += 1;
    }
    counts
}
