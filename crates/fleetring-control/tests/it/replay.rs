//! Replaying a command log, from scratch or from a snapshot, gives the same states and outcomes.

use std::collections::BTreeMap;

use fleetring_control::{Applied, State, apply};

use crate::common::{config, fixed_ops, record};

/// Long enough to cover joins, leaves, leases, and several promotions per database.
const LOG_LEN: usize = 4_000;

#[test]
fn replaying_the_log_gives_identical_states_and_outcomes() {
    let (log, outcomes, final_state) = record(&fixed_ops(0x5EED, LOG_LEN));

    let mut replayed = State::new(config());
    let replayed_outcomes: Vec<_> = log
        .iter()
        .map(|command| apply(&mut replayed, command.clone()))
        .collect();

    assert_eq!(replayed_outcomes, outcomes);
    assert_eq!(replayed, final_state);
}

#[test]
fn replaying_from_a_midway_snapshot_gives_identical_states_and_outcomes() {
    let (log, outcomes, final_state) = record(&fixed_ops(0xC0FFEE, LOG_LEN));

    for split in [1, LOG_LEN / 3, LOG_LEN / 2, LOG_LEN - 1] {
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
            restored, final_state,
            "final state after the snapshot at {split}"
        );
        assert_eq!(
            serde_json::to_string(&restored).expect("state serializes"),
            serde_json::to_string(&final_state).expect("state serializes"),
        );
    }
}

#[test]
fn the_command_log_survives_serialization() {
    let (log, outcomes, final_state) = record(&fixed_ops(7, 500));
    let encoded = serde_json::to_string(&log).expect("log serializes");
    let decoded: Vec<fleetring_control::Command> =
        serde_json::from_str(&encoded).expect("log deserializes");
    assert_eq!(decoded, log);

    let encoded = serde_json::to_string(&outcomes).expect("outcomes serialize");
    let decoded: Vec<fleetring_control::Outcome> =
        serde_json::from_str(&encoded).expect("outcomes deserialize");
    assert_eq!(decoded, outcomes);

    let mut replayed = State::new(config());
    for command in log {
        let _ = apply(&mut replayed, command);
    }
    assert_eq!(replayed, final_state);
}

/// Guards the generator: a replay test over a log that never reaches promotions proves little.
#[test]
fn the_generated_log_exercises_every_kind_of_change() {
    let (_, outcomes, _) = record(&fixed_ops(0x5EED, LOG_LEN));
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for outcome in &outcomes {
        let kind = match outcome {
            Ok(Applied::MemberJoined { .. }) => "joined",
            Ok(Applied::MemberLeft { .. }) => "left",
            Ok(Applied::HeartbeatRecorded { .. }) => "heartbeat",
            Ok(Applied::DatabaseCreated { .. }) => "created",
            Ok(Applied::DatabaseDropped { .. }) => "dropped",
            Ok(Applied::LeaseGranted { .. }) => "granted",
            Ok(Applied::LeaseRenewed { .. }) => "renewed",
            Ok(Applied::PromotionStarted { .. }) => "started",
            Ok(Applied::FenceRecorded { .. }) => "fenced",
            Ok(Applied::PromotionCompleted { .. }) => "completed",
            Ok(_) => "other",
            Err(_) => "rejected",
        };
        *counts.entry(kind).or_default() += 1;
    }
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
        "rejected",
    ] {
        assert!(
            counts.get(kind).copied().unwrap_or(0) > 0,
            "no '{kind}' outcomes in {counts:?}"
        );
    }
}
