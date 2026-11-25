//! The format-1 control log 0.9.0's own code wrote (FR-064, ADR-0040 point 8).

use std::path::{Path, PathBuf};

use fleetring_control::{Command, ControlConfig, Feature, State, apply};
use fleetringd::control::CommandLog;

/// The fixture's directory: `control.db` and `commands.json`, as `fleetringd-v0.9.0` wrote them.
pub(crate) fn directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/control-0.9.0")
}

/// Every command the fixture's log holds, in order.
pub(crate) fn commands() -> Vec<Command> {
    let text = std::fs::read_to_string(directory().join("commands.json"))
        .expect("the fixture's commands can be read");
    serde_json::from_str(&text).expect("the fixture's commands decode")
}

/// The state replaying the fixture's commands with `apply` reaches.
pub(crate) fn replayed() -> State {
    let mut state = State::new(ControlConfig::new());
    for command in commands() {
        let _outcome = apply(&mut state, command);
    }
    state
}

/// Copies the fixture's `control.db` into `dir`, and returns the copy's path.
pub(crate) fn copy_into(dir: &Path) -> PathBuf {
    let path = dir.join("control.db");
    std::fs::copy(directory().join("control.db"), &path).expect("the fixture can be copied");
    path
}

/// Checks that `state` holds what the 0.9.0 writer made: its forks, delegations, kept replica
/// set, incarnations, tombstones, enabled features, and members' software.
pub(crate) fn assert_holds_the_writers_history(state: &State) {
    for feature in Feature::ALL {
        assert!(state.is_enabled(feature), "{feature:?} is enabled");
    }
    assert!(
        state.members().all(|member| member
            .software()
            .is_some_and(|software| software.release().to_string() == "0.9.0")),
        "every member's software survives"
    );
    assert!(
        state.databases().any(|database| database.fork().is_some()),
        "a fork survives"
    );
    assert!(state.delegations().len() > 0, "a delegation survives");
    assert!(
        state
            .databases()
            .any(|database| database.kept_replicas().is_some()),
        "a kept replica set survives"
    );
    assert!(
        state
            .databases()
            .all(|database| database.incarnation().get() != 0),
        "every database has its incarnation"
    );
    assert!(!state.tombstones().is_empty(), "tombstones survive");
}

#[test]
fn the_0_9_0_fixture_recovers_to_its_replay() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = copy_into(dir.path());
    let log = CommandLog::open(&path, ControlConfig::new()).expect("the fixture opens");
    let recovered = log.recover().expect("the fixture recovers");
    assert!(recovered.snapshot.is_some(), "the fixture has a snapshot");
    assert_eq!(
        recovered.last_index.get(),
        u64::try_from(commands().len()).expect("a small log")
    );
    assert_eq!(recovered.state, replayed());
    assert_holds_the_writers_history(&recovered.state);
}
