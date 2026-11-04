//! Version compatibility and fleet features (ADR-0038): which releases may join, how members'
//! software is recorded, and how a command that needs a feature is gated.

use fleetring_control::{
    Action, Applied, Command, Feature, JoinKind, Member, Outcome, Rejection, Release, Software,
    State, apply,
};

use crate::common::{at, config, id};

fn run(state: &mut State, millis: u64, action: Action) -> Outcome {
    apply(state, Command::new(at(millis), action))
}

fn release(text: &str) -> Release {
    text.parse().expect("valid release")
}

/// A member on `version` that supports every feature this release knows.
fn current(version: &str) -> Software {
    Feature::ALL
        .into_iter()
        .fold(Software::new(release(version), 1), Software::with_feature)
}

fn join(node: &str, software: Option<Software>) -> Action {
    let member = Member::new(id(node));
    Action::Join {
        member: match software {
            Some(software) => member.with_software(software),
            None => member,
        },
    }
}

fn fleet(members: &[(&str, Option<Software>)]) -> State {
    let mut state = State::new(config());
    for (node, software) in members {
        run(&mut state, 0, join(node, software.clone())).expect("join");
    }
    state
}

#[test]
fn a_member_that_reports_nothing_runs_the_baseline() {
    let state = fleet(&[("old", None)]);
    let member = state.member(&id("old")).expect("member");
    assert_eq!(member.software(), None);
    assert_eq!(member.running(), Software::baseline());
    assert_eq!(member.running().release(), &release("0.3.0"));
}

#[test]
fn the_next_line_joins_beside_a_member_that_reports_nothing() {
    let mut state = fleet(&[("old", None)]);
    run(&mut state, 0, join("new", Some(current("0.4.0")))).expect("0.4 beside 0.3");
    run(&mut state, 0, join("patch", Some(current("0.3.7")))).expect("0.3.7 beside 0.3");
}

#[test]
fn a_join_two_lines_away_is_refused_naming_both_releases() {
    let mut state = fleet(&[("old", None), ("new", Some(current("0.4.1")))]);
    let before = state.clone();
    let refused = run(&mut state, 5, join("far", Some(current("0.5.0"))))
        .expect_err("0.5 can't run beside 0.3");
    assert_eq!(
        refused,
        Rejection::IncompatibleVersion {
            id: id("far"),
            release: release("0.5.0"),
            member: id("old"),
            member_release: release("0.3.0"),
        }
    );
    assert_eq!(
        refused.to_string(),
        "node 'far' runs fleetringd 0.5.0, which can't run beside node 'old' on 0.3.0: a fleet \
         runs at most two adjacent release lines at once"
    );
    assert_eq!(state, before, "a refused join changes nothing");
    let older = run(&mut state, 5, join("older", Some(current("0.2.9"))))
        .expect_err("0.2 can't run beside 0.4");
    assert!(matches!(older, Rejection::IncompatibleVersion { .. }));
}

#[test]
fn down_members_count_toward_the_window() {
    let mut state = fleet(&[("old", None), ("new", Some(current("0.4.0")))]);
    // Long after "old" stopped sending heartbeats, it is down but still a member.
    let refused = run(&mut state, 60_000, join("far", Some(current("0.5.0"))));
    assert!(matches!(
        refused,
        Err(Rejection::IncompatibleVersion { member, .. }) if member == id("old")
    ));
}

#[test]
fn rejoining_on_a_new_release_is_an_update_that_grows_the_membership_epoch() {
    let mut state = fleet(&[("a", None), ("b", None)]);
    let epoch = state.membership_epoch();
    let rejoined = run(&mut state, 10, join("a", None)).expect("rejoin");
    assert!(matches!(
        rejoined,
        Applied::MemberJoined {
            kind: JoinKind::Rejoined,
            ..
        }
    ));
    assert_eq!(state.membership_epoch(), epoch);
    let upgraded = run(&mut state, 20, join("a", Some(current("0.4.0")))).expect("upgrade");
    assert!(matches!(
        upgraded,
        Applied::MemberJoined {
            kind: JoinKind::Updated,
            ..
        }
    ));
    assert!(state.membership_epoch() > epoch);
    assert_eq!(
        state.member(&id("a")).and_then(Member::software),
        Some(&current("0.4.0"))
    );
}

#[test]
fn a_heartbeat_corrects_the_record_once_and_is_never_refused_for_its_software() {
    // "a" joined a control node too old to record software.
    let mut state = fleet(&[("a", None), ("b", None)]);
    let epoch = state.membership_epoch();
    let heartbeat = |software| Action::Heartbeat {
        id: id("a"),
        replicas: Vec::new(),
        unknown_reports: fleetring_control::UnknownReports::Skip,
        software,
    };
    run(&mut state, 10, heartbeat(Some(current("0.4.0")))).expect("recorded");
    let corrected = state.membership_epoch();
    assert!(corrected > epoch);
    assert_eq!(
        state.member(&id("a")).and_then(Member::software),
        Some(&current("0.4.0"))
    );
    // The same software again changes nothing.
    run(&mut state, 20, heartbeat(Some(current("0.4.0")))).expect("unchanged");
    assert_eq!(state.membership_epoch(), corrected);
    // Software the window would refuse at a join is still recorded: the node is live.
    run(&mut state, 30, heartbeat(Some(current("0.9.0")))).expect("never refused");
    assert_eq!(
        state.member(&id("a")).map(Member::running),
        Some(current("0.9.0"))
    );
}

#[test]
fn a_mixed_fleet_refuses_a_command_a_member_does_not_support() {
    let mut state = fleet(&[("a", Some(current("0.4.0"))), ("b", None)]);
    run(&mut state, 0, Action::Drain { id: id("a") }).expect("drain");
    let before = state.clone();
    let refused = run(&mut state, 10, Action::Undrain { id: id("a") })
        .expect_err("0.3.0 doesn't know undrain");
    assert_eq!(
        refused,
        Rejection::FeatureUnsupported {
            feature: Feature::Undrain,
            id: id("b"),
            release: release("0.3.0"),
        }
    );
    assert_eq!(
        refused.to_string(),
        "the fleet can't use 'undrain' yet: node 'b' runs fleetringd 0.3.0, which doesn't \
         support it; upgrade every member first"
    );
    assert_eq!(state, before, "nothing applied, the time included");
    assert!(state.is_draining(&id("a")));
    assert!(!state.is_enabled(Feature::Undrain));
}

#[test]
fn the_first_command_after_every_member_supports_a_feature_enables_it_for_good() {
    let mut state = fleet(&[("a", Some(current("0.4.0"))), ("b", None)]);
    run(&mut state, 0, Action::Drain { id: id("a") }).expect("drain");
    run(&mut state, 5, join("b", Some(current("0.4.0")))).expect("b upgraded");
    let undrained = run(&mut state, 10, Action::Undrain { id: id("a") }).expect("undrain");
    assert_eq!(undrained, Applied::MemberUndrained { id: id("a") });
    assert!(!state.is_draining(&id("a")));
    assert!(state.is_enabled(Feature::Undrain));
    assert_eq!(state.features().collect::<Vec<_>>(), [Feature::Undrain]);

    // From now on, a member without the feature is refused: rolling "b" back is refused by the
    // fleet, as is a new member on the old release.
    for (node, software) in [("b", None), ("c", Some(Software::new(release("0.4.0"), 1)))] {
        let refused = run(&mut state, 20, join(node, software)).expect_err("lacks undrain");
        assert!(
            matches!(&refused, Rejection::MissingFeature { feature: Feature::Undrain, id: who, .. } if *who == id(node)),
            "{refused:?}"
        );
    }
}

#[test]
fn every_member_live_or_down_must_support_a_feature() {
    let mut state = fleet(&[("a", Some(current("0.4.0"))), ("b", None)]);
    // "b" is down by now, but it is still a member and would come back on 0.3.0.
    run(&mut state, 60_000, join("a", Some(current("0.4.0")))).expect("a heartbeats on");
    assert!(matches!(
        run(&mut state, 60_000, Action::Undrain { id: id("a") }),
        Err(Rejection::FeatureUnsupported { id: who, .. }) if who == id("b")
    ));
}

#[test]
fn the_gate_check_agrees_with_apply() {
    let mut state = fleet(&[("a", Some(current("0.4.0"))), ("b", None)]);
    let undrain = Action::Undrain { id: id("a") };
    assert_eq!(
        state.check_features(&undrain),
        run(&mut state.clone(), 1, undrain.clone()).map(|_| ())
    );
    assert_eq!(state.check_features(&Action::Tick), Ok(()));
    run(&mut state, 5, join("b", Some(current("0.4.0")))).expect("b upgraded");
    assert_eq!(state.check_features(&undrain), Ok(()));
}

#[test]
fn undraining_a_member_that_is_not_draining_changes_nothing_else() {
    let mut state = fleet(&[("a", Some(current("0.4.0")))]);
    let epoch = state.membership_epoch();
    run(&mut state, 1, Action::Undrain { id: id("a") }).expect("undrain");
    assert!(!state.is_draining(&id("a")));
    assert_eq!(state.membership_epoch(), epoch);
    assert!(matches!(
        run(&mut state, 2, Action::Undrain { id: id("z") }),
        Err(Rejection::UnknownMember { .. })
    ));
}

#[test]
fn states_and_commands_without_software_read_as_before() {
    // A 0.3.0 heartbeat and state, as they were logged, still read.
    let heartbeat: Action =
        serde_json::from_str(r#"{"heartbeat":{"id":"a","replicas":[],"unknown_reports":"skip"}}"#)
            .expect("old heartbeat");
    assert!(matches!(
        heartbeat,
        Action::Heartbeat { software: None, .. }
    ));
    let state = fleet(&[("a", None)]);
    let text = serde_json::to_string(&state).expect("serialize");
    assert!(
        !text.contains("software") && !text.contains("features"),
        "{text}"
    );
    let read: State = serde_json::from_str(&text).expect("deserialize");
    assert_eq!(read, state);
}
