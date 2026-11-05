//! A control node rolled back to the 0.5 line reads this build's control log and snapshots as
//! long as no command has used a new feature (ADR-0038, point 4; ADR-0041, amendment 1).
//!
//! The module `v0_5` holds release 0.5.0's own definitions, copied with their serde attributes
//! from the `fleetringd-v0.5.0` tag, of every type on the decode path that this build changed:
//! `Feature`, `Software`, `Member`, `MemberEntry`, `State`, the stored database record, and the
//! two actions that carry software. Every other type on the path is unchanged since that tag, so
//! the tests use this crate's own.

use std::collections::BTreeMap;

use fleetring_control::{
    Action, Command, Feature, Member, Rejection, Release, Software, State, apply,
};

use crate::common::{at, config, create, current_software, db, id, run};

/// Release 0.5.0's types, as the tag defines them.
#[expect(
    dead_code,
    reason = "0.5.0's definitions are kept whole; the tests read only some of their fields"
)]
mod v0_5 {
    use std::collections::{BTreeMap, BTreeSet};

    use fleetring::{Epoch, Host, NodeId, ReplicationFactor, Weight, Zone};
    use fleetring_control::{
        Acks, ControlConfig, DatabaseName, Durability, Engine, HostedReplica, Lease, LeaseEpoch,
        Promotion, Release, Report, Revocations, Timestamp, UnknownReports,
    };
    use serde::Deserialize;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum Feature {
        Undrain,
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct Software {
        pub(crate) release: Release,
        pub(crate) control_format: u32,
        #[serde(default)]
        pub(crate) features: BTreeSet<Feature>,
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct Member {
        pub(crate) id: NodeId,
        pub(crate) zone: Option<Zone>,
        pub(crate) host: Option<Host>,
        pub(crate) weight: Weight,
        #[serde(default)]
        pub(crate) address: Option<String>,
        #[serde(default)]
        pub(crate) software: Option<Software>,
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct MemberEntry {
        pub(crate) member: Member,
        pub(crate) last_heartbeat: Timestamp,
        pub(crate) reports: BTreeMap<DatabaseName, Report>,
        #[serde(default)]
        pub(crate) draining: bool,
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct DatabaseRecord {
        pub(crate) name: DatabaseName,
        pub(crate) replication_factor: ReplicationFactor,
        pub(crate) engine: Engine,
        #[serde(default)]
        pub(crate) acks: Acks,
        #[serde(default)]
        pub(crate) min_replicas: Option<u8>,
        #[serde(default)]
        pub(crate) pending_durability: Option<Durability>,
        pub(crate) epoch: LeaseEpoch,
        pub(crate) lease: Option<Lease>,
        pub(crate) promotion: Option<Promotion>,
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct State {
        pub(crate) config: ControlConfig,
        pub(crate) now: Timestamp,
        pub(crate) membership_epoch: Epoch,
        pub(crate) members: BTreeMap<NodeId, MemberEntry>,
        pub(crate) databases: BTreeMap<DatabaseName, DatabaseRecord>,
        #[serde(default)]
        pub(crate) revocations: Revocations,
        #[serde(default)]
        pub(crate) features: BTreeSet<Feature>,
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct Command {
        pub(crate) at: Timestamp,
        pub(crate) action: Action,
    }

    /// The two of 0.5.0's actions that carry software; the others are unchanged.
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum Action {
        Join {
            member: Member,
        },
        Heartbeat {
            id: NodeId,
            replicas: Vec<HostedReplica>,
            #[serde(default = "reject")]
            unknown_reports: UnknownReports,
            #[serde(default)]
            software: Option<Software>,
        },
    }

    /// 0.5.0's rule for a logged heartbeat that predates the field.
    fn reject() -> UnknownReports {
        UnknownReports::Reject
    }
}

/// This build's software with a name from a release after it, as a newer member's software is
/// kept in the log.
fn current_with_a_future_feature() -> Software {
    let mut stored = serde_json::to_value(current_software()).expect("software serializes");
    stored["newer_features"]
        .as_array_mut()
        .expect("this build has newer features")
        .push("future-x".into());
    serde_json::from_value(stored).expect("software decodes")
}

#[test]
fn the_0_5_line_decodes_joins_heartbeats_and_snapshots_of_members_with_new_features() {
    let mut state = State::new(config());
    let software = current_with_a_future_feature();
    let join = Command::new(
        at(1),
        Action::Join {
            member: Member::new(id("new")).with_software(software.clone()),
        },
    );
    let heartbeat = Command::new(
        at(2),
        Action::Heartbeat {
            id: id("new"),
            replicas: Vec::new(),
            unknown_reports: fleetring_control::UnknownReports::Skip,
            software: Some(software),
        },
    );
    apply(&mut state, join.clone()).expect("join");
    apply(&mut state, heartbeat.clone()).expect("heartbeat");
    run(&mut state, 3, create("plain", 1)).expect("create");

    let undrain_only = |software: Option<&v0_5::Software>| {
        let software = software.expect("the member reports software");
        assert_eq!(software.release, Release::new(0, 6, 0));
        assert_eq!(
            software.features.iter().copied().collect::<Vec<_>>(),
            [v0_5::Feature::Undrain]
        );
    };

    let logged = serde_json::to_string(&join).expect("serializes");
    let decoded: v0_5::Command = serde_json::from_str(&logged).expect("0.5 decodes the join");
    let v0_5::Action::Join { member } = decoded.action else {
        panic!("a join decodes as a join: {decoded:?}");
    };
    undrain_only(member.software.as_ref());

    let logged = serde_json::to_string(&heartbeat).expect("serializes");
    let decoded: v0_5::Command = serde_json::from_str(&logged).expect("0.5 decodes the heartbeat");
    let v0_5::Action::Heartbeat { software, .. } = decoded.action else {
        panic!("a heartbeat decodes as a heartbeat: {decoded:?}");
    };
    undrain_only(software.as_ref());

    let snapshot = serde_json::to_string(&state).expect("serializes");
    let decoded: v0_5::State = serde_json::from_str(&snapshot).expect("0.5 decodes the snapshot");
    let entry = decoded.members.get(&id("new")).expect("the member");
    undrain_only(entry.member.software.as_ref());
    assert!(decoded.features.is_empty());
    assert!(decoded.databases.contains_key(&db("plain")));
}

/// The vendored types are as strict as 0.5.0's: had this build written every feature in
/// `features`, as before the amendment, 0.5 couldn't have read the log at all.
#[test]
fn the_0_5_line_refuses_a_feature_name_it_does_not_know_in_features() {
    let before_the_amendment =
        r#"{"release":"0.6.0","control_format":1,"features":["metadata","undrain"]}"#;
    assert!(serde_json::from_str::<v0_5::Software>(before_the_amendment).is_err());
    let after = serde_json::to_string(&current_software()).expect("serializes");
    assert!(serde_json::from_str::<v0_5::Software>(&after).is_ok());
}

#[test]
fn unknown_feature_names_round_trip_through_the_log_and_snapshots() {
    let mut state = State::new(config());
    let join = Command::new(
        at(1),
        Action::Join {
            member: Member::new(id("next")).with_software(current_with_a_future_feature()),
        },
    );
    let logged = serde_json::to_string(&join).expect("serializes");
    let decoded: Command = serde_json::from_str(&logged).expect("decodes");
    assert_eq!(decoded, join);
    apply(&mut state, decoded).expect("join");

    let snapshot = serde_json::to_string(&state).expect("serializes");
    let restored: State = serde_json::from_str(&snapshot).expect("restores");
    assert_eq!(restored, state);
    let software = restored
        .member(&id("next"))
        .and_then(Member::software)
        .expect("software");
    assert_eq!(
        software.unknown_features().collect::<Vec<_>>(),
        ["future-x"]
    );
    assert_eq!(
        serde_json::to_string(&restored).expect("serializes"),
        snapshot
    );
}

/// A name this release doesn't know never counts as a feature: a member that reports only
/// unknown names lacks every feature here, and the gate refuses what it can't act on.
#[test]
fn screening_ignores_unknown_feature_names() {
    let mut state = State::new(config());
    let unknown_only: Software = serde_json::from_str(
        r#"{"release":"0.6.0","control_format":1,"features":["future-x"],"newer_features":["metadatas","expiry-v2"]}"#,
    )
    .expect("decodes");
    assert_eq!(unknown_only.features().count(), 0);
    assert!(Feature::ALL.into_iter().all(|f| !unknown_only.supports(f)));
    for (node, software) in [("a", current_software()), ("b", unknown_only)] {
        let member = Member::new(id(node)).with_software(software);
        run(&mut state, 0, Action::Join { member }).expect("join");
    }
    let labelled = Action::UpdateDatabase {
        name: db("any"),
        change: fleetring_control::MetadataChange::default(),
        by: None,
        if_version: None,
    };
    for action in [labelled, Action::Undrain { id: id("a") }] {
        let refused = state
            .check_admission(&action)
            .expect_err("b supports nothing");
        assert!(
            matches!(&refused, Rejection::FeatureUnsupported { id, .. } if id.as_str() == "b"),
            "{refused:?}"
        );
    }
    let members: BTreeMap<_, _> = state
        .members()
        .map(|member| (member.id().clone(), member.running().features().count()))
        .collect();
    assert_eq!(members.get(&id("b")), Some(&0));
}
