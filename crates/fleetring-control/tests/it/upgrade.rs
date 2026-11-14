//! FR-096: a fleet upgrading from 0.7 to 0.8 (ADR-0038, point 4; ADR-0046, point 8; ADR-0047,
//! point 12; ADR-0031, amendment 2, point 6).
//!
//! While a member runs 0.7.0, every command that needs `fork`, `delegation` or `handover` is
//! refused before it is logged, naming that member, and a membership change moves replica sets
//! at once, as on 0.7. Once every member runs 0.8 and reports all three, nothing is enabled until
//! the command that needs a feature first runs, and that command enables only its own feature.
//! Until then, the log and the snapshots decode on 0.7.0, so a node can roll back; after, they
//! don't, so rollback is refused. The 0.7.0 decoder is `rollback::v0_7`, copied from the tag.

use fleetring::NodeId;
use fleetring_control::delegation::AncestorTarget;
use fleetring_control::{
    Action, Applied, Command, Feature, Incarnation, Member, Outcome, Rejection, Release, Revoked,
    Software, State, Timestamp, UnknownReports, apply,
};

use crate::common::delegation::{
    Ask, RECORDER, as_ancestor, leaf, principal, principal_target, root_token, token,
};
use crate::common::fork::{SOURCE, create_at, fork_of, fork_request};
use crate::common::{at, config, create, db, id};
use crate::rollback::v0_7;

/// The leased databases a membership change may move.
const LEASED: [&str; 10] = ["t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9"];

/// What a 0.7.0 node reports: its release and its three features, none of 0.8's.
fn v0_7_0() -> Software {
    [Feature::Undrain, Feature::Metadata, Feature::Expiry]
        .into_iter()
        .fold(
            Software::new(Release::new(0, 7, 0), 1),
            Software::with_feature,
        )
}

/// What a 0.8.0 node reports: every feature this build knows.
fn v0_8_0() -> Software {
    Feature::ALL.into_iter().fold(
        Software::new(Release::new(0, 8, 0), 1),
        Software::with_feature,
    )
}

/// A control node's view of the upgrade: its state and the log its writer kept.
struct Fleet {
    state: State,
    log: Vec<Command>,
    clock: u64,
}

impl Fleet {
    /// Members `n0` and `n1` on 0.8.0 and `n2` on `n2`'s software, with [`SOURCE`] and every
    /// database in [`LEASED`] created at three replicas and leased to their first replica.
    fn new(n2: &Software) -> Self {
        let mut fleet = Self {
            state: State::new(config()),
            log: Vec::new(),
            clock: 0,
        };
        for (node, software) in [("n0", v0_8_0()), ("n1", v0_8_0()), ("n2", n2.clone())] {
            let member = Member::new(id(node)).with_software(software);
            fleet.run(Action::Join { member }).expect("join");
        }
        fleet.run(create(SOURCE, 3)).expect("create the source");
        for name in LEASED {
            fleet.run(create(name, 3)).expect("create");
            let node = fleet.replica_set(name)[0].clone();
            fleet
                .run(Action::AcquireLease {
                    database: db(name),
                    node,
                })
                .expect("lease");
        }
        fleet
    }

    /// Screens and logs `action` as the control node's writer does: a command the gate refuses
    /// is never logged; one it admits is logged and applied.
    fn run(&mut self, action: Action) -> Outcome {
        self.clock += 10;
        self.state.check_admission(&action)?;
        let command = Command::stamped(at(self.clock), id(RECORDER), action);
        self.log.push(command.clone());
        apply(&mut self.state, command)
    }

    /// Runs the membership change `action` as the writer logs it: preceded by `EnableHandover`
    /// when the change would keep a replica set.
    fn change(&mut self, action: Action) -> Outcome {
        if self.state.handover_due(&action) {
            assert_eq!(
                self.run(Action::EnableHandover),
                Ok(Applied::HandoverEnabled)
            );
        }
        self.run(action)
    }

    /// Expects the gate to refuse `action` without logging it or changing the state.
    fn refused(&mut self, action: Action) -> Rejection {
        let before = serde_json::to_string(&self.state).expect("serializes");
        let logged = self.log.len();
        let rejection = self.run(action).expect_err("refused");
        assert_eq!(self.log.len(), logged, "{rejection:?} was logged");
        assert_eq!(
            serde_json::to_string(&self.state).expect("serializes"),
            before,
            "{rejection:?} changed the state"
        );
        rejection
    }

    /// `node` heartbeats with `software`.
    fn heartbeat(&mut self, node: &str, software: Software) {
        self.run(Action::Heartbeat {
            id: id(node),
            replicas: Vec::new(),
            unknown_reports: UnknownReports::Skip,
            software: Some(software),
            disk_free_bytes: None,
        })
        .expect("heartbeat");
    }

    fn replica_set(&self, name: &str) -> Vec<NodeId> {
        self.state.replica_set(&db(name)).expect("placed")
    }

    /// The 0.8 features enabled: 0.7's own, such as `metadata`, which a fork enables beside
    /// `fork`, don't hold a rollback back.
    fn enabled(&self) -> Vec<Feature> {
        self.state
            .features()
            .filter(|feature| !v0_7_0().supports(*feature))
            .collect()
    }

    /// Asserts that a 0.7.0 control node decodes the whole log and the current snapshot, so a
    /// rollback to 0.7 is safe.
    fn assert_0_7_reads_it(&self) {
        for command in &self.log {
            if let Err(error) = v0_7::decodes_command(command) {
                panic!("0.7 can't decode {command:?}: {error}");
            }
        }
        if let Err(error) = v0_7::decodes_state(&self.state) {
            panic!("0.7 can't decode the snapshot: {error}");
        }
    }

    /// Asserts that a 0.7.0 control node refuses the snapshot, naming `feature`, and the last
    /// command logged, which enabled it.
    fn assert_0_7_refuses_it(&self, feature: Feature) {
        assert!(self.enabled().contains(&feature), "{feature} isn't enabled");
        let error = v0_7::decodes_state(&self.state).expect_err("0.7 refuses the snapshot");
        assert!(
            self.enabled()
                .iter()
                .any(|enabled| error.to_string().contains(enabled.as_str())),
            "{error}"
        );
        let last = self.log.last().expect("a command");
        assert!(v0_7::decodes_command(last).is_err(), "{last:?}");
    }
}

/// `node`'s join, reporting `software`.
fn join(node: &str, software: Software) -> Action {
    Action::Join {
        member: Member::new(id(node)).with_software(software),
    }
}

/// A child delegation of the root agent `agent-42`, asked with its root token.
fn delegation() -> Action {
    Ask::new(1, "c7", leaf("agent-42.c7"), 2 * 3_600_000).action(&root_token("agent-42", "root"))
}

/// Every command that needs a 0.8 feature, with the feature.
fn commands_that_need_0_8() -> Vec<(Feature, Action)> {
    let root = root_token("agent-42", "root");
    vec![
        (Feature::Fork, fork_request(1, "fork")),
        (Feature::Delegation, delegation()),
        (
            Feature::Delegation,
            Action::RevokePrincipal {
                principal: principal("agent-42"),
            },
        ),
        (
            Feature::Delegation,
            Action::RevokeToken {
                revoked: Revoked::principal(principal("agent-42")),
                until: Timestamp::MAX,
            },
        ),
        (
            Feature::Delegation,
            as_ancestor(&root, principal_target("agent-42")),
        ),
        (
            Feature::Delegation,
            as_ancestor(&root, AncestorTarget::Token(token("root"))),
        ),
        (Feature::Handover, Action::EnableHandover),
        (
            Feature::Handover,
            Action::ReleaseKeptReplicas {
                database: db("t0"),
                incarnation: Incarnation::LEGACY,
                epoch: fleetring_control::LeaseEpoch::new(1),
            },
        ),
    ]
}

#[test]
fn a_mixed_fleet_refuses_every_0_8_command_naming_the_0_7_member() {
    let mut fleet = Fleet::new(&v0_7_0());
    // The 0.8 members report every feature, which enables nothing.
    fleet.heartbeat("n0", v0_8_0());
    fleet.heartbeat("n1", v0_8_0());
    for (feature, action) in commands_that_need_0_8() {
        let unsupported = Rejection::FeatureUnsupported {
            feature,
            id: id("n2"),
            release: Release::new(0, 7, 0),
        };
        assert_eq!(fleet.refused(action.clone()), unsupported, "{action:?}");
    }
    // A membership change moves replica sets at once and keeps none, as on 0.7.
    let before: Vec<_> = LEASED.iter().map(|name| fleet.replica_set(name)).collect();
    let n3 = join("n3", v0_8_0());
    assert!(!fleet.state.handover_due(&n3));
    fleet.run(n3).expect("join");
    let moved = LEASED
        .iter()
        .zip(&before)
        .filter(|(name, before)| fleet.replica_set(name) != **before)
        .count();
    assert!(moved > 0, "n3's join moved no leased database");
    for name in LEASED {
        let database = fleet.state.database(&db(name)).expect("exists");
        assert_eq!(database.kept_replicas(), None, "{name}");
        assert_eq!(database.incarnation(), Incarnation::LEGACY, "{name}");
    }
    // What 0.7 revokes still works, as 0.7 writes it.
    fleet
        .run(Action::RevokeToken {
            revoked: Revoked::Token(token("t1")),
            until: at(60_000),
        })
        .expect("a token revocation");
    assert!(fleet.enabled().is_empty(), "{:?}", fleet.enabled());
    fleet.assert_0_7_reads_it();
}

#[test]
fn once_every_member_runs_0_8_each_feature_enables_on_its_first_command() {
    let mut fleet = Fleet::new(&v0_7_0());
    // n2 restarts on 0.8 and rejoins; its heartbeats report the same.
    fleet.run(join("n2", v0_8_0())).expect("rejoin");
    fleet.heartbeat("n2", v0_8_0());
    assert!(fleet.state.members().all(|member| {
        Feature::ALL
            .into_iter()
            .all(|f| member.running().supports(f))
    }));
    // Commands that need no feature enable none: a fleet can still roll back.
    fleet.run(Action::Tick).expect("tick");
    fleet.run(create("plain", 3)).expect("create");
    fleet
        .run(Action::RevokeToken {
            revoked: Revoked::Token(token("t1")),
            until: at(60_000),
        })
        .expect("a token revocation");
    assert!(fleet.enabled().is_empty(), "{:?}", fleet.enabled());
    fleet.assert_0_7_reads_it();

    // The first fork enables `fork`, and only it.
    fleet.run(fork_request(1, "fork")).expect("the first fork");
    assert_eq!(fleet.enabled(), [Feature::Fork]);
    fleet.assert_0_7_refuses_it(Feature::Fork);
    // A database created from then on carries its incarnation; those before keep 0, seeded.
    fleet.run(create_at("fresh", 3, 0x99)).expect("create");
    assert_eq!(
        fleet.state.database(&db("fresh")).map(|d| d.incarnation()),
        Some(Incarnation::new(0x99))
    );
    for name in [SOURCE, "plain"].into_iter().chain(LEASED) {
        let database = fleet.state.database(&db(name)).expect("exists");
        assert_eq!(database.incarnation(), Incarnation::LEGACY, "{name}");
        assert!(database.is_seeded(), "{name}");
    }

    // The first membership change that keeps a replica set enables `handover`.
    let n3 = join("n3", v0_8_0());
    assert!(fleet.state.handover_due(&n3), "n3's join keeps no set");
    fleet.change(n3).expect("join");
    assert_eq!(fleet.enabled(), [Feature::Fork, Feature::Handover]);
    let enabling = &fleet.log[fleet.log.len() - 2];
    assert!(v0_7::decodes_command(enabling).is_err(), "{enabling:?}");
    assert!(LEASED.iter().any(|name| {
        fleet
            .state
            .database(&db(name))
            .is_some_and(|d| d.kept_replicas().is_some())
    }));

    // The first delegation enables `delegation`.
    assert!(matches!(
        fleet.run(delegation()),
        Ok(Applied::DelegationRecorded { .. })
    ));
    assert_eq!(
        fleet.enabled(),
        [Feature::Fork, Feature::Delegation, Feature::Handover]
    );
    fleet.assert_0_7_refuses_it(Feature::Delegation);

    // A 0.7 node can no longer join.
    assert!(matches!(
        fleet.run(join("old", v0_7_0())),
        Err(Rejection::MissingFeature { .. })
    ));
}

/// Each command that needs a 0.8 feature, run first on a fleet that runs 0.8 throughout, enables
/// its own feature and no other, and ends rollback to 0.7 (ADR-0038's "a log with an enabled
/// feature can't load without it", one case per feature and per first command).
#[test]
fn every_first_command_enables_its_own_feature_alone_and_ends_rollback() {
    for (feature, action) in commands_that_need_0_8() {
        let mut fleet = Fleet::new(&v0_8_0());
        fleet.assert_0_7_reads_it();
        let outcome = match &action {
            // A release needs a kept set, which only the enabling command makes possible, and
            // `apply` takes a principal revocation only as its own action: each is screened by
            // the gate in a mixed fleet, but can't be a fleet's first command.
            Action::ReleaseKeptReplicas { .. }
            | Action::RevokeToken {
                revoked: Revoked::Principal { .. },
                ..
            } => continue,
            Action::EnableHandover => fleet.change(join("n3", v0_8_0())),
            _ => fleet.run(action.clone()),
        };
        assert!(outcome.is_ok(), "{action:?}: {outcome:?}");
        assert_eq!(fleet.enabled(), [feature], "{action:?}");
        let error = v0_7::decodes_state(&fleet.state).expect_err("0.7 refuses the snapshot");
        assert!(error.to_string().contains(feature.as_str()), "{error}");
        let enabling = fleet
            .log
            .iter()
            .find(|command| !command.action.features().is_empty())
            .expect("the enabling command");
        assert!(v0_7::decodes_command(enabling).is_err(), "{enabling:?}");
    }
}

/// A snapshot 0.7.0 wrote, its members upgraded one by one to 0.8: nothing is enabled, and it
/// still decodes on 0.7, until the first fork.
#[test]
fn a_0_7_snapshot_upgrades_in_place_and_enables_only_on_first_use() {
    let snapshot = include_str!("fixtures/pre_0_8_state.json");
    let mut fleet = Fleet {
        state: serde_json::from_str(snapshot).expect("the snapshot loads"),
        log: Vec::new(),
        clock: 0,
    };
    fleet.clock = fleet.state.now().as_millis();
    let members: Vec<String> = fleet
        .state
        .members()
        .map(|member| member.id().as_str().to_owned())
        .collect();
    for node in &members {
        fleet.heartbeat(node, v0_8_0());
        assert!(fleet.enabled().is_empty(), "{:?}", fleet.enabled());
        fleet.assert_0_7_reads_it();
    }
    for database in fleet.state.databases() {
        assert_eq!(database.incarnation(), Incarnation::LEGACY);
        assert!(database.is_seeded() && database.is_ready());
    }
    fleet
        .run(fork_of(1, "d0-try", "d0", 3))
        .expect("the first fork");
    assert!(fleet.state.is_enabled(Feature::Fork));
    fleet.assert_0_7_refuses_it(Feature::Fork);
}
