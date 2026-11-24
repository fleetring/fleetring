//! Tombstones in control state (ADR-0050): created by a drop or an expiry, forgotten by a stamped
//! `Tick`, gated as a fleet feature, and covered by replay.

use fleetring_control::{
    Action, Command, Feature, Incarnation, LeaseEpoch, MAX_TOMBSTONES_PER_NAME, Member,
    MetadataSpec, Millis, Outcome, Rejection, Release, Software, State, TombstoneCause, apply,
};

use crate::common::{at, config, create, create_always, current_software, db, id};
use crate::replay::{
    check_replay_from_scratch, check_replay_from_snapshots, check_replay_through_serialization,
};

const WINDOW: Millis = Millis::new(1_000);

/// A control log being written: every command applied and kept with its outcome.
struct Log {
    state: State,
    commands: Vec<Command>,
    outcomes: Vec<Outcome>,
}

impl Log {
    fn new() -> Self {
        Self {
            state: State::new(config()),
            commands: Vec::new(),
            outcomes: Vec::new(),
        }
    }

    /// A log whose one member supports every feature, with tombstones and incarnations on.
    fn enabled() -> Self {
        let mut log = Self::new();
        let member = Member::new(id("a")).with_software(current_software());
        log.run(0, Action::Join { member }).expect("joins");
        log.run(0, Action::EnableIncarnations).expect("enables");
        log.run(0, Action::SetTombstoneWindow { window: WINDOW })
            .expect("enables");
        log
    }

    fn run(&mut self, millis: u64, action: Action) -> Outcome {
        let command = Command::new(at(millis), action);
        let outcome = apply(&mut self.state, command.clone());
        self.commands.push(command);
        self.outcomes.push(outcome.clone());
        outcome
    }

    fn lease(&mut self, millis: u64, name: &str) {
        self.run(
            millis,
            Action::AcquireLease {
                database: db(name),
                node: id("a"),
            },
        )
        .expect("leased");
    }

    fn drop_database(&mut self, millis: u64, name: &str) {
        self.run(millis, Action::DropDatabase { name: db(name) })
            .expect("dropped");
    }
}

/// A log that makes and forgets tombstones, with an expiry, and a name created again,
/// replays to the identical state from scratch, from snapshots, and through its
/// serialized form.
#[test]
fn tombstones_replay_through_creation_and_purging() {
    let mut log = Log::enabled();
    log.run(10, create_always("scratch", 1, 7))
        .expect("created");
    log.lease(10, "scratch");
    log.drop_database(20, "scratch");
    let kept = log.state.tombstones().of(&db("scratch"));
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].incarnation(), Incarnation::new(7));
    assert_eq!(kept[0].epoch(), LeaseEpoch::new(1));
    assert_eq!(kept[0].cause(), TombstoneCause::Dropped);
    assert_eq!(kept[0].dropped_at(), at(20));
    assert_eq!(kept[0].kept_until(), at(1_020));

    // The name again, and an expiry of it, while the first tombstone is kept.
    let mut expiring = create_always("scratch", 1, 8);
    if let Action::CreateDatabase { metadata, .. } = &mut expiring {
        *metadata = Some(MetadataSpec {
            expires_at: Some(at(100)),
            ..MetadataSpec::default()
        });
    }
    log.run(30, expiring).expect("created again");
    log.lease(30, "scratch");
    log.run(
        100,
        Action::ExpireDatabase {
            name: db("scratch"),
        },
    )
    .expect("expired");
    let kept = log.state.tombstones().of(&db("scratch"));
    assert_eq!(kept.len(), 2);
    assert_eq!(kept[1].cause(), TombstoneCause::Expired);

    // A later window applies to later drops, and leaves the ones already kept alone.
    log.run(
        200,
        Action::SetTombstoneWindow {
            window: Millis::new(5_000),
        },
    )
    .expect("set");
    log.run(200, create_always("other", 1, 9)).expect("created");
    log.drop_database(300, "other");
    assert_eq!(
        log.state.tombstones().of(&db("other"))[0].kept_until(),
        at(5_300)
    );

    // Each is forgotten at its own time, by a stamped command.
    log.run(1_019, Action::Tick).expect("ticks");
    assert_eq!(log.state.tombstones().len(), 3);
    assert!(log.state.tombstones().purge_due(at(1_020)));
    log.run(1_020, Action::Tick).expect("ticks");
    assert_eq!(log.state.tombstones().of(&db("scratch")).len(), 1);
    log.run(1_100, Action::Tick).expect("ticks");
    assert!(log.state.tombstones().of(&db("scratch")).is_empty());
    assert_eq!(log.state.tombstones().len(), 1);
    log.run(5_300, Action::Tick).expect("ticks");
    assert!(log.state.tombstones().is_empty());

    check_replay_from_scratch(&log.commands, &log.outcomes, &log.state);
    check_replay_from_snapshots(&log.commands, &log.outcomes, &log.state);
    check_replay_through_serialization(&log.commands, &log.outcomes, &log.state);
}

/// A database created again under the name never takes an incarnation a tombstone holds, so an
/// old tombstone can't name it, at any epoch.
#[test]
fn a_tombstone_never_names_the_database_created_again() {
    let mut log = Log::enabled();
    log.run(10, create_always("reused", 1, 7)).expect("created");
    log.lease(10, "reused");
    log.drop_database(20, "reused");
    assert!(matches!(
        log.run(30, create_always("reused", 1, 7)),
        Err(Rejection::IncarnationUnavailable { .. })
    ));
    assert!(matches!(
        log.run(30, create_always("elsewhere", 1, 7)),
        Err(Rejection::IncarnationUnavailable { .. })
    ));
    log.run(30, create_always("reused", 1, 8)).expect("created");
    log.lease(30, "reused");

    let live = log.state.database(&db("reused")).expect("the new database");
    assert_eq!(
        live.epoch(),
        LeaseEpoch::new(1),
        "the tombstone's epoch too"
    );
    let tombstone = log.state.tombstones().of(&db("reused"))[0];
    assert!(!tombstone.names_copy(Some(live.incarnation()), live.epoch()));
    assert!(tombstone.names_copy(Some(Incarnation::new(7)), LeaseEpoch::new(1)));
    assert!(
        tombstone.names_copy(Some(Incarnation::new(7)), LeaseEpoch::ZERO),
        "a copy that missed later epochs is the dropped database's"
    );
    assert!(
        !tombstone.names_copy(Some(Incarnation::new(7)), LeaseEpoch::new(2)),
        "a copy past the drop's epoch isn't named"
    );
    assert!(
        !tombstone.names_copy(None, LeaseEpoch::new(1)),
        "a copy that records no incarnation isn't named"
    );
}

/// A database of the legacy incarnation leaves no tombstone, since that incarnation can't tell
/// the databases of a name apart.
#[test]
fn the_legacy_incarnation_leaves_no_tombstone() {
    let mut log = Log::new();
    let member = Member::new(id("a")).with_software(current_software());
    log.run(0, Action::Join { member }).expect("joins");
    log.run(5, create("old", 1)).expect("created");
    log.run(5, Action::SetTombstoneWindow { window: WINDOW })
        .expect("enables");
    assert!(
        log.state
            .database(&db("old"))
            .expect("exists")
            .incarnation()
            .is_legacy()
    );
    log.drop_database(10, "old");
    assert!(log.state.tombstones().is_empty());
}

/// In a fleet with a member that lacks the feature, the window can't be set, a drop records
/// nothing, and the writer isn't asked to enable it: the copies stay, as orphans.
#[test]
fn a_mixed_fleet_records_no_tombstone() {
    let mut log = Log::new();
    let old = Feature::ALL
        .into_iter()
        .filter(|feature| *feature != Feature::Tombstones)
        .fold(
            Software::new(Release::new(0, 8, 1), 1),
            Software::with_feature,
        );
    let member = Member::new(id("a")).with_software(old);
    log.run(0, Action::Join { member }).expect("joins");
    log.run(0, Action::EnableIncarnations).expect("enables");
    log.run(5, create_always("mixed", 1, 7)).expect("created");
    let dropping = Action::DropDatabase { name: db("mixed") };
    assert!(!log.state.tombstones_due(&dropping, WINDOW));
    assert!(matches!(
        log.run(5, Action::SetTombstoneWindow { window: WINDOW }),
        Err(Rejection::FeatureUnsupported {
            feature: Feature::Tombstones,
            ..
        })
    ));
    log.drop_database(10, "mixed");
    assert!(log.state.tombstones().is_empty());
    assert!(!log.state.is_enabled(Feature::Tombstones));
}

/// The writer logs the window before a drop only while the state's differs from its own.
#[test]
fn the_window_is_logged_only_when_it_changes() {
    let log = Log::enabled();
    let dropping = Action::DropDatabase { name: db("any") };
    assert!(!log.state.tombstones_due(&dropping, WINDOW));
    assert!(log.state.tombstones_due(&dropping, Millis::new(2_000)));
    assert!(!log.state.tombstones_due(&Action::Tick, Millis::new(2_000)));
}

/// A state from before the feature serializes with no trace of it.
#[test]
fn a_state_without_tombstones_serializes_as_before() {
    let state = State::new(config());
    let text = serde_json::to_string(&state).expect("serializes");
    assert!(!text.contains("tombstones"), "{text}");
    let decoded: State = serde_json::from_str(&text).expect("decodes");
    assert_eq!(decoded, state);
}

/// One name keeps at most its newest tombstones, however often it is dropped.
#[test]
fn a_name_dropped_often_keeps_a_bounded_number_of_tombstones() {
    let mut log = Log::enabled();
    let rounds = u64::try_from(MAX_TOMBSTONES_PER_NAME).expect("small") + 4;
    for round in 1..=rounds {
        log.run(round, create_always("churn", 1, round))
            .expect("created");
        log.drop_database(round, "churn");
    }
    let kept = log.state.tombstones().of(&db("churn"));
    assert_eq!(kept.len(), MAX_TOMBSTONES_PER_NAME);
    assert_eq!(
        kept[0].incarnation(),
        Incarnation::new(5),
        "the oldest went"
    );
}
