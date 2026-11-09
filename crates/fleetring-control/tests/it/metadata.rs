//! Database metadata and expiry (ADR-0041): what creations record, how updates patch it, the
//! feature gate, and expiry judged at the command's time alone.

use std::collections::BTreeSet;

use fleetring_control::{
    Action, Applied, DatabaseName, DescriptionChange, ExpiryChange, Feature, LabelKey, Labels,
    LeaseEpoch, Member, Metadata, MetadataChange, MetadataSpec, PrincipalName, Rejection, Release,
    Software, State, Timestamp, parse_label,
};

use crate::common::{at, config, create, current_software, db, heartbeat, id, member, run};

/// A creation of `tenant:acme` and the state after it, written by release 0.5.0, before
/// metadata existed: one member reporting no software, the database created without durability
/// settings, and its first lease. A creation and a record without metadata must still be written
/// byte for byte like this (ADR-0041, point 1).
const CREATION_0_5_0: &str = r#"{"at":1000,"action":{"create_database":{"name":"tenant:acme","replication_factor":1,"engine":"sqlite","durability":null}}}"#;
const STATE_0_5_0: &str = r#"{"config":{"lease_duration":1000,"failover_margin":200,"heartbeat_timeout":3000},"now":1000,"membership_epoch":1,"members":{"n0":{"member":{"id":"n0","zone":"z0","host":null,"weight":1},"last_heartbeat":1000,"reports":{}}},"databases":{"tenant:acme":{"name":"tenant:acme","replication_factor":1,"engine":"sqlite","acks":"quorum","min_replicas":1,"pending_durability":null,"epoch":1,"lease":{"holder":"n0","epoch":1,"expires_at":2000},"promotion":null}}}"#;

/// A fleet of `members`, each on this release line with every feature.
fn fleet(members: &[&str]) -> State {
    let mut state = State::new(config());
    for node in members {
        let member = Member::new(id(node)).with_software(current_software());
        run(&mut state, 0, Action::Join { member }).expect("join");
    }
    state
}

fn principal(name: &str) -> PrincipalName {
    name.parse().expect("a valid principal name")
}

fn key(text: &str) -> LabelKey {
    text.parse().expect("a valid label key")
}

fn labels(pairs: &[&str]) -> Labels {
    Labels::try_from_pairs(
        pairs
            .iter()
            .map(|pair| parse_label(pair).expect("a valid label")),
    )
    .expect("few labels")
}

fn create_with_metadata(name: &str, spec: MetadataSpec) -> Action {
    let Action::CreateDatabase {
        name,
        replication_factor,
        engine,
        durability,
        ..
    } = create(name, 1)
    else {
        unreachable!("create makes a creation")
    };
    Action::CreateDatabase {
        name,
        replication_factor,
        engine,
        durability,
        metadata: Some(spec),
        incarnation: fleetring_control::Incarnation::LEGACY,
    }
}

fn expiring_at(millis: u64) -> MetadataSpec {
    MetadataSpec {
        expires_at: Some(at(millis)),
        ..MetadataSpec::default()
    }
}

fn update(name: &str, change: MetadataChange) -> Action {
    Action::UpdateDatabase {
        name: db(name),
        change,
        by: None,
        if_version: None,
    }
}

fn set_labels(pairs: &[&str]) -> MetadataChange {
    MetadataChange {
        set_labels: labels(pairs),
        ..MetadataChange::default()
    }
}

fn set_expiry(millis: u64) -> MetadataChange {
    MetadataChange {
        expiry: ExpiryChange::Set(at(millis)),
        ..MetadataChange::default()
    }
}

fn metadata<'s>(state: &'s State, name: &str) -> &'s Metadata {
    state
        .database(&db(name))
        .and_then(|database| database.metadata())
        .expect("the database has metadata")
}

fn label_of(state: &State, name: &str, label: &str) -> Option<String> {
    metadata(state, name)
        .labels()
        .get(&key(label))
        .map(|value| value.as_str().to_owned())
}

#[test]
fn a_creation_with_metadata_records_it_with_the_creator_and_time() {
    let mut state = fleet(&["a"]);
    let spec = MetadataSpec {
        description: Some(
            "Memory for agent 42.\nTables: facts."
                .parse()
                .expect("valid"),
        ),
        labels: labels(&["purpose=memory", "agent=42"]),
        expires_at: Some(at(90_000)),
        created_by: Some(principal("agent-42")),
    };
    let created = run(
        &mut state,
        5_000,
        create_with_metadata("agent-42-memory", spec),
    );
    assert_eq!(
        created,
        Ok(Applied::DatabaseCreated {
            database: db("agent-42-memory")
        })
    );
    let recorded = metadata(&state, "agent-42-memory");
    assert_eq!(
        recorded.description().map(|d| d.as_str()),
        Some("Memory for agent 42.\nTables: facts.")
    );
    assert_eq!(recorded.description_by(), Some(&principal("agent-42")));
    assert_eq!(recorded.labels(), &labels(&["agent=42", "purpose=memory"]));
    assert_eq!(recorded.created_by(), Some(&principal("agent-42")));
    assert_eq!(recorded.created_at(), Some(at(5_000)));
    assert_eq!(recorded.expires_at(), Some(at(90_000)));
    assert_eq!(recorded.version(), 1);

    // A creation that records only its creator has no description, so no author either.
    let creator_only = MetadataSpec {
        created_by: Some(principal("agent-42")),
        ..MetadataSpec::default()
    };
    run(
        &mut state,
        6_000,
        create_with_metadata("plain", creator_only),
    )
    .expect("create");
    let recorded = metadata(&state, "plain");
    assert_eq!(recorded.created_by(), Some(&principal("agent-42")));
    assert_eq!(recorded.description_by(), None);
    assert_eq!(recorded.created_at(), Some(at(6_000)));
}

#[test]
fn a_creation_without_metadata_serializes_as_before() {
    let mut state = State::new(config());
    run(&mut state, 1_000, Action::Join { member: member(0) }).expect("join");
    let creation = fleetring_control::Command::new(at(1_000), create("tenant:acme", 1));
    assert_eq!(
        serde_json::to_string(&creation).expect("serializes"),
        CREATION_0_5_0
    );
    let decoded: fleetring_control::Command =
        serde_json::from_str(CREATION_0_5_0).expect("a 0.5.0 creation decodes");
    assert_eq!(decoded, creation);

    fleetring_control::apply(&mut state, creation).expect("create");
    let acquire = Action::AcquireLease {
        database: db("tenant:acme"),
        node: id("n0"),
    };
    run(&mut state, 1_000, acquire).expect("lease");
    assert_eq!(
        serde_json::to_string(&state).expect("serializes"),
        STATE_0_5_0
    );
    let restored: State = serde_json::from_str(STATE_0_5_0).expect("a 0.5.0 state decodes");
    assert_eq!(restored, state);
    assert!(
        restored
            .database(&db("tenant:acme"))
            .is_some_and(|database| database.metadata().is_none())
    );
}

#[test]
fn a_mixed_fleet_refuses_metadata_and_changes_nothing() {
    let mut state = fleet(&["a"]);
    // The previous line, which knows no metadata.
    let old = Member::new(id("b"))
        .with_software(Software::new(Release::new(0, 5, 0), 1).with_feature(Feature::Undrain));
    run(&mut state, 0, Action::Join { member: old }).expect("join");
    run(&mut state, 0, create("plain", 1)).expect("a plain creation needs no feature");
    let before = state.clone();

    let refusals = [
        create_with_metadata(
            "described",
            MetadataSpec {
                labels: labels(&["purpose=task"]),
                ..MetadataSpec::default()
            },
        ),
        create_with_metadata("expiring", expiring_at(90_000)),
        update("plain", set_labels(&["purpose=task"])),
        update("plain", set_expiry(90_000)),
        Action::ExpireDatabase { name: db("plain") },
    ];
    for action in refusals {
        let refused = Rejection::FeatureUnsupported {
            feature: Feature::Metadata,
            id: id("b"),
            release: Release::new(0, 5, 0),
        };
        // The writer screens before it logs, with the same answer `apply` gives.
        assert_eq!(state.check_features(&action), Err(refused.clone()));
        assert_eq!(state.check_admission(&action), Err(refused.clone()));
        assert_eq!(run(&mut state, 10, action), Err(refused));
        assert_eq!(state, before);
    }
    assert!(!state.is_enabled(Feature::Metadata));
}

/// A fleet whose members report `metadata` but not `expiry`, as a release built before the
/// control node's expiry sweep did (ADR-0041, point 3), takes metadata and refuses any expiry,
/// enabling only what it accepted.
#[test]
fn a_fleet_without_expiry_takes_metadata_and_refuses_an_expiry() {
    let without_expiry = Software::new(Release::new(0, 6, 0), 1)
        .with_feature(Feature::Undrain)
        .with_feature(Feature::Metadata);
    let mut state = State::new(config());
    for node in ["a", "b"] {
        let member = Member::new(id(node)).with_software(without_expiry.clone());
        run(&mut state, 0, Action::Join { member }).expect("join");
    }
    let before = state.clone();
    let expiring = create_with_metadata("scratch", expiring_at(90_000));
    let refused = Rejection::FeatureUnsupported {
        feature: Feature::Expiry,
        id: id("a"),
        release: Release::new(0, 6, 0),
    };
    assert_eq!(state.check_admission(&expiring), Err(refused.clone()));
    assert_eq!(run(&mut state, 1, expiring), Err(refused));
    assert_eq!(state, before);

    let labelled = MetadataSpec {
        labels: labels(&["purpose=task"]),
        ..MetadataSpec::default()
    };
    run(&mut state, 1, create_with_metadata("scratch", labelled)).expect("create");
    assert!(state.is_enabled(Feature::Metadata));
    assert!(!state.is_enabled(Feature::Expiry));
    for refused in [
        update("scratch", set_expiry(90_000)),
        Action::ExpireDatabase {
            name: db("scratch"),
        },
    ] {
        assert!(matches!(
            run(&mut state, 2, refused),
            Err(Rejection::FeatureUnsupported {
                feature: Feature::Expiry,
                ..
            })
        ));
    }
    run(&mut state, 2, update("scratch", set_labels(&["agent=7"]))).expect("update");
}

#[test]
fn the_first_creation_with_metadata_enables_the_feature() {
    let mut state = fleet(&["a", "b"]);
    run(&mut state, 0, create("plain", 1)).expect("create");
    assert_eq!(
        state.features().count(),
        0,
        "a plain creation enables nothing"
    );

    let labelled = MetadataSpec {
        labels: labels(&["purpose=memory"]),
        ..MetadataSpec::default()
    };
    run(&mut state, 1, create_with_metadata("memory", labelled)).expect("create");
    assert_eq!(state.features().collect::<Vec<_>>(), [Feature::Metadata]);

    // A rejected creation with an expiry enables nothing more.
    let passed = create_with_metadata("late", expiring_at(1));
    assert!(matches!(
        run(&mut state, 2, passed),
        Err(Rejection::ExpiryPassed { .. })
    ));
    assert!(!state.is_enabled(Feature::Expiry));

    run(
        &mut state,
        3,
        create_with_metadata("scratch", expiring_at(90_000)),
    )
    .expect("create");
    assert_eq!(
        state.features().collect::<Vec<_>>(),
        [Feature::Metadata, Feature::Expiry]
    );

    // From then on a member that lacks either feature can't join.
    let lacking = Member::new(id("c"))
        .with_software(Software::new(Release::new(0, 6, 0), 1).with_feature(Feature::Metadata));
    assert!(matches!(
        run(&mut state, 4, Action::Join { member: lacking }),
        Err(Rejection::MissingFeature {
            feature: Feature::Expiry,
            ..
        })
    ));
}

#[test]
fn updates_patch_labels_in_log_order() {
    let mut state = fleet(&["a"]);
    run(&mut state, 0, create("shared", 1)).expect("create");
    run(&mut state, 1, update("shared", set_labels(&["agent=7"]))).expect("first agent");
    run(
        &mut state,
        1,
        update("shared", set_labels(&["purpose=task"])),
    )
    .expect("second agent");
    assert_eq!(label_of(&state, "shared", "agent").as_deref(), Some("7"));
    assert_eq!(
        label_of(&state, "shared", "purpose").as_deref(),
        Some("task")
    );

    run(
        &mut state,
        2,
        update("shared", set_labels(&["purpose=memory"])),
    )
    .expect("later");
    run(
        &mut state,
        2,
        update("shared", set_labels(&["purpose=cache"])),
    )
    .expect("latest");
    assert_eq!(
        label_of(&state, "shared", "purpose").as_deref(),
        Some("cache")
    );
    assert_eq!(metadata(&state, "shared").version(), 4);
}

#[test]
fn removals_apply_before_sets() {
    let mut state = fleet(&["a"]);
    run(&mut state, 0, create("db", 1)).expect("create");
    run(
        &mut state,
        1,
        update("db", set_labels(&["purpose=task", "agent=7"])),
    )
    .expect("set");
    let change = MetadataChange {
        set_labels: labels(&["purpose=memory"]),
        remove_labels: BTreeSet::from([key("purpose"), key("agent"), key("absent")]),
        ..MetadataChange::default()
    };
    run(&mut state, 2, update("db", change)).expect("patch");
    assert_eq!(
        metadata(&state, "db").labels(),
        &labels(&["purpose=memory"])
    );
}

#[test]
fn if_version_makes_an_update_conditional() {
    let mut state = fleet(&["a"]);
    run(&mut state, 0, create("db", 1)).expect("create");
    let conditional = |version: u64| Action::UpdateDatabase {
        name: db("db"),
        change: set_labels(&["purpose=task"]),
        by: None,
        if_version: Some(version),
    };

    // A database without metadata is at version 0.
    let before = state.clone();
    assert_eq!(
        run(&mut state, 1, conditional(1)),
        Err(Rejection::MetadataConflict {
            database: db("db"),
            expected: 1,
            current: 0,
        })
    );
    assert_eq!(state, before);
    assert_eq!(
        run(&mut state, 1, conditional(0)),
        Ok(Applied::DatabaseUpdated {
            database: db("db"),
            version: 1,
        })
    );
    let before = state.clone();
    assert!(matches!(
        run(&mut state, 2, conditional(0)),
        Err(Rejection::MetadataConflict {
            expected: 0,
            current: 1,
            ..
        })
    ));
    assert_eq!(state, before);
    run(&mut state, 2, conditional(1)).expect("at version 1");
    assert_eq!(metadata(&state, "db").version(), 2);
}

#[test]
fn an_update_past_32_labels_is_refused() {
    let mut state = fleet(&["a"]);
    run(&mut state, 0, create("db", 1)).expect("create");
    let thirty_two: Vec<String> = (0..32).map(|i| format!("k{i}=v")).collect();
    let pairs: Vec<&str> = thirty_two.iter().map(String::as_str).collect();
    run(&mut state, 1, update("db", set_labels(&pairs))).expect("32 labels");
    let before = state.clone();
    assert_eq!(
        run(&mut state, 2, update("db", set_labels(&["one-more=v"]))),
        Err(Rejection::TooManyLabels {
            database: db("db"),
            count: 33,
            max: 32,
        })
    );
    assert_eq!(state, before);

    // Replacing a value, or removing one while adding another, stays within the limit.
    run(&mut state, 3, update("db", set_labels(&["k0=w"]))).expect("replace");
    let swap = MetadataChange {
        set_labels: labels(&["one-more=v"]),
        remove_labels: BTreeSet::from([key("k1")]),
        ..MetadataChange::default()
    };
    run(&mut state, 4, update("db", swap)).expect("swap");
    assert_eq!(metadata(&state, "db").labels().len(), 32);
}

#[test]
fn the_description_records_who_last_set_it() {
    let mut state = fleet(&["a"]);
    let spec = MetadataSpec {
        description: Some("First.".parse().expect("valid")),
        created_by: Some(principal("agent-1")),
        ..MetadataSpec::default()
    };
    run(&mut state, 0, create_with_metadata("db", spec)).expect("create");
    let set = |text: &str, by: Option<&str>| Action::UpdateDatabase {
        name: db("db"),
        change: MetadataChange {
            description: DescriptionChange::Set(text.parse().expect("valid")),
            ..MetadataChange::default()
        },
        by: by.map(principal),
        if_version: None,
    };
    run(&mut state, 1, set("Second.", Some("agent-2"))).expect("update");
    let recorded = metadata(&state, "db");
    assert_eq!(recorded.description().map(|d| d.as_str()), Some("Second."));
    assert_eq!(recorded.description_by(), Some(&principal("agent-2")));
    assert_eq!(recorded.created_by(), Some(&principal("agent-1")));

    // A label change leaves the author alone; an anonymous caller is no author.
    let labelled = Action::UpdateDatabase {
        name: db("db"),
        change: set_labels(&["purpose=memory"]),
        by: Some(principal("agent-3")),
        if_version: None,
    };
    run(&mut state, 2, labelled).expect("update");
    assert_eq!(
        metadata(&state, "db").description_by(),
        Some(&principal("agent-2"))
    );
    run(&mut state, 3, set("Third.", None)).expect("update");
    assert_eq!(metadata(&state, "db").description_by(), None);

    let clear = Action::UpdateDatabase {
        name: db("db"),
        change: MetadataChange {
            description: DescriptionChange::Clear,
            ..MetadataChange::default()
        },
        by: Some(principal("agent-4")),
        if_version: None,
    };
    run(&mut state, 4, clear).expect("clear");
    assert_eq!(metadata(&state, "db").description(), None);
    assert_eq!(metadata(&state, "db").description_by(), None);
}

#[test]
fn a_description_set_without_an_author_records_none() {
    let mut state = fleet(&["a"]);
    run(&mut state, 0, create("restored", 1)).expect("create");
    let restore = Action::UpdateDatabase {
        name: db("restored"),
        change: MetadataChange {
            description: DescriptionChange::SetWithoutAuthor(
                "From the archive.".parse().expect("valid"),
            ),
            ..MetadataChange::default()
        },
        by: Some(principal("ops")),
        if_version: None,
    };
    run(&mut state, 1, restore).expect("update");
    let recorded = metadata(&state, "restored");
    assert_eq!(
        recorded.description().map(|d| d.as_str()),
        Some("From the archive.")
    );
    assert_eq!(recorded.description_by(), None);
}

/// A database created before metadata existed gets it at its first update, with no creator and
/// no creation time.
#[test]
fn a_database_without_metadata_gets_it_at_its_first_update() {
    let mut state = fleet(&["a"]);
    run(&mut state, 0, create("old", 1)).expect("create");
    assert!(
        state
            .database(&db("old"))
            .is_some_and(|d| d.metadata().is_none())
    );
    run(
        &mut state,
        7,
        update("old", set_labels(&["purpose=memory"])),
    )
    .expect("update");
    let recorded = metadata(&state, "old");
    assert_eq!(recorded.created_by(), None);
    assert_eq!(recorded.created_at(), None);
    assert_eq!(recorded.version(), 1);

    assert_eq!(
        run(&mut state, 8, update("missing", set_labels(&["a=b"]))),
        Err(Rejection::UnknownDatabase {
            database: db("missing")
        })
    );
}

#[test]
fn an_expiry_at_or_before_the_command_time_is_refused() {
    let mut state = fleet(&["a"]);
    run(&mut state, 10_000, Action::Tick).expect("tick");
    let before = state.clone();
    for expires_at in [9_999, 10_000] {
        assert_eq!(
            run(
                &mut state,
                10_000,
                create_with_metadata("db", expiring_at(expires_at))
            ),
            Err(Rejection::ExpiryPassed {
                database: db("db"),
                expires_at: at(expires_at),
            })
        );
        // A stamp that went backwards runs at the state's time, so it can't sneak one in.
        assert!(matches!(
            run(
                &mut state,
                5_000,
                create_with_metadata("db", expiring_at(expires_at))
            ),
            Err(Rejection::ExpiryPassed { .. })
        ));
        assert_eq!(state, before);
    }
    run(
        &mut state,
        10_000,
        create_with_metadata("db", expiring_at(10_001)),
    )
    .expect("create");

    let before = state.clone();
    assert_eq!(
        run(&mut state, 20_000, update("db", set_expiry(20_000))),
        Err(Rejection::ExpiryPassed {
            database: db("db"),
            expires_at: at(20_000),
        })
    );
    assert_eq!(state, before);
}

#[test]
fn expire_database_drops_only_once_the_expiry_has_passed() {
    let mut state = fleet(&["a"]);
    let t = 60_000;
    run(
        &mut state,
        0,
        create_with_metadata("scratch", expiring_at(t)),
    )
    .expect("create");
    run(
        &mut state,
        0,
        Action::AcquireLease {
            database: db("scratch"),
            node: id("a"),
        },
    )
    .expect("lease");
    run(&mut state, t - 1, heartbeat("a", &[("scratch", &[1])])).expect("heartbeat");
    assert!(state.report(&id("a"), &db("scratch")).is_some());

    let before = state.clone();
    let expire = || Action::ExpireDatabase {
        name: db("scratch"),
    };
    assert_eq!(
        run(&mut state, t - 1, expire()),
        Err(Rejection::NotExpired {
            database: db("scratch"),
            expires_at: Some(at(t)),
        })
    );
    assert_eq!(state, before);

    assert_eq!(
        run(&mut state, t, expire()),
        Ok(Applied::DatabaseExpired {
            database: db("scratch"),
            epoch: LeaseEpoch::new(1),
            expires_at: at(t),
        })
    );
    assert!(state.database(&db("scratch")).is_none());
    assert!(state.report(&id("a"), &db("scratch")).is_none());
    assert_eq!(
        run(
            &mut state,
            t + 1,
            Action::RenewLease {
                database: db("scratch"),
                node: id("a"),
                epoch: LeaseEpoch::new(1),
            }
        ),
        Err(Rejection::UnknownDatabase {
            database: db("scratch")
        })
    );
    assert_eq!(
        run(&mut state, t + 1, expire()),
        Err(Rejection::UnknownDatabase {
            database: db("scratch")
        })
    );

    // A database without an expiry never expires.
    run(&mut state, t, create("kept", 1)).expect("create");
    assert_eq!(
        run(
            &mut state,
            u64::MAX,
            Action::ExpireDatabase { name: db("kept") }
        ),
        Err(Rejection::NotExpired {
            database: db("kept"),
            expires_at: None,
        })
    );
}

/// The sweep reads the expired databases at one moment and submits later; an update in between
/// that extends or clears the expiry wins.
#[test]
fn an_extended_expiry_wins_over_a_stale_sweep() {
    let mut state = fleet(&["a"]);
    run(
        &mut state,
        0,
        create_with_metadata("extended", expiring_at(1_000)),
    )
    .expect("create");
    run(
        &mut state,
        0,
        create_with_metadata("cleared", expiring_at(1_000)),
    )
    .expect("create");
    let due = state.expired_databases(at(1_000));
    assert_eq!(due, [db("cleared"), db("extended")]);

    run(&mut state, 900, update("extended", set_expiry(5_000))).expect("extend");
    let never = MetadataChange {
        expiry: ExpiryChange::Clear,
        ..MetadataChange::default()
    };
    run(&mut state, 900, update("cleared", never)).expect("clear");
    for name in due {
        let expires_at = metadata(&state, name.as_str()).expires_at();
        assert_eq!(
            run(
                &mut state,
                1_000,
                Action::ExpireDatabase { name: name.clone() }
            ),
            Err(Rejection::NotExpired {
                database: name,
                expires_at,
            })
        );
    }
    assert_eq!(state.databases().count(), 2);
    assert!(state.expired_databases(at(4_999)).is_empty());
    assert_eq!(state.expired_databases(at(5_000)), [db("extended")]);
}

#[test]
fn expired_databases_are_listed_by_expiry_then_name() {
    let mut state = fleet(&["a"]);
    for (name, expires_at) in [("c", 3_000), ("b", 2_000), ("a", 3_000), ("d", 9_000)] {
        run(
            &mut state,
            0,
            create_with_metadata(name, expiring_at(expires_at)),
        )
        .expect("create");
    }
    run(&mut state, 0, create("never", 1)).expect("create");
    assert!(state.expired_databases(at(1_999)).is_empty());
    assert_eq!(state.expired_databases(at(2_000)), [db("b")]);
    let names: Vec<DatabaseName> = state.expired_databases(at(8_999));
    assert_eq!(names, [db("b"), db("a"), db("c")]);
    assert_eq!(
        state.expired_databases(Timestamp::MAX),
        [db("b"), db("a"), db("c"), db("d")]
    );
}

#[test]
fn a_stamp_that_goes_backwards_never_expires_early() {
    let mut state = fleet(&["a"]);
    run(
        &mut state,
        0,
        create_with_metadata("scratch", expiring_at(10_000)),
    )
    .expect("create");
    run(&mut state, 9_000, Action::Tick).expect("tick");
    let expire = || Action::ExpireDatabase {
        name: db("scratch"),
    };

    // Before the expiry, no stamp can make it pass; the query agrees.
    for stamp in [0, 5_000, 9_999] {
        assert!(matches!(
            run(&mut state, stamp, expire()),
            Err(Rejection::NotExpired { .. })
        ));
    }
    assert!(state.expired_databases(at(5_000)).is_empty());
    assert_eq!(state.now(), at(9_000));

    // Once the state's time has passed it, a stamp that went backwards counts as that time.
    run(&mut state, 10_000, Action::Tick).expect("tick");
    assert_eq!(state.expired_databases(at(1_000)), [db("scratch")]);
    assert_eq!(
        run(&mut state, 1_000, expire()),
        Ok(Applied::DatabaseExpired {
            database: db("scratch"),
            epoch: LeaseEpoch::ZERO,
            expires_at: at(10_000),
        })
    );
    assert_eq!(state.now(), at(10_000));
}

/// With no members, no feature can be shown to be supported, so a command that needs one is
/// refused rather than enabling it: enabling `expiry`, which no release reports yet, would refuse
/// every later join, this build's included (ADR-0041, amendment 3).
#[test]
fn a_fleet_without_members_enables_no_feature() {
    let mut state = State::new(config());
    let before = state.clone();
    let expiring = create_with_metadata("scratch", expiring_at(90_000));
    let refused = Rejection::FeatureWithoutMembers {
        feature: Feature::Metadata,
    };
    assert_eq!(state.check_admission(&expiring), Err(refused.clone()));
    assert_eq!(run(&mut state, 1, expiring), Err(refused));
    assert_eq!(state, before);
    assert_eq!(state.features().count(), 0);

    // A plain creation needs no feature, and a member of this build joins afterwards.
    run(&mut state, 1, create("plain", 1)).expect("create");
    let member = Member::new(id("a")).with_software(current_software());
    run(&mut state, 2, Action::Join { member }).expect("join");
    run(&mut state, 3, update("plain", set_expiry(90_000))).expect("now it applies");
    assert!(state.is_enabled(Feature::Expiry));
}

/// The rule changes no replay of an older log: before it, the only gated action was `Undrain`,
/// which names a member, so in a fleet without members it was rejected anyway and changed
/// nothing.
#[test]
fn an_undrain_in_a_fleet_without_members_changed_nothing_before_the_rule_either() {
    let mut state = State::new(config());
    run(&mut state, 0, create("plain", 1)).expect("create");
    let before = state.clone();
    for stamp in [0, 5_000] {
        let refused = run(&mut state, stamp, Action::Undrain { id: id("a") });
        assert_eq!(
            refused,
            Err(Rejection::FeatureWithoutMembers {
                feature: Feature::Undrain
            })
        );
        assert_eq!(state, before);
    }

    // Past the gate, as before the rule, the transition itself rejects it: with `undrain`
    // enabled and every member gone, the gate passes and the member is unknown.
    let member = Member::new(id("a")).with_software(current_software());
    run(&mut state, 1, Action::Join { member }).expect("join");
    run(&mut state, 1, Action::Undrain { id: id("a") }).expect("undrain");
    run(&mut state, 1, Action::Leave { id: id("a") }).expect("leave");
    let before = state.clone();
    assert_eq!(
        run(&mut state, 2, Action::Undrain { id: id("a") }),
        Err(Rejection::UnknownMember { id: id("a") })
    );
    assert_eq!(state, before);
}
