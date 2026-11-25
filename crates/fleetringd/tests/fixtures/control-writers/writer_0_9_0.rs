//! Writes the format-1 control log fixture with 0.9.0's own code (FR-064, ADR-0040 point 8).
//!
//! Not compiled in this tree: cargo discovers only `tests/*.rs` and `tests/*/main.rs`.
//! `scripts/write-control-fixtures.sh` copies it into a worktree at `fleetringd-v0.9.0` as
//! `crates/fleetringd/tests/control_fixture_writer.rs` and runs it there, so it uses only API
//! present at that tag. It drives the single writer, so the enabling commands it injects
//! (`EnableIncarnations`, `EnableHandover`, `SetTombstoneWindow`) are in the log where 0.9.0
//! logs them.
//!
//! It writes `control.db` and `commands.json` (every logged command, in order) to
//! `$FIXTURE_OUT`.

use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor, Zone};
use fleetring_control::delegation::{CallerToken, CanonicalRequest, DelegationId, TokenClaims, TokenKind};
use fleetring_control::fork::{
    ChainHash, ForkIdentity, ForkIntent, ForkLimits, ForkState, OperationId, PlacementCaughtUp,
    ResetReport, StagedCopy,
};
use fleetring_control::scope::{Scope, Scopes};
use fleetring_control::{
    Acks, Action, Applied, ControlConfig, DatabaseName, Description, DescriptionChange,
    Durability, Engine, ExpiryChange, Feature, HostedReplica, Incarnation, IncarnationRule,
    LeaseEpoch, Member, MetadataChange, MetadataSpec, Millis, Position, PrincipalName, Release,
    ReplicaRole, ReplicasCaughtUp, Revoked, Software, State, Timestamp, UnknownReports,
};
use fleetringd::clock::SimClock;
use fleetringd::control::{CommandLog, ControlHandle, ControlNode, ControlOptions, StorageIo};

/// 2025-11-24T16:00:00Z, when the fixture's control node starts its clock.
const START: u64 = 1_764_000_000_000;

/// One hour in milliseconds.
const HOUR: u64 = 3_600_000;

/// The control node that stamps every command.
const RECORDER: &str = "c1";

fn id(name: &str) -> NodeId {
    name.parse().expect("a node id")
}

fn db(name: &str) -> DatabaseName {
    name.parse().expect("a database name")
}

fn zone(name: &str) -> Zone {
    name.parse().expect("a zone")
}

fn principal(name: &str) -> PrincipalName {
    name.parse().expect("a principal name")
}

fn factor(value: u8) -> ReplicationFactor {
    ReplicationFactor::new(value).expect("a nonzero replication factor")
}

fn scopes(texts: &[&str]) -> Scopes {
    texts
        .iter()
        .map(|text| text.parse::<Scope>().expect("a scope"))
        .collect::<Scopes>()
        .normalized()
}

/// 0.9.0 with every feature, as every member reports it.
fn software() -> Software {
    Feature::ALL
        .into_iter()
        .fold(Software::new(Release::new(0, 9, 0), 1), Software::with_feature)
}

const NOTES: &str = "tenant:acme/notes";
const BILLING: &str = "tenant:acme/billing";
const SCRATCH: &str = "tenant:acme/scratch";
const FORK: &str = "tenant:acme/notes-review";

/// The members, with their zones.
const MEMBERS: [(&str, &str); 5] = [("n1", "a"), ("n2", "b"), ("n3", "c"), ("n4", "a"), ("n5", "b")];

/// The control node, its clock, and what it has logged.
struct Writer {
    handle: ControlHandle,
}

impl Writer {
    fn state(&self) -> std::sync::Arc<State> {
        self.handle.state()
    }

    fn now(&self) -> Timestamp {
        self.state().now()
    }

    /// Submits `action` after `millis` of paused time, and requires the writer to accept it.
    async fn run(&self, millis: u64, action: Action) -> Applied {
        tokio::time::advance(Duration::from_millis(millis)).await;
        let committed = self
            .handle
            .submit(action.clone())
            .await
            .unwrap_or_else(|error| panic!("{action:?} wasn't logged: {error}"));
        committed
            .into_outcome()
            .unwrap_or_else(|rejection| panic!("{action:?} was rejected: {rejection:?}"))
    }

    /// Every member heartbeats, reporting what `replicas` says it hosts.
    async fn heartbeats(&self, replicas: impl Fn(&str) -> Vec<HostedReplica>) {
        let members: Vec<String> = self
            .state()
            .members()
            .map(|member| member.id().as_str().to_owned())
            .collect();
        for (n, node) in members.iter().enumerate() {
            let unknown_reports = if n % 2 == 0 {
                UnknownReports::Skip
            } else {
                UnknownReports::Reject
            };
            self.run(
                10,
                Action::Heartbeat {
                    id: id(node),
                    replicas: replicas(node),
                    unknown_reports,
                    software: Some(software()),
                    disk_free_bytes: Some(40 << 30),
                },
            )
            .await;
        }
    }

    /// Submits every action the fork driver proposes now, as the leader's timers would.
    async fn drive_forks(&self) -> Vec<Action> {
        let proposed = fleetring_control::fork::actions(&self.state(), self.now());
        for action in &proposed {
            self.run(0, action.clone()).await;
        }
        proposed
    }

    fn replica_set(&self, name: &str) -> Vec<NodeId> {
        self.state().replica_set(&db(name)).expect("a replica set")
    }

    fn epoch(&self, name: &str) -> LeaseEpoch {
        self.state().database(&db(name)).expect("the database").epoch()
    }

    fn incarnation(&self, name: &str) -> Incarnation {
        self.state()
            .database(&db(name))
            .expect("the database")
            .incarnation()
    }
}

/// The primary of `name` at `epoch` and `incarnation`, reporting placement at `membership` caught
/// up.
fn caught_up_primary(name: &str, incarnation: Incarnation, epoch: LeaseEpoch, membership: fleetring::Epoch) -> HostedReplica {
    HostedReplica::new(db(name), ReplicaRole::Primary, Position::new([9]))
        .with_log_epoch(epoch)
        .with_replicas_caught_up(ReplicasCaughtUp::new(incarnation, epoch, membership))
}

fn create(name: &str, replication_factor: u8, incarnation: u64) -> Action {
    Action::CreateDatabase {
        name: db(name),
        replication_factor: factor(replication_factor),
        engine: Engine::Sqlite,
        durability: None,
        metadata: None,
        incarnation: Incarnation::new(incarnation),
        incarnation_rule: IncarnationRule::Always,
    }
}

fn root_token() -> CallerToken {
    CallerToken {
        claims: TokenClaims {
            fleet: "acme".into(),
            principal: principal("agent-42"),
            id: "root-42".parse().expect("a token id"),
            issued_at: Some(Timestamp::from_millis(START - HOUR)),
            not_before: Timestamp::from_millis(START - HOUR),
            expires_at: Timestamp::from_millis(START + 30 * 24 * HOUR),
            scopes: scopes(&[
                "admin:agent-42-*",
                "stream:agent-42-*",
                "admin:agent-42.*",
                "stream:agent-42.*",
                "grant:agent-42.*",
            ]),
            kind: Some(TokenKind::Agent),
            owner: Some(principal("sam")),
            parent: None,
            chain: Vec::new(),
        },
        kid: "authority".parse().expect("a key id"),
    }
}

/// `caller` delegates its child `segment` for two hours from `now`, as operation and draw
/// `n`.
fn delegate(caller: &CallerToken, segment: &str, n: u8, now: Timestamp) -> Action {
    let child = format!("{}.{segment}", caller.claims.principal);
    let child_scopes = scopes(&[&format!("admin:{child}-*"), &format!("stream:{child}-*")]);
    let start = now.as_millis() - now.as_millis() % 1_000;
    Action::RecordDelegation {
        operation: OperationId::from_bytes([n; 16]),
        requested_by: caller.claims.principal.clone(),
        request: CanonicalRequest {
            segment: segment.into(),
            scopes: Some(child_scopes.clone()),
            lifetime: None,
            not_before: None,
            caller_token: caller.claims.id.clone(),
        },
        caller: caller.clone(),
        id: DelegationId::from_bytes([n ^ 0x8F; 16]),
        scopes: child_scopes,
        not_before: Timestamp::from_millis(start),
        expires_at: Timestamp::from_millis(start + 2 * HOUR),
    }
}

#[tokio::test(start_paused = true)]
#[ignore = "writes the 0.9.0 control fixture; scripts/write-control-fixtures.sh runs it at the tag"]
async fn write() {
    let out = PathBuf::from(std::env::var("FIXTURE_OUT").expect("FIXTURE_OUT names a directory"));
    std::fs::create_dir_all(&out).expect("the output directory can be created");
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("control.db");
    let options = ControlOptions::new(ControlConfig::new())
        .with_recorder(id(RECORDER))
        .with_snapshot_every(NonZeroU64::new(25).expect("nonzero"))
        .with_storage_io(StorageIo::Inline)
        .with_tombstone_window(Millis::new(7 * 24 * HOUR));
    let node = ControlNode::open(&path, options, SimClock::new(Timestamp::from_millis(START)))
        .await
        .expect("the control node opens");
    let w = Writer {
        handle: node.handle(),
    };

    // Members, and their heartbeats with software and free disk.
    for (name, zone_name) in MEMBERS {
        let member = Member::new(id(name))
            .with_zone(zone(zone_name))
            .with_software(software());
        w.run(10, Action::Join { member }).await;
    }
    w.heartbeats(|_| Vec::new()).await;

    // Databases. The first creation under `always` makes the writer log `EnableIncarnations`.
    let mut notes = create(NOTES, 3, 0x0000_0000_0000_0101);
    if let Action::CreateDatabase { metadata, .. } = &mut notes {
        *metadata = Some(MetadataSpec {
            description: Some("Meeting notes".parse::<Description>().expect("a description")),
            created_by: Some(principal("sam")),
            ..MetadataSpec::default()
        });
    }
    w.run(10, notes).await;
    let mut billing = create(BILLING, 3, 0x0000_0000_0000_0102);
    if let Action::CreateDatabase { durability, .. } = &mut billing {
        *durability = Some(Durability::new(Acks::All, factor(3)));
    }
    w.run(10, billing).await;
    let mut scratch = create(SCRATCH, 1, 0x0000_0000_0000_0103);
    if let Action::CreateDatabase { metadata, .. } = &mut scratch {
        *metadata = Some(MetadataSpec {
            expires_at: Some(Timestamp::from_millis(START + 3_000)),
            ..MetadataSpec::default()
        });
    }
    w.run(10, scratch).await;

    // Leases on each database's preferred primary, and their renewals.
    for name in [NOTES, BILLING, SCRATCH] {
        let primary = w.replica_set(name)[0].clone();
        w.run(
            10,
            Action::AcquireLease {
                database: db(name),
                node: primary.clone(),
            },
        )
        .await;
        w.run(
            10,
            Action::RenewLease {
                database: db(name),
                node: primary,
                epoch: w.epoch(name),
            },
        )
        .await;
    }
    let (key, value) = fleetring_control::parse_label("team=platform").expect("a label");
    let set_labels = fleetring_control::Labels::try_from_pairs([(key, value)]).expect("labels");
    w.run(
        10,
        Action::UpdateDatabase {
            name: db(NOTES),
            change: MetadataChange {
                description: DescriptionChange::Set(
                    "Meeting notes, reviewed".parse().expect("a description"),
                ),
                set_labels,
                remove_labels: BTreeSet::new(),
                expiry: ExpiryChange::Keep,
            },
            by: Some(principal("sam")),
            if_version: None,
        },
    )
    .await;
    w.run(10, Action::Tick).await;

    // A drain and an undrain of n4, which hosts no replica.
    w.run(10, Action::Drain { id: id("n4") }).await;
    w.run(10, Action::Undrain { id: id("n4") }).await;

    // A fork of the notes, carried through seeding, its first lease, readiness, and the release
    // of its holders.
    let mut intent = ForkIntent::new(db(FORK), db(NOTES), factor(3));
    intent.requested_by = Some(principal("sam"));
    let operation = OperationId::from_bytes([0x0F; 16]);
    w.run(
        10,
        Action::ForkDatabase {
            operation,
            intent,
            incarnation: Incarnation::new(0x0000_0000_0000_0201),
            resolved_expiry: None,
            limits: ForkLimits {
                max_attempts: 3,
                deadline: Millis::new(HOUR),
                attempt_timeout: Millis::new(600_000),
                seeder_timeout: Millis::new(60_000),
                operation_retention: Millis::new(24 * HOUR),
                admission_min_free_bytes: None,
            },
        },
    )
    .await;
    let fork = ForkIdentity::new(db(FORK), w.incarnation(FORK), operation);
    let source_incarnation = w.incarnation(NOTES);
    let holders = w.replica_set(FORK);
    for holder in &holders {
        let staged = StagedCopy::new(
            fork.clone(),
            1,
            Position::new([1, 0]),
            ChainHash::new([7, 7]),
            source_incarnation,
        );
        w.run(
            10,
            Action::Heartbeat {
                id: holder.clone(),
                replicas: vec![
                    HostedReplica::new(db(FORK), ReplicaRole::Follower, Position::default())
                        .with_staged(staged),
                ],
                unknown_reports: UnknownReports::Skip,
                software: None,
                disk_free_bytes: Some(40 << 30),
            },
        )
        .await;
    }
    w.drive_forks().await;
    w.run(
        10,
        Action::AcquireLease {
            database: db(FORK),
            node: holders[0].clone(),
        },
    )
    .await;
    let fork_epoch = w.epoch(FORK);
    for holder in &holders {
        let report = HostedReplica::new(db(FORK), ReplicaRole::Primary, Position::new([9]))
            .with_reset(ResetReport::committed(
                fork.clone(),
                fork_epoch,
                Position::new([3]),
            ));
        w.run(
            10,
            Action::Heartbeat {
                id: holder.clone(),
                replicas: vec![report],
                unknown_reports: UnknownReports::Skip,
                software: None,
                disk_free_bytes: None,
            },
        )
        .await;
    }
    w.drive_forks().await;
    let membership = w.state().membership_epoch();
    w.run(
        10,
        Action::Heartbeat {
            id: holders[0].clone(),
            replicas: vec![
                HostedReplica::new(db(FORK), ReplicaRole::Primary, Position::new([4]))
                    .with_log_epoch(fork_epoch)
                    .with_placement_caught_up(PlacementCaughtUp::new(
                        fork.clone(),
                        fork_epoch,
                        membership,
                    )),
            ],
            unknown_reports: UnknownReports::Skip,
            software: None,
            disk_free_bytes: None,
        },
    )
    .await;
    w.drive_forks().await;
    w.run(10, Action::Tick).await;

    // A delegation, its resend (answered from the state, not logged), and revocations.
    let caller = root_token();
    let delegation = delegate(&caller, "c7", 0xD1, w.now());
    let recorded = w.run(10, delegation.clone()).await;
    let Applied::DelegationRecorded { claims, .. } = recorded else {
        panic!("the delegation wasn't recorded: {recorded:?}");
    };
    let resent = w.run(10, delegation).await;
    assert!(
        matches!(resent, Applied::DelegationRecorded { .. }),
        "{resent:?}"
    );
    w.run(
        10,
        Action::RevokeToken {
            revoked: Revoked::Token(claims.id.clone()),
            until: claims.expires_at,
        },
    )
    .await;
    w.run(
        10,
        Action::RevokePrincipal {
            principal: principal("agent-42.c7"),
        },
    )
    .await;
    // A second child, which nothing revokes, so the state keeps its record.
    w.run(10, delegate(&caller, "c8", 0xD2, w.now())).await;

    // A leave of n3 as `fleetring node remove` logs it. It is the first membership change that
    // keeps a replica set, so the writer logs `EnableHandover` before it.
    w.heartbeats(|_| Vec::new()).await;
    w.run(10, Action::Leave { id: id("n3") }).await;
    assert!(w.state().is_enabled(Feature::Handover), "the leave enabled handover");
    // The billing database's primary reports placement caught up, and the leader's timers
    // release its kept set; the notes keep theirs.
    let membership = w.state().membership_epoch();
    let primary = w
        .state()
        .database(&db(BILLING))
        .and_then(|database| database.lease())
        .map(|lease| lease.holder().clone())
        .expect("the billing database is leased");
    let report = caught_up_primary(BILLING, w.incarnation(BILLING), w.epoch(BILLING), membership);
    w.run(
        10,
        Action::Heartbeat {
            id: primary,
            replicas: vec![report],
            unknown_reports: UnknownReports::Skip,
            software: None,
            disk_free_bytes: None,
        },
    )
    .await;
    let releases = w.state().releases_due();
    assert!(!releases.is_empty(), "the billing database's kept set is due for release");
    for release in releases {
        w.run(10, release).await;
    }

    // A drop, before which the writer logs `SetTombstoneWindow`, and an expiry, so tombstones
    // are kept.
    w.run(10, Action::DropDatabase { name: db(BILLING) }).await;
    let until_expiry = (START + 3_000).saturating_sub(w.now().as_millis()) + 10;
    w.heartbeats(|_| Vec::new()).await;
    w.run(until_expiry, Action::ExpireDatabase { name: db(SCRATCH) })
        .await;
    w.heartbeats(|_| Vec::new()).await;
    w.run(10, Action::Tick).await;

    let state = w.state();
    node.shutdown().await;

    // What the fixture must hold.
    assert!(state.is_enabled(Feature::Handover), "handover is enabled");
    assert!(
        state
            .database(&db(FORK))
            .and_then(|database| database.fork())
            .is_some_and(|record| matches!(record.state(), ForkState::Ready { .. })),
        "the fork is ready"
    );
    assert!(state.delegations().len() > 0, "a delegation is recorded");
    assert!(
        state.databases().any(|database| database.kept_replicas().is_some()),
        "a replica set is kept"
    );
    assert!(!state.tombstones().is_empty(), "tombstones are kept");

    // The log as 0.9.0 left it, and its commands in order.
    let log = CommandLog::open(&path, ControlConfig::new()).expect("the log reopens");
    let recovered = log.recover().expect("the log recovers");
    assert_eq!(*state, recovered.state, "the log recovers to the published state");
    assert!(recovered.snapshot.is_some(), "a snapshot was taken partway");
    let commands: Vec<_> = log
        .commands()
        .expect("the commands read back")
        .into_iter()
        .map(|(_, command)| command)
        .collect();
    drop(log);
    let logged = |name: &str| {
        commands
            .iter()
            .filter(|command| {
                serde_json::to_value(&command.action)
                    .ok()
                    .and_then(|value| match value {
                        serde_json::Value::String(unit) => Some(unit),
                        serde_json::Value::Object(map) => map.keys().next().cloned(),
                        _ => None,
                    })
                    .is_some_and(|action| action == name)
            })
            .count()
    };
    for name in [
        "enable_incarnations",
        "enable_handover",
        "set_tombstone_window",
        "release_kept_replicas",
        "complete_fork",
        "mark_fork_ready",
        "release_fork_holders",
        "revoke_token",
        "revoke_principal",
        "drop_database",
        "expire_database",
        "update_database",
        "undrain",
    ] {
        assert!(logged(name) > 0, "the log holds a {name}");
    }
    // The resend was answered from the state.
    assert_eq!(logged("record_delegation"), 2, "the resend isn't logged");
    std::fs::write(
        out.join("commands.json"),
        serde_json::to_string_pretty(&commands).expect("the commands encode") + "\n",
    )
    .expect("commands.json can be written");
    assert!(
        !dir.path().join("control.db-wal").exists(),
        "closing the log checkpointed its WAL"
    );
    std::fs::copy(&path, out.join("control.db")).expect("control.db can be copied");
}
