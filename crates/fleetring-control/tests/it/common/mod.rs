//! Shared helpers: label shorthands, a test configuration, and a command generator that peeks at
//! the state so that generated logs reach leases, promotions, and fencing often.

use fleetring::{NodeId, ReplicationFactor, Weight, Zone};
use fleetring_control::{
    Acks, Action, Command, ControlConfig, DatabaseName, DescriptionChange, Durability, Engine,
    ExpiryChange, Feature, HostedReplica, LeaseEpoch, Member, MetadataChange, MetadataSpec, Millis,
    Outcome, Position, Release, ReplicaRole, Revoked, Software, State, Timestamp, apply,
    parse_label,
};
use proptest::prelude::*;

/// Node IDs the generator draws from.
pub(crate) const NODES: [&str; 5] = ["n0", "n1", "n2", "n3", "n4"];

/// Database names the generator draws from.
pub(crate) const DATABASES: [&str; 3] = ["d0", "d1", "d2"];

pub(crate) fn id(name: &str) -> NodeId {
    name.parse().expect("valid node id")
}

pub(crate) fn db(name: &str) -> DatabaseName {
    name.parse().expect("valid database name")
}

pub(crate) fn zone(name: &str) -> Zone {
    name.parse().expect("valid zone")
}

pub(crate) fn at(millis: u64) -> Timestamp {
    Timestamp::from_millis(millis)
}

pub(crate) fn factor(value: u8) -> ReplicationFactor {
    ReplicationFactor::new(value).expect("nonzero replication factor")
}

/// 1 s leases, a 200 ms failover margin, and a 3 s heartbeat timeout.
pub(crate) fn config() -> ControlConfig {
    ControlConfig::new()
        .with_lease_duration(Millis::new(1_000))
        .with_failover_margin(Millis::new(200))
        .with_heartbeat_timeout(Millis::new(3_000))
}

/// The generator's default attributes for node `index`: three zones, alternating weights.
pub(crate) fn member(index: usize) -> Member {
    let weight = Weight::new(1 + u32::try_from(index % 2).expect("small")).expect("nonzero");
    Member::new(id(NODES[index]))
        .with_zone(zone(&format!("z{}", index % 3)))
        .with_weight(weight)
}

pub(crate) fn create(name: &str, replication_factor: u8) -> Action {
    Action::CreateDatabase {
        name: db(name),
        replication_factor: factor(replication_factor),
        engine: Engine::Sqlite,
        durability: None,
        metadata: None,
    }
}

/// Creates `name` with `replication_factor` replicas in `acks`, with the floor at W.
pub(crate) fn create_with(name: &str, replication_factor: u8, acks: Acks) -> Action {
    Action::CreateDatabase {
        name: db(name),
        replication_factor: factor(replication_factor),
        engine: Engine::Sqlite,
        durability: Some(Durability::new(acks, factor(replication_factor))),
        metadata: None,
    }
}

pub(crate) fn heartbeat(node: &str, reports: &[(&str, &[u8])]) -> Action {
    Action::Heartbeat {
        id: id(node),
        replicas: reports
            .iter()
            .map(|(name, position)| {
                HostedReplica::new(db(name), ReplicaRole::Follower, Position::new(*position))
            })
            .collect(),
        unknown_reports: fleetring_control::UnknownReports::Skip,
        software: None,
    }
}

/// Applies `action` at `millis` and returns the outcome.
pub(crate) fn run(state: &mut State, millis: u64, action: Action) -> Outcome {
    apply(state, Command::new(at(millis), action))
}

/// One generator step: which command to make, two selectors, and how far time moves.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Op {
    pub(crate) kind: u8,
    pub(crate) a: u8,
    pub(crate) b: u8,
    pub(crate) dt: u16,
}

pub(crate) fn arb_op() -> impl Strategy<Value = Op> {
    (any::<u8>(), any::<u8>(), any::<u8>(), 0_u16..400).prop_map(|(kind, a, b, dt)| Op {
        kind,
        a,
        b,
        dt,
    })
}

/// What software generated members report.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum SoftwareMix {
    /// Releases from two adjacent lines and one too far from them, some without any feature, so
    /// that version refusals and feature gates replay.
    #[default]
    Mixed,
    /// Every member runs a current release with every feature, so that commands the features
    /// gate, such as database metadata and expiry, are accepted and replay.
    Current,
}

/// Turns [`Op`]s into commands, keeping the control node's clock.
///
/// The generator reads the state only to aim commands at things that exist (the current lease
/// holder, a promotion's epoch, replica-set members), and sometimes aims them wrong on purpose.
#[derive(Debug, Default)]
pub(crate) struct Generator {
    clock: u64,
    mix: SoftwareMix,
}

impl Generator {
    /// A generator whose members report `mix`.
    pub(crate) fn with_mix(mix: SoftwareMix) -> Self {
        Self { clock: 0, mix }
    }

    pub(crate) fn command(&mut self, state: &State, op: Op) -> Command {
        self.clock += u64::from(op.dt);
        // Now and then the stamp goes backwards, as a control node's clock might.
        let stamp = if op.b.is_multiple_of(13) {
            self.clock.saturating_sub(500)
        } else {
            self.clock
        };
        Command::new(at(stamp), action(state, op, self.mix))
    }
}

/// The software a current member reports: a 0.6 release with every feature.
pub(crate) fn current_software() -> Software {
    Feature::ALL.into_iter().fold(
        Software::new(Release::new(0, 6, 0), 1),
        Software::with_feature,
    )
}

/// Software a generated member reports: releases from two adjacent lines and one too far from
/// them, with and without the `undrain` feature.
pub(crate) fn software_for(b: usize) -> Software {
    match b % 4 {
        0 => Software::new(Release::new(0, 3, 1), 1),
        1 => Software::new(Release::new(0, 4, 0), 1).with_feature(Feature::Undrain),
        2 => Software::new(Release::new(0, 4, 2), 1).with_feature(Feature::Undrain),
        _ => Software::new(Release::new(0, 6, 0), 1).with_feature(Feature::Undrain),
    }
}

fn action(state: &State, op: Op, mix: SoftwareMix) -> Action {
    let a = usize::from(op.a);
    let b = usize::from(op.b);
    let node = NODES[a % NODES.len()];
    let database = db(DATABASES[a % DATABASES.len()]);
    let reported = |b: usize| match mix {
        SoftwareMix::Mixed => software_for(b),
        SoftwareMix::Current => current_software(),
    };
    match op.kind % 16 {
        0 => {
            let mut joining = member(a % NODES.len());
            if b.is_multiple_of(7) {
                joining = joining.with_weight(Weight::new(3).expect("nonzero"));
            }
            // Members move between addresses, so replays cover them too.
            if b.is_multiple_of(2) {
                joining = joining.with_address(format!("{node}.local:{}", 7400 + b % 3));
            }
            // Members report software, sometimes too far apart to run together, so replays cover
            // version refusals and feature gates too. Current members always report theirs.
            if b % 5 == 1 || mix == SoftwareMix::Current {
                joining = joining.with_software(reported(b));
            }
            Action::Join { member: joining }
        }
        1 if b.is_multiple_of(3) => Action::Leave { id: id(node) },
        1 if b % 3 == 1 => Action::Tick,
        // Drops are rare, so that databases live long enough to reach promotions.
        8 if b.is_multiple_of(10) => Action::DropDatabase { name: database },
        8 if b % 10 == 5 => Action::Drain { id: id(node) },
        8 if b % 10 == 9 => Action::Undrain { id: id(node) },
        // Revocations of a few tokens and keys, some already past, so the list grows, shrinks
        // with time, and rejects.
        8 if b % 10 == 3 => Action::RevokeToken {
            revoked: if a.is_multiple_of(4) {
                Revoked::Key(format!("k{}", a % 3).parse().expect("valid key id"))
            } else {
                Revoked::Token(format!("t{}", a % 5).parse().expect("valid token id"))
            },
            until: at(state.now().as_millis() + u64::from(op.a) * 20).max(at(u64::from(op.b) * 10)),
        },
        // Metadata only among current members: in a mixed fleet the gate refuses it, except in
        // a fleet with no members at all, where it would enable the feature and refuse every
        // later join, and the rest of the log would test nothing.
        8 if b % 10 == 1 && mix == SoftwareMix::Current => update(state, op, database),
        // Half of the expiries aim where the control node's sweep would, the rest anywhere.
        8 if b % 10 == 6 && mix == SoftwareMix::Current => Action::ExpireDatabase {
            name: state
                .expired_databases(state.now())
                .into_iter()
                .find(|_| a.is_multiple_of(2))
                .unwrap_or(database),
        },
        8 if b % 10 == 7 => Action::TransferLease {
            database,
            target: (b % 4 == 3).then(|| id(NODES[(a + 1) % NODES.len()])),
        },
        1 | 8 if b % 5 == 4 => Action::ChangeDurability {
            database,
            durability: Durability::new(acks_for(b), factor(1 + op.b % 3)),
        },
        1 | 8 => {
            let factor = 1 + op.b % 3;
            let mut creation = if b.is_multiple_of(4) {
                create(DATABASES[a % DATABASES.len()], factor)
            } else {
                create_with(DATABASES[a % DATABASES.len()], factor, acks_for(b))
            };
            if b % 4 == 2
                && mix == SoftwareMix::Current
                && let Action::CreateDatabase { metadata, .. } = &mut creation
            {
                *metadata = Some(metadata_spec(state, op));
            }
            creation
        }
        2..=7 => {
            let mut replicas: Vec<HostedReplica> = DATABASES
                .iter()
                .enumerate()
                .filter(|(i, name)| (b >> i) & 1 == 1 && state.database(&db(name)).is_some())
                .map(|(i, name)| {
                    let position = vec![op.b.rotate_left(u32::try_from(i).expect("small")), op.a];
                    // Some replicas report the epoch their log came from, as newer nodes do.
                    let log_epoch = LeaseEpoch::new(u64::from(op.a % 3));
                    HostedReplica::new(db(name), ReplicaRole::Follower, Position::new(position))
                        .with_log_epoch(log_epoch)
                })
                .collect();
            if b.is_multiple_of(11) {
                replicas.push(HostedReplica::new(
                    db("missing"),
                    ReplicaRole::Follower,
                    Position::default(),
                ));
            }
            // Both rules replay: logs written before the field have `Reject` heartbeats.
            let unknown_reports = if b.is_multiple_of(3) {
                fleetring_control::UnknownReports::Reject
            } else {
                fleetring_control::UnknownReports::Skip
            };
            Action::Heartbeat {
                id: id(node),
                replicas,
                unknown_reports,
                software: (b % 7 == 2).then(|| reported(b)),
            }
        }
        9 => Action::AcquireLease {
            node: pick_replica(state, &database, b),
            database,
        },
        10 => {
            let lease = state.database(&database).and_then(|d| d.lease());
            let (node, epoch) = match lease {
                Some(lease) if !b.is_multiple_of(5) => (lease.holder().clone(), lease.epoch()),
                Some(lease) => (id(NODES[b % NODES.len()]), lease.epoch()),
                None => (id(node), LeaseEpoch::new(1)),
            };
            Action::RenewLease {
                database,
                node,
                epoch,
            }
        }
        11 => Action::StartPromotion { database },
        12..=14 => Action::RecordFenced {
            node: pick_replica(state, &database, b),
            epoch: promotion_epoch(state, &database, b),
            position: Position::new([op.b, op.a]),
            database,
        },
        _ => Action::CompletePromotion {
            epoch: promotion_epoch(state, &database, b),
            database,
        },
    }
}

/// What a generated creation records: a `purpose` label, sometimes a description, and sometimes
/// an expiry. The expiry is up to 12 s past the state's time, and sometimes at it, so that some
/// have already passed when the command runs.
fn metadata_spec(state: &State, op: Op) -> MetadataSpec {
    let purpose = if op.a.is_multiple_of(2) {
        "purpose=memory"
    } else {
        "purpose=task"
    };
    MetadataSpec {
        description: op
            .a
            .is_multiple_of(3)
            .then(|| format!("Generated database {}.\nIt holds nothing.", op.a))
            .map(|text| text.parse().expect("a valid description")),
        labels: fleetring_control::Labels::try_from_pairs([
            parse_label(purpose).expect("a valid label")
        ])
        .expect("one label"),
        expires_at: (op.a % 2 == 1)
            .then(|| at(state.now().as_millis() + u64::from(op.a / 4 % 4) * 4_000)),
        created_by: Some("agent-1".parse().expect("a valid principal name")),
    }
}

/// A metadata update: sets or removes a label, sometimes sets the expiry (sometimes to the
/// state's time, which is refused) or the description, and sometimes requires a version, the
/// current one or one off.
fn update(state: &State, op: Op, database: DatabaseName) -> Action {
    let a = usize::from(op.a);
    let mut change = MetadataChange::default();
    let key = format!("k{}", a % 3);
    if a.is_multiple_of(2) {
        change.set_labels = fleetring_control::Labels::try_from_pairs([parse_label(&format!(
            "{key}=v{}",
            op.b % 4
        ))
        .expect("a valid label")])
        .expect("one label");
    } else {
        change
            .remove_labels
            .insert(key.parse().expect("a valid key"));
    }
    if a.is_multiple_of(5) {
        let ahead = u64::from(op.b / 10 % 4) * 3_000;
        change.expiry = ExpiryChange::Set(at(state.now().as_millis() + ahead));
    }
    if a % 7 == 3 {
        change.description = DescriptionChange::Set("Updated.".parse().expect("valid"));
    }
    let version = state
        .database(&database)
        .and_then(|d| d.metadata())
        .map_or(0, fleetring_control::Metadata::version);
    Action::UpdateDatabase {
        name: database,
        change,
        by: Some("agent-2".parse().expect("a valid principal name")),
        if_version: a.is_multiple_of(3).then(|| version + u64::from(op.b % 2)),
    }
}

/// A member of the database's replica set, or any generator node if it has none.
/// Every mode in turn, so generated logs cover each one's quorums.
fn acks_for(b: usize) -> Acks {
    match b % 3 {
        0 => Acks::Quorum,
        1 => Acks::All,
        _ => Acks::Local,
    }
}

fn pick_replica(state: &State, database: &DatabaseName, b: usize) -> NodeId {
    match state.replica_set(database) {
        Ok(replicas) if !replicas.is_empty() && !b.is_multiple_of(9) => {
            replicas[b % replicas.len()].clone()
        }
        _ => id(NODES[b % NODES.len()]),
    }
}

/// The epoch of the database's promotion, sometimes one too low.
fn promotion_epoch(state: &State, database: &DatabaseName, b: usize) -> LeaseEpoch {
    let epoch = state
        .database(database)
        .and_then(|d| d.promotion())
        .map_or(LeaseEpoch::new(1), |promotion| promotion.epoch());
    if b.is_multiple_of(6) {
        LeaseEpoch::new(epoch.get().saturating_sub(1))
    } else {
        epoch
    }
}

/// A deterministic stream of [`Op`]s for tests that need a long fixed log (splitmix64).
pub(crate) fn fixed_ops(seed: u64, len: usize) -> Vec<Op> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let bytes = z.to_le_bytes();
            Op {
                kind: bytes[0],
                a: bytes[1],
                b: bytes[2],
                dt: u16::from_le_bytes([bytes[3], bytes[4]]) % 400,
            }
        })
        .collect()
}

/// Generates a command log from `ops` by running it against a fresh state, and returns the log
/// with the outcomes and the final state.
pub(crate) fn record(ops: &[Op]) -> (Vec<Command>, Vec<Outcome>, State) {
    record_with(ops, SoftwareMix::Mixed)
}

/// Generates a command log from `ops` as [`record`] does, with members reporting `mix`.
pub(crate) fn record_with(ops: &[Op], mix: SoftwareMix) -> (Vec<Command>, Vec<Outcome>, State) {
    let mut state = State::new(config());
    let mut generator = Generator::with_mix(mix);
    let mut log = Vec::with_capacity(ops.len());
    let mut outcomes = Vec::with_capacity(ops.len());
    for &op in ops {
        let command = generator.command(&state, op);
        log.push(command.clone());
        outcomes.push(apply(&mut state, command));
    }
    (log, outcomes, state)
}
