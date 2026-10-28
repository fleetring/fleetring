//! Shared helpers: label shorthands, a test configuration, and a command generator that peeks at
//! the state so that generated logs reach leases, promotions, and fencing often.

use fleetring::{NodeId, ReplicationFactor, Weight, Zone};
use fleetring_control::{
    Action, Command, ControlConfig, DatabaseName, Engine, HostedReplica, LeaseEpoch, Member,
    Millis, Outcome, Position, Role, State, Timestamp, apply,
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
    }
}

pub(crate) fn heartbeat(node: &str, reports: &[(&str, &[u8])]) -> Action {
    Action::Heartbeat {
        id: id(node),
        replicas: reports
            .iter()
            .map(|(name, position)| {
                HostedReplica::new(db(name), Role::Follower, Position::new(*position))
            })
            .collect(),
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

/// Turns [`Op`]s into commands, keeping the control node's clock.
///
/// The generator reads the state only to aim commands at things that exist (the current lease
/// holder, a promotion's epoch, replica-set members), and sometimes aims them wrong on purpose.
#[derive(Debug, Default)]
pub(crate) struct Generator {
    clock: u64,
}

impl Generator {
    pub(crate) fn command(&mut self, state: &State, op: Op) -> Command {
        self.clock += u64::from(op.dt);
        // Now and then the stamp goes backwards, as a control node's clock might.
        let stamp = if op.b.is_multiple_of(13) {
            self.clock.saturating_sub(500)
        } else {
            self.clock
        };
        Command::new(at(stamp), action(state, op))
    }
}

fn action(state: &State, op: Op) -> Action {
    let a = usize::from(op.a);
    let b = usize::from(op.b);
    let node = NODES[a % NODES.len()];
    let database = db(DATABASES[a % DATABASES.len()]);
    match op.kind % 16 {
        0 => {
            let mut joining = member(a % NODES.len());
            if b.is_multiple_of(7) {
                joining = joining.with_weight(Weight::new(3).expect("nonzero"));
            }
            Action::Join { member: joining }
        }
        1 if b.is_multiple_of(3) => Action::Leave { id: id(node) },
        1 | 8 => create(DATABASES[a % DATABASES.len()], 1 + op.b % 3),
        2..=7 => {
            let mut replicas: Vec<HostedReplica> = DATABASES
                .iter()
                .enumerate()
                .filter(|(i, name)| (b >> i) & 1 == 1 && state.database(&db(name)).is_some())
                .map(|(i, name)| {
                    let position = vec![op.b.rotate_left(u32::try_from(i).expect("small")), op.a];
                    HostedReplica::new(db(name), Role::Follower, Position::new(position))
                })
                .collect();
            if b.is_multiple_of(11) {
                replicas.push(HostedReplica::new(
                    db("missing"),
                    Role::Follower,
                    Position::default(),
                ));
            }
            Action::Heartbeat {
                id: id(node),
                replicas,
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

/// A member of the database's replica set, or any generator node if it has none.
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
    let mut state = State::new(config());
    let mut generator = Generator::default();
    let mut log = Vec::with_capacity(ops.len());
    let mut outcomes = Vec::with_capacity(ops.len());
    for &op in ops {
        let command = generator.command(&state, op);
        log.push(command.clone());
        outcomes.push(apply(&mut state, command));
    }
    (log, outcomes, state)
}
