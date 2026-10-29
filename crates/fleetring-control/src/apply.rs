//! The state machine's transition function and its outcomes.

use std::collections::{BTreeMap, BTreeSet};
use std::iter;

use fleetring::{Epoch, NodeId, ReplicationFactor};
use serde::{Deserialize, Serialize};

use crate::command::UnknownReports;
use crate::state::{MemberEntry, build_ring, quorums_for};
use crate::{
    Action, Command, Database, DatabaseName, Durability, DurabilityError, Engine, HostedReplica,
    Lease, LeaseEpoch, Member, Position, Promotion, Quorums, State, Timestamp,
};

/// The result of applying a command: what changed, or why nothing did.
pub type Outcome = Result<Applied, Rejection>;

/// Applies `command` to `state` and reports what happened.
///
/// This is the only function that changes control state. It reads no clock, randomness, or
/// environment, so applying the same commands to the same state always gives the same states and
/// outcomes.
///
/// **Time.** A command runs at its stamp, or at [`State::now`] if the stamp is earlier: a stamp
/// that goes backwards counts as the last accepted time. Accepting a command moves
/// [`State::now`] to the time it ran at.
///
/// **Rejections** leave the state exactly as it was, including [`State::now`].
///
/// # Errors
///
/// Returns a [`Rejection`] when the command breaks a rule of ADR-0017 or ADR-0018; each variant
/// documents its rule.
///
/// # Examples
///
/// ```
/// use fleetring::ReplicationFactor;
/// use fleetring_control::{
///     apply, Action, Applied, Command, ControlConfig, Engine, LeaseEpoch, Member, State,
///     Timestamp,
/// };
///
/// let mut state = State::new(ControlConfig::new());
/// let at = Timestamp::from_millis(1_000);
/// let db = "tenant:acme".parse()?;
/// apply(&mut state, Command::new(at, Action::Join { member: Member::new("a".parse()?) }))?;
/// apply(&mut state, Command::new(at, Action::CreateDatabase {
///     name: db,
///     replication_factor: ReplicationFactor::ONE,
///     engine: Engine::Sqlite,
///     durability: None,
/// }))?;
/// let granted = apply(&mut state, Command::new(at, Action::AcquireLease {
///     database: "tenant:acme".parse()?,
///     node: "a".parse()?,
/// }))?;
/// let Applied::LeaseGranted { lease, .. } = granted else { panic!("expected a lease") };
/// assert_eq!(lease.epoch(), LeaseEpoch::new(1));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn apply(state: &mut State, command: Command) -> Outcome {
    let now = command.at.max(state.now);
    let applied = match command.action {
        Action::Join { member } => join(state, now, member),
        Action::Leave { id } => leave(state, id),
        Action::Heartbeat {
            id,
            replicas,
            unknown_reports,
        } => heartbeat(state, now, id, replicas, unknown_reports),
        Action::CreateDatabase {
            name,
            replication_factor,
            engine,
            durability,
        } => create_database(state, name, replication_factor, engine, durability),
        Action::ChangeDurability {
            database,
            durability,
        } => change_durability(state, database, durability),
        Action::DropDatabase { name } => drop_database(state, name),
        Action::AcquireLease { database, node } => acquire_lease(state, now, database, node),
        Action::RenewLease {
            database,
            node,
            epoch,
        } => renew_lease(state, now, &database, &node, epoch),
        Action::StartPromotion { database } => start_promotion(state, now, database),
        Action::RecordFenced {
            database,
            node,
            epoch,
            position,
        } => record_fenced(state, database, node, epoch, position),
        Action::CompletePromotion { database, epoch } => {
            complete_promotion(state, now, database, epoch)
        }
        Action::Tick => Ok(Applied::Ticked { at: now }),
    }?;
    state.now = now;
    Ok(applied)
}

/// What an accepted command did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Applied {
    /// A member joined, rejoined, or changed its placement attributes.
    MemberJoined {
        /// The member's ID.
        id: NodeId,
        /// The membership epoch after the join.
        membership_epoch: Epoch,
        /// Whether the member is new, rejoined unchanged, or changed its attributes.
        kind: JoinKind,
    },

    /// A member left.
    MemberLeft {
        /// The member's ID.
        id: NodeId,
        /// The membership epoch after the departure.
        membership_epoch: Epoch,
    },

    /// A heartbeat was recorded.
    HeartbeatRecorded {
        /// The member's ID.
        id: NodeId,
        /// The reported databases the control plane doesn't know, left out of the reports.
        unknown: Vec<DatabaseName>,
    },

    /// A database was created.
    DatabaseCreated {
        /// The database's name.
        database: DatabaseName,
    },

    /// New durability settings were recorded for a database, to take effect at its next lease.
    DurabilityChanged {
        /// The database's name.
        database: DatabaseName,
        /// The settings that take effect at the next lease.
        durability: Durability,
        /// The settings in effect until then.
        current: Durability,
    },

    /// A database was dropped, with its lease and any promotion.
    DatabaseDropped {
        /// The database's name.
        database: DatabaseName,
        /// The epoch the database was at when it was dropped. A node still serving it at that
        /// epoch stops once its next renewal is refused.
        epoch: LeaseEpoch,
    },

    /// A database's first lease was granted.
    LeaseGranted {
        /// The database.
        database: DatabaseName,
        /// The new lease.
        lease: Lease,
    },

    /// A lease was renewed.
    LeaseRenewed {
        /// The database.
        database: DatabaseName,
        /// The lease with its new expiry.
        lease: Lease,
    },

    /// A promotion started and issued a new epoch.
    PromotionStarted {
        /// The database.
        database: DatabaseName,
        /// The new epoch.
        epoch: LeaseEpoch,
        /// The chosen candidate.
        candidate: NodeId,
        /// The log position the candidate last reported.
        position: Position,
    },

    /// A member was recorded as fenced at a promotion's epoch.
    FenceRecorded {
        /// The database.
        database: DatabaseName,
        /// The fenced member.
        node: NodeId,
        /// How many current replica-set members are fenced now.
        fenced: u8,
        /// The promotion quorum Q.
        needed: u8,
    },

    /// A promotion completed and its candidate holds the lease.
    PromotionCompleted {
        /// The database.
        database: DatabaseName,
        /// The new lease.
        lease: Lease,
        /// How many replica-set members were fenced, the candidate included.
        fenced: u8,
        /// The replica set's quorums at completion.
        quorums: Quorums,
        /// The most advanced position among the fenced members. The new primary adopts this
        /// log before it accepts writes (ADR-0018, point 4).
        adopted: Position,
    },

    /// Time moved on, and nothing else changed.
    Ticked {
        /// The state's time after the tick.
        at: Timestamp,
    },
}

/// How a join changed the membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum JoinKind {
    /// The member is new. The membership epoch grew.
    New,
    /// A known member rejoined with the same attributes. The membership epoch didn't change.
    Rejoined,
    /// A known member rejoined with new attributes. The membership epoch grew.
    Updated,
}

/// Why a command was rejected. A rejected command changes nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Rejection {
    /// The node is not a member.
    #[error("node '{id}' is not a member")]
    UnknownMember {
        /// The node's ID.
        id: NodeId,
    },

    /// The database doesn't exist.
    #[error("database '{database}' does not exist")]
    UnknownDatabase {
        /// The database.
        database: DatabaseName,
    },

    /// A database with this name already exists.
    #[error("database '{database}' already exists")]
    DatabaseExists {
        /// The database.
        database: DatabaseName,
    },

    /// The cluster has no members, so no database has a replica set.
    #[error("the cluster has no members")]
    NoMembers,

    /// The durability settings don't fit the database's replication factor.
    #[error("invalid durability for database '{database}': {source}")]
    InvalidDurability {
        /// The database.
        database: DatabaseName,
        /// What is wrong with the settings.
        source: DurabilityError,
    },

    /// The join would make a ring that can't be built, such as a host in two zones.
    #[error("join would make an invalid ring: {reason}")]
    InvalidTopology {
        /// Why the ring can't be built.
        reason: String,
    },

    /// The membership epoch can't grow any further.
    #[error("membership epoch is exhausted")]
    MembershipEpochExhausted,

    /// A heartbeat reports the same database twice.
    #[error("heartbeat reports database '{database}' more than once")]
    DuplicateReport {
        /// The database.
        database: DatabaseName,
    },

    /// The node is down, so it can't take a lease.
    #[error("node '{id}' is down")]
    MemberDown {
        /// The node's ID.
        id: NodeId,
    },

    /// The node is not in the database's replica set.
    #[error("node '{id}' is not in the replica set of database '{database}'")]
    NotInReplicaSet {
        /// The node's ID.
        id: NodeId,
        /// The database.
        database: DatabaseName,
    },

    /// The database already has a lease; a new holder needs a promotion.
    #[error("database '{database}' already has a lease; a new holder needs a promotion")]
    LeaseAlreadyIssued {
        /// The database.
        database: DatabaseName,
    },

    /// The database has no lease yet.
    #[error("database '{database}' has no lease")]
    NoLease {
        /// The database.
        database: DatabaseName,
    },

    /// The node doesn't hold the database's lease.
    #[error("node '{id}' does not hold the lease of database '{database}'")]
    NotLeaseHolder {
        /// The node's ID.
        id: NodeId,
        /// The database.
        database: DatabaseName,
    },

    /// The command names an epoch that is not the current one.
    #[error("epoch {given} of database '{database}' is stale; the current epoch is {current}")]
    StaleEpoch {
        /// The database.
        database: DatabaseName,
        /// The epoch the command named.
        given: LeaseEpoch,
        /// The epoch the command needed to name.
        current: LeaseEpoch,
    },

    /// The database's epoch counter can't grow any further.
    #[error("epochs of database '{database}' are exhausted")]
    LeaseEpochExhausted {
        /// The database.
        database: DatabaseName,
    },

    /// The current lease hasn't expired by the failover margin yet.
    #[error("lease of database '{database}' can be replaced only after {available_after}")]
    LeaseNotExpired {
        /// The database.
        database: DatabaseName,
        /// The lease's expiry plus the failover margin. A promotion may start once the time is
        /// past it.
        available_after: Timestamp,
    },

    /// A promotion is already in progress and its candidate is live and not stalled.
    #[error("database '{database}' already has a promotion in progress")]
    PromotionInProgress {
        /// The database.
        database: DatabaseName,
    },

    /// No promotion is in progress.
    #[error("database '{database}' has no promotion in progress")]
    NoPromotion {
        /// The database.
        database: DatabaseName,
    },

    /// No live replica-set member has reported a position for the database.
    #[error("database '{database}' has no live candidate with a reported position")]
    NoCandidate {
        /// The database.
        database: DatabaseName,
    },

    /// The member is already fenced at this promotion's epoch.
    #[error("node '{id}' is already fenced for database '{database}'")]
    AlreadyFenced {
        /// The node's ID.
        id: NodeId,
        /// The database.
        database: DatabaseName,
    },

    /// The promotion's candidate is down or has left.
    #[error("candidate '{id}' is down")]
    CandidateDown {
        /// The candidate's ID.
        id: NodeId,
    },

    /// The promotion's candidate hasn't fenced itself yet.
    #[error("candidate '{id}' is not fenced")]
    CandidateNotFenced {
        /// The candidate's ID.
        id: NodeId,
    },

    /// Fewer than Q replica-set members are fenced.
    #[error("promotion needs {needed} fenced members; {fenced} are fenced")]
    QuorumNotReached {
        /// How many replica-set members are fenced.
        fenced: u8,
        /// The promotion quorum Q.
        needed: u8,
    },
}

fn join(state: &mut State, now: Timestamp, member: Member) -> Outcome {
    let id = member.id().clone();
    let kind = match state.members.get(&id) {
        None => JoinKind::New,
        Some(entry) if entry.member == member => JoinKind::Rejoined,
        Some(_) => JoinKind::Updated,
    };
    let membership_epoch = if kind == JoinKind::Rejoined {
        state.membership_epoch
    } else {
        let next = next_membership_epoch(state.membership_epoch)?;
        let others = state
            .members
            .values()
            .filter(|entry| entry.member.id() != &id)
            .map(|entry| entry.member.to_node());
        build_ring(others.chain(iter::once(member.to_node())), next).map_err(|error| {
            Rejection::InvalidTopology {
                reason: error.to_string(),
            }
        })?;
        next
    };

    state.membership_epoch = membership_epoch;
    state.members.insert(
        id.clone(),
        MemberEntry {
            member,
            last_heartbeat: now,
            reports: BTreeMap::new(),
        },
    );
    Ok(Applied::MemberJoined {
        id,
        membership_epoch,
        kind,
    })
}

fn leave(state: &mut State, id: NodeId) -> Outcome {
    if !state.members.contains_key(&id) {
        return Err(Rejection::UnknownMember { id });
    }
    let membership_epoch = next_membership_epoch(state.membership_epoch)?;

    state.members.remove(&id);
    state.membership_epoch = membership_epoch;
    Ok(Applied::MemberLeft {
        id,
        membership_epoch,
    })
}

fn heartbeat(
    state: &mut State,
    now: Timestamp,
    id: NodeId,
    replicas: Vec<HostedReplica>,
    unknown_reports: UnknownReports,
) -> Outcome {
    if !state.members.contains_key(&id) {
        return Err(Rejection::UnknownMember { id });
    }
    let mut seen = BTreeSet::new();
    let mut unknown = Vec::new();
    for replica in &replicas {
        if !seen.insert(replica.database()) {
            return Err(Rejection::DuplicateReport {
                database: replica.database().clone(),
            });
        }
        if state.database(replica.database()).is_some() {
            continue;
        }
        match unknown_reports {
            UnknownReports::Skip => unknown.push(replica.database().clone()),
            UnknownReports::Reject => {
                return Err(Rejection::UnknownDatabase {
                    database: replica.database().clone(),
                });
            }
        }
    }
    let reports = replicas
        .into_iter()
        .filter(|replica| !unknown.contains(replica.database()))
        .map(|replica| replica.into_report(now))
        .collect();

    let Some(entry) = state.members.get_mut(&id) else {
        return Err(Rejection::UnknownMember { id });
    };
    entry.last_heartbeat = now;
    entry.reports = reports;
    Ok(Applied::HeartbeatRecorded { id, unknown })
}

fn create_database(
    state: &mut State,
    name: DatabaseName,
    replication_factor: ReplicationFactor,
    engine: Engine,
    durability: Option<Durability>,
) -> Outcome {
    if state.databases.contains_key(&name) {
        return Err(Rejection::DatabaseExists { database: name });
    }
    let durability = durability.unwrap_or_else(|| Durability::quorum(replication_factor));
    durability
        .check(replication_factor)
        .map_err(|source| Rejection::InvalidDurability {
            database: name.clone(),
            source,
        })?;
    state.databases.insert(
        name.clone(),
        Database::new(name.clone(), replication_factor, engine, durability),
    );
    Ok(Applied::DatabaseCreated { database: name })
}

fn change_durability(state: &mut State, database: DatabaseName, durability: Durability) -> Outcome {
    let db = state.database_or_reject(&database)?;
    durability
        .check(db.replication_factor())
        .map_err(|source| Rejection::InvalidDurability {
            database: database.clone(),
            source,
        })?;
    let current = db.durability();
    let db = database_mut(state, &database)?;
    db.set_pending_durability(durability);
    Ok(Applied::DurabilityChanged {
        database,
        durability,
        current,
    })
}

/// Removes the database with every member's report of it, so that no stale report outlives it.
/// Reports are replaced wholesale by each heartbeat anyway; dropping them here keeps a snapshot
/// taken right after the drop identical to one taken after the next heartbeats.
fn drop_database(state: &mut State, name: DatabaseName) -> Outcome {
    let Some(database) = state.databases.remove(&name) else {
        return Err(Rejection::UnknownDatabase { database: name });
    };
    for entry in state.members.values_mut() {
        entry.reports.remove(&name);
    }
    Ok(Applied::DatabaseDropped {
        database: name,
        epoch: database.epoch(),
    })
}

fn acquire_lease(
    state: &mut State,
    now: Timestamp,
    database: DatabaseName,
    node: NodeId,
) -> Outcome {
    let db = state.database_or_reject(&database)?;
    if db.lease().is_some() {
        return Err(Rejection::LeaseAlreadyIssued { database });
    }
    let epoch = db
        .epoch()
        .next()
        .ok_or_else(|| Rejection::LeaseEpochExhausted {
            database: database.clone(),
        })?;
    require_member(state, &node)?;
    if !state.is_live(&node, now) {
        return Err(Rejection::MemberDown { id: node });
    }
    if !state.replica_set(&database)?.contains(&node) {
        return Err(Rejection::NotInReplicaSet { id: node, database });
    }
    let lease = Lease::new(
        node,
        epoch,
        now.saturating_add(state.config.lease_duration()),
    );

    let db = database_mut(state, &database)?;
    db.set_epoch(epoch);
    db.apply_pending_durability();
    db.set_lease(lease.clone());
    Ok(Applied::LeaseGranted { database, lease })
}

fn renew_lease(
    state: &mut State,
    now: Timestamp,
    database: &DatabaseName,
    node: &NodeId,
    epoch: LeaseEpoch,
) -> Outcome {
    let db = state.database_or_reject(database)?;
    let Some(lease) = db.lease() else {
        return Err(Rejection::NoLease {
            database: database.clone(),
        });
    };
    if epoch != db.epoch() || epoch != lease.epoch() {
        return Err(Rejection::StaleEpoch {
            database: database.clone(),
            given: epoch,
            current: db.epoch(),
        });
    }
    if lease.holder() != node {
        return Err(Rejection::NotLeaseHolder {
            id: node.clone(),
            database: database.clone(),
        });
    }
    require_member(state, node)?;
    let expires_at = now.saturating_add(state.config.lease_duration());

    let db = database_mut(state, database)?;
    let Some(lease) = db.lease_mut() else {
        return Err(Rejection::NoLease {
            database: database.clone(),
        });
    };
    lease.set_expires_at(expires_at);
    Ok(Applied::LeaseRenewed {
        database: database.clone(),
        lease: lease.clone(),
    })
}

fn start_promotion(state: &mut State, now: Timestamp, database: DatabaseName) -> Outcome {
    let PromotionPlan {
        epoch,
        candidate,
        position,
    } = plan_promotion(state, now, &database)?;

    let db = database_mut(state, &database)?;
    db.set_epoch(epoch);
    db.set_promotion(Some(Promotion::new(epoch, candidate.clone(), now)));
    Ok(Applied::PromotionStarted {
        database,
        epoch,
        candidate,
        position,
    })
}

/// What a promotion of one database would do if it started now.
#[derive(Debug)]
pub(crate) struct PromotionPlan {
    pub(crate) epoch: LeaseEpoch,
    pub(crate) candidate: NodeId,
    pub(crate) position: Position,
}

/// Checks whether a promotion of `database` may start at `now`, and picks its epoch and
/// candidate, without changing anything.
///
/// The candidate is the live replica-set member whose log is the most advanced: the highest log
/// epoch, then the highest position, and the earliest in preference order on ties.
pub(crate) fn plan_promotion(
    state: &State,
    now: Timestamp,
    database: &DatabaseName,
) -> Result<PromotionPlan, Rejection> {
    let config = state.config;
    let db = state.database_or_reject(database)?;
    let Some(lease) = db.lease() else {
        return Err(Rejection::NoLease {
            database: database.clone(),
        });
    };
    let available_after = lease.expires_at().saturating_add(config.failover_margin());
    if now <= available_after {
        return Err(Rejection::LeaseNotExpired {
            database: database.clone(),
            available_after,
        });
    }
    if let Some(promotion) = db.promotion() {
        let stalled_after = promotion.started_at().saturating_add(
            config
                .lease_duration()
                .saturating_add(config.failover_margin()),
        );
        if state.is_live(promotion.candidate(), now) && now <= stalled_after {
            return Err(Rejection::PromotionInProgress {
                database: database.clone(),
            });
        }
    }
    let epoch = db
        .epoch()
        .next()
        .ok_or_else(|| Rejection::LeaseEpochExhausted {
            database: database.clone(),
        })?;

    let mut best: Option<(&NodeId, LeaseEpoch, &Position)> = None;
    let replicas = state.replica_set(database)?;
    for id in &replicas {
        if !state.is_live(id, now) {
            continue;
        }
        let Some(report) = state.report(id, database) else {
            continue;
        };
        let rank = (report.log_epoch(), report.position());
        // Strictly greater, so the earliest in preference order wins ties (ADR-0018, point 7).
        if best.is_none_or(|(_, log_epoch, position)| rank > (log_epoch, position)) {
            best = Some((id, report.log_epoch(), report.position()));
        }
    }
    let Some((candidate, _, position)) = best else {
        return Err(Rejection::NoCandidate {
            database: database.clone(),
        });
    };
    Ok(PromotionPlan {
        epoch,
        candidate: candidate.clone(),
        position: position.clone(),
    })
}

fn record_fenced(
    state: &mut State,
    database: DatabaseName,
    node: NodeId,
    epoch: LeaseEpoch,
    position: Position,
) -> Outcome {
    let db = state.database_or_reject(&database)?;
    let promotion = current_promotion(db, &database, epoch)?;
    require_member(state, &node)?;
    let replicas = state.replica_set(&database)?;
    if !replicas.contains(&node) {
        return Err(Rejection::NotInReplicaSet { id: node, database });
    }
    if promotion.fenced_position(&node).is_some() {
        return Err(Rejection::AlreadyFenced { id: node, database });
    }
    let fenced = promotion
        .fenced()
        .filter(|(id, _)| replicas.contains(id))
        .count()
        .saturating_add(1);
    let fenced = u8::try_from(fenced).unwrap_or(u8::MAX);
    let needed = quorums_for(replicas.len(), db.durability().acks()).promotion();

    let db = database_mut(state, &database)?;
    let Some(promotion) = db.promotion_mut() else {
        return Err(Rejection::NoPromotion { database });
    };
    promotion.record_fenced(node.clone(), position);
    Ok(Applied::FenceRecorded {
        database,
        node,
        fenced,
        needed,
    })
}

fn complete_promotion(
    state: &mut State,
    now: Timestamp,
    database: DatabaseName,
    epoch: LeaseEpoch,
) -> Outcome {
    let db = state.database_or_reject(&database)?;
    let promotion = current_promotion(db, &database, epoch)?;
    let candidate = promotion.candidate().clone();
    if !state.is_live(&candidate, now) {
        return Err(Rejection::CandidateDown { id: candidate });
    }
    let replicas = state.replica_set(&database)?;
    if !replicas.contains(&candidate) {
        return Err(Rejection::NotInReplicaSet {
            id: candidate,
            database,
        });
    }
    if promotion.fenced_position(&candidate).is_none() {
        return Err(Rejection::CandidateNotFenced { id: candidate });
    }
    // The promotion fences under the settings the old epoch's writes were acknowledged with, so
    // every write it acknowledged reaches a fenced member; the new settings apply from the new
    // lease on.
    let quorums = quorums_for(replicas.len(), db.durability().acks());
    let fenced: Vec<&Position> = promotion
        .fenced()
        .filter(|(id, _)| replicas.contains(id))
        .map(|(_, position)| position)
        .collect();
    let fenced_count = u8::try_from(fenced.len()).unwrap_or(u8::MAX);
    if fenced_count < quorums.promotion() {
        return Err(Rejection::QuorumNotReached {
            fenced: fenced_count,
            needed: quorums.promotion(),
        });
    }
    let adopted = fenced.into_iter().max().cloned().unwrap_or_default();
    let lease = Lease::new(
        candidate,
        epoch,
        now.saturating_add(state.config.lease_duration()),
    );

    let db = database_mut(state, &database)?;
    db.apply_pending_durability();
    db.set_lease(lease.clone());
    db.set_promotion(None);
    Ok(Applied::PromotionCompleted {
        database,
        lease,
        fenced: fenced_count,
        quorums,
        adopted,
    })
}

/// Returns the database's promotion if `epoch` is its epoch.
fn current_promotion<'db>(
    db: &'db Database,
    database: &DatabaseName,
    epoch: LeaseEpoch,
) -> Result<&'db Promotion, Rejection> {
    let Some(promotion) = db.promotion() else {
        return Err(Rejection::NoPromotion {
            database: database.clone(),
        });
    };
    if promotion.epoch() != epoch {
        return Err(Rejection::StaleEpoch {
            database: database.clone(),
            given: epoch,
            current: promotion.epoch(),
        });
    }
    Ok(promotion)
}

fn require_member(state: &State, id: &NodeId) -> Result<(), Rejection> {
    if state.members.contains_key(id) {
        Ok(())
    } else {
        Err(Rejection::UnknownMember { id: id.clone() })
    }
}

fn database_mut<'s>(
    state: &'s mut State,
    database: &DatabaseName,
) -> Result<&'s mut Database, Rejection> {
    state
        .databases
        .get_mut(database)
        .ok_or_else(|| Rejection::UnknownDatabase {
            database: database.clone(),
        })
}

fn next_membership_epoch(epoch: Epoch) -> Result<Epoch, Rejection> {
    epoch
        .get()
        .checked_add(1)
        .map(Epoch::new)
        .ok_or(Rejection::MembershipEpochExhausted)
}
