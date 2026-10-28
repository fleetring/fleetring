//! Property tests: ADR-0018's invariants hold over generated command logs.

use std::collections::{BTreeMap, BTreeSet};

use fleetring_control::{
    Applied, Command, DatabaseName, JoinKind, LeaseEpoch, Liveness, Millis, Outcome, State, apply,
};
use proptest::prelude::*;

use crate::common::{Generator, NODES, Op, arb_op, config, id, member};

/// Runs `ops` through the generator and calls `check` with the state before each command, the
/// command, its outcome, and the state after it.
fn drive(ops: &[Op], mut check: impl FnMut(&State, &Command, &Outcome, &State)) -> State {
    let mut state = State::new(config());
    let mut generator = Generator::default();
    for &op in ops {
        let command = generator.command(&state, op);
        let before = state.clone();
        let outcome = apply(&mut state, command.clone());
        check(&before, &command, &outcome, &state);
    }
    state
}

fn lease_epoch(state: &State, database: &DatabaseName) -> Option<LeaseEpoch> {
    state
        .database(database)
        .and_then(|db| db.lease())
        .map(|lease| lease.epoch())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn no_lease_or_epoch_is_issued_at_or_below_the_current_one(
        ops in prop::collection::vec(arb_op(), 1..300),
    ) {
        drive(&ops, |before, _, outcome, after| {
            match outcome {
                Ok(
                    Applied::LeaseGranted { database, lease }
                    | Applied::PromotionCompleted { database, lease, .. },
                ) => {
                    let previous = lease_epoch(before, database).unwrap_or(LeaseEpoch::ZERO);
                    assert!(lease.epoch() > previous, "lease at {} after {previous}", lease.epoch());
                }
                Ok(Applied::PromotionStarted { database, epoch, .. }) => {
                    let current = before.database(database).map(|db| db.epoch());
                    assert!(Some(*epoch) > current, "epoch {epoch} issued after {current:?}");
                }
                _ => {}
            }
            for db in before.databases() {
                let now = after.database(db.name()).map(|db| db.epoch());
                assert!(now >= Some(db.epoch()), "epoch of {} went backwards", db.name());
            }
        });
    }

    #[test]
    fn no_two_leases_for_one_database_share_an_epoch(
        ops in prop::collection::vec(arb_op(), 1..300),
    ) {
        let mut granted: BTreeMap<DatabaseName, BTreeSet<LeaseEpoch>> = BTreeMap::new();
        drive(&ops, |_, _, outcome, _| {
            if let Ok(
                Applied::LeaseGranted { database, lease }
                | Applied::PromotionCompleted { database, lease, .. },
            ) = outcome
            {
                let fresh = granted.entry(database.clone()).or_default().insert(lease.epoch());
                assert!(fresh, "two leases of {database} at epoch {}", lease.epoch());
            }
        });
    }

    #[test]
    fn a_rejected_command_leaves_the_state_unchanged(
        ops in prop::collection::vec(arb_op(), 1..300),
    ) {
        drive(&ops, |before, command, outcome, after| {
            if let Err(rejection) = outcome {
                assert_eq!(after, before, "{command:?} was rejected ({rejection}) but changed the state");
            }
        });
    }

    #[test]
    fn a_promotion_never_completes_with_fewer_than_q_fenced_members(
        ops in prop::collection::vec(arb_op(), 1..300),
    ) {
        drive(&ops, |before, _, outcome, _| {
            let Ok(Applied::PromotionCompleted { database, lease, fenced, quorums, .. }) = outcome
            else {
                return;
            };
            assert!(*fenced >= quorums.promotion(), "{fenced} fenced, Q = {}", quorums.promotion());
            // Recount independently from the state the command ran against.
            let replicas = before.replica_set(database).expect("database exists");
            assert_eq!(usize::from(quorums.replicas()), replicas.len());
            let promotion = before
                .database(database)
                .and_then(|db| db.promotion())
                .expect("a promotion was in progress");
            let counted = promotion.fenced().filter(|(node, _)| replicas.contains(node)).count();
            assert!(counted >= usize::from(quorums.promotion()));
            assert!(promotion.fenced_position(lease.holder()).is_some(), "candidate not fenced");
            assert_eq!(lease.epoch(), promotion.epoch());
        });
    }

    #[test]
    fn a_member_marked_down_rejoins_with_its_old_id_cleanly(
        ops in prop::collection::vec(arb_op(), 0..150),
        index in 0..NODES.len(),
        silence in 1_u64..50_000,
    ) {
        let mut state = drive(&ops, |_, _, _, _| {});
        let joining = member(index);
        let node = id(NODES[index]);
        let start = state.now().as_millis();
        if state.member(&node).is_none() {
            let now = state.now();
            let joined = apply(&mut state, Command::new(now, fleetring_control::Action::Join {
                member: joining,
            }));
            prop_assume!(joined.is_ok());
        }
        let current = state.member(&node).cloned().expect("joined");

        // Silence past the heartbeat timeout marks the member down.
        let last = state.last_heartbeat(&node).expect("member").as_millis();
        let timeout = state.config().heartbeat_timeout().get();
        let later = fleetring_control::Timestamp::from_millis(last.max(start) + timeout + silence);
        prop_assert_eq!(state.liveness(&node, later), Some(Liveness::Down));

        let ring_before = state.ring().expect("not empty").nodes().to_vec();
        let epoch_before = state.membership_epoch();
        let databases_before: Vec<_> = state.databases().cloned().collect();

        let outcome = apply(&mut state, Command::new(later, fleetring_control::Action::Join {
            member: current.clone(),
        }));
        prop_assert_eq!(
            outcome,
            Ok(Applied::MemberJoined {
                id: node.clone(),
                membership_epoch: epoch_before,
                kind: JoinKind::Rejoined,
            })
        );
        prop_assert_eq!(state.liveness(&node, later), Some(Liveness::Live));
        prop_assert_eq!(state.member(&node), Some(&current));
        prop_assert_eq!(state.membership_epoch(), epoch_before);
        prop_assert_eq!(state.ring().expect("not empty").nodes().to_vec(), ring_before);
        let databases_after: Vec<_> = state.databases().cloned().collect();
        prop_assert_eq!(databases_after, databases_before);
        let still_live = later.saturating_add(Millis::new(timeout));
        prop_assert_eq!(state.liveness(&node, still_live), Some(Liveness::Live));
    }
}
