//! FR-091 on the harness: `Control.DelegateToken` under faults, resends, issuer rotation, and a
//! racing revocation (ADR-0047, points 1.6, 5, and invariants I1, I4, and I7).
//!
//! The control host has a delegation issuer pair from the fleet's authority
//! ([`FleetBuilder::control_issuer`](fleetringd::sim::FleetBuilder::control_issuer)), and the
//! root agent `agent-42`'s token is signed by that authority, as `agent add` signs it.
//!
//! Every scenario checks the history the same way ([`check_delegations`]):
//!
//! - every answer for one operation ID names the same delegation, so a resend never mints a
//!   second (I7);
//! - no token was used beyond its scopes: every database a child created is one its scopes
//!   allow (I1);
//! - control state records each operation once.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::time::Duration;

use fleetring_control::delegation::DelegationId;
use fleetring_control::fork::OperationId;
use fleetring_control::{Action, Feature, Millis, Revoked, Timestamp};
use fleetringd::auth::{Action as ScopeAction, BearerToken, Scopes, Target};
use fleetringd::sim::{Ack, Delegations, Fleet, Operation, Operator, Outcome, Script, seeds};
use fleetringd::tls::{Principal, Validity};
use tonic::Code;

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const AGENT: &str = "agent";
const OPERATOR: &str = "operator";
const HOUR: Duration = Duration::from_secs(3_600);

/// `agent-42`'s root scopes: read-write within its namespace, and a grant over its children.
const ROOT_SCOPES: [&str; 5] = [
    "admin:agent-42-*",
    "admin:agent-42.*",
    "stream:agent-42-*",
    "stream:agent-42.*",
    "grant:agent-42.*",
];

fn fleet(seed: u64, data: usize) -> Result<Fleet, Box<dyn Error>> {
    let mut builder = Fleet::builder(seed)
        .control(CONTROL)
        .control_issuer(CONTROL);
    for host in DATA.iter().take(data) {
        builder = builder.data(host);
    }
    Ok(builder.operation_timeout(Duration::from_secs(1)).build()?)
}

fn agent_42() -> Result<Principal, Box<dyn Error>> {
    Ok("agent-42".parse()?)
}

/// `agent-42`'s root token, valid three hours.
fn root(fleet: &Fleet) -> Result<BearerToken, Box<dyn Error>> {
    Ok(fleet.agent_token("agent-42", "root-42", &ROOT_SCOPES, 3 * HOUR)?)
}

/// A delegation of `segment` under operation `operation`, as `agent-42` on its certificate.
fn delegate(
    token: &BearerToken,
    operation: u8,
    segment: &str,
    scopes: &[&str],
) -> Result<Operation, Box<dyn Error>> {
    Ok(Operation::Delegate {
        certificate: agent_42()?,
        token: token.clone(),
        operation: OperationId::from_bytes([operation; 16]),
        segment: segment.to_owned(),
        scopes: scopes.iter().map(|scope| (*scope).to_owned()).collect(),
        lifetime: None,
    })
}

/// Pushes `operation` to `operator` and runs until it is answered, then returns its outcome.
fn once(
    fleet: &mut Fleet,
    operator: &Operator,
    operation: Operation,
) -> Result<Outcome, Box<dyn Error>> {
    let before = answered(fleet, &operation);
    operator.push(operation.clone());
    fleet.run_until(Duration::from_secs(30), "an answer", |fleet| {
        answered(fleet, &operation) > before
    })?;
    let history = fleet.history();
    let record = history
        .records()
        .iter()
        .rev()
        .find(|record| *record.operation() == operation)
        .ok_or("no record")?;
    Ok(record.outcome().clone())
}

fn answered(fleet: &Fleet, operation: &Operation) -> usize {
    fleet
        .history()
        .records()
        .iter()
        .filter(|record| record.operation() == operation)
        .count()
}

fn delegated(outcome: &Outcome) -> Result<(BearerToken, DelegationId, String), Box<dyn Error>> {
    match outcome.ack() {
        Some(Ack::Delegated {
            token,
            delegation,
            issuer_key,
            ..
        }) => Ok((token.clone(), delegation.clone(), issuer_key.to_string())),
        _ => Err(format!("expected a delegation, got {outcome:?}").into()),
    }
}

fn assert_rejected(outcome: &Outcome, code: Code, says: &str) {
    match outcome {
        Outcome::Rejected { code: got, message } => {
            assert_eq!(*got, code, "{message}");
            assert!(message.contains(says), "{message:?} doesn't say {says:?}");
        }
        other => panic!("expected {code:?} saying {says:?}, got {other:?}"),
    }
}

/// The history's delegation checks (I1, I7), and that control state recorded each delegated
/// operation once. Returns how many operations were delegated.
fn check_delegations(fleet: &Fleet) -> Result<usize, Box<dyn Error>> {
    let history = fleet.history();
    let mut by_operation: BTreeMap<OperationId, BTreeSet<DelegationId>> = BTreeMap::new();
    let mut scopes_of: BTreeMap<String, Scopes> = BTreeMap::new();
    for (operation, ack) in history.acknowledged() {
        if let (
            Operation::Delegate { operation, .. },
            Ack::Delegated {
                token,
                delegation,
                scopes,
                ..
            },
        ) = (operation, ack)
        {
            by_operation
                .entry(*operation)
                .or_default()
                .insert(delegation.clone());
            let parsed: Scopes = scopes
                .iter()
                .map(|scope| scope.parse())
                .collect::<Result<_, _>>()?;
            scopes_of.insert(token.expose().to_owned(), parsed);
        }
    }
    for (operation, delegations) in &by_operation {
        assert_eq!(
            delegations.len(),
            1,
            "operation {operation} was answered with several delegations: {delegations:?}"
        );
    }
    for (operation, ack) in history.acknowledged() {
        if let (
            Operation::CreateAs {
                token, database, ..
            },
            Ack::Created,
        ) = (operation, ack)
        {
            let Some(scopes) = scopes_of.get(token.expose()) else {
                continue;
            };
            assert!(
                scopes.allows(Target::Database(ScopeAction::Admin, database)),
                "a child created {database}, beyond its scopes {scopes:?}"
            );
        }
    }
    let state = fleet.control_state().ok_or("the control node is down")?;
    let kept: Vec<_> = state
        .delegation_operations()
        .filter(|(operation, _)| by_operation.contains_key(operation))
        .collect();
    assert_eq!(
        kept.len(),
        by_operation.len(),
        "every delegated operation is kept once"
    );
    for (operation, kept) in kept {
        let answered = by_operation.get(operation).ok_or("an operation")?;
        assert!(answered.contains(&kept.delegation), "{operation}");
    }
    Ok(by_operation.len())
}

/// I1 and I7 under a partition, a control restart, and a rotation: `agent-42` delegates twelve
/// children, resending each delegation that got no answer, and uses each child's token inside
/// and outside its prefix.
fn delegates_under_partitions_and_control_restart(seed: u64) -> TestResult {
    let mut fleet = fleet(seed, 3)?;
    fleet.await_ring(Duration::from_secs(30))?;
    let token = root(&fleet)?;
    fleet.client(
        AGENT,
        Delegations::new(agent_42()?, token, 1, 12, Duration::from_millis(300)),
    );
    let delegations = |fleet: &Fleet| {
        fleet
            .history()
            .acknowledged()
            .filter(|(_, ack)| matches!(ack, Ack::Delegated { .. }))
            .count()
    };
    fleet.run_until(Duration::from_secs(60), "three delegations", |fleet| {
        delegations(fleet) >= 3
    })?;
    fleet.partition(AGENT, CONTROL)?;
    fleet.run_for(Duration::from_secs(2))?;
    fleet.repair(AGENT, CONTROL)?;
    fleet.run_until(Duration::from_secs(60), "six delegations", |fleet| {
        delegations(fleet) >= 6
    })?;
    fleet.crash(CONTROL)?;
    fleet.run_for(Duration::from_secs(1))?;
    fleet.rotate_issuer(CONTROL)?;
    fleet.restart(CONTROL)?;
    fleet.await_workloads(Duration::from_secs(300))?;
    let delegated = check_delegations(&fleet)?;
    assert_eq!(delegated, 12, "every child was delegated in the end");
    let keys: BTreeSet<String> = fleet
        .history()
        .acknowledged()
        .filter_map(|(_, ack)| match ack {
            Ack::Delegated { issuer_key, .. } => Some(issuer_key.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(
        keys.len(),
        2,
        "the rotation brought a second issuer key: {keys:?}"
    );
    fleet.finish()?;
    Ok(())
}

#[test]
fn delegate_under_partitions_and_control_restart() {
    seeds::run(
        BINARY,
        &seeds::FIXED,
        delegates_under_partitions_and_control_restart,
    );
}

/// I7: replies slower than the client waits, so each delegation is resent before its first
/// answer arrives; the control plane records each once and answers every resend with it.
fn lost_replies_record_once(seed: u64) -> TestResult {
    let mut fleet = fleet(seed, 1)?;
    fleet.await_ring(Duration::from_secs(30))?;
    let token = root(&fleet)?;
    fleet.client(
        AGENT,
        Delegations::new(agent_42()?, token, 2, 3, Duration::from_millis(100)),
    );
    fleet.set_latency(AGENT, CONTROL, Duration::from_millis(1_500))?;
    fleet.run_for(Duration::from_secs(8))?;
    fleet.set_latency(AGENT, CONTROL, Duration::from_millis(5))?;
    fleet.await_workloads(Duration::from_secs(120))?;
    let lost = fleet
        .history()
        .records()
        .iter()
        .filter(|record| {
            matches!(record.operation(), Operation::Delegate { .. })
                && matches!(record.outcome(), Outcome::Indeterminate { .. })
        })
        .count();
    assert!(lost > 0, "no delegation lost its reply");
    assert_eq!(check_delegations(&fleet)?, 3);
    let state = fleet.control_state().ok_or("the control node is down")?;
    assert_eq!(state.delegations().len(), 3, "recorded once each");
    fleet.finish()?;
    Ok(())
}

#[test]
fn lost_reply_retry_records_once() {
    seeds::run(BINARY, &seeds::FIXED, lost_replies_record_once);
}

/// How a race's revocation of `agent-42.c7` is made.
#[derive(Debug, Clone, Copy)]
enum Revocation {
    /// The operator revokes `c7`'s delegation ID with `fleet`.
    FleetToken,
    /// The operator revokes the principal `agent-42.c7` with `fleet`.
    FleetPrincipal,
    /// `agent-42` revokes the principal `agent-42.c7` under the ancestor rule.
    AncestorPrincipal,
    /// `agent-42` revokes `c7`'s delegation ID under the ancestor rule.
    AncestorToken,
}

/// I4 end to end: `agent-42.c7` delegates children while `how` revokes it. Every child recorded
/// before the revocation is pruned with it, none is recorded after, and no token of the subtree
/// works once the revocation is answered.
fn revocation_races(seed: u64, how: Revocation) -> TestResult {
    let mut fleet = fleet(seed, 1)?;
    fleet.await_ring(Duration::from_secs(30))?;
    let operator = Operator::new();
    fleet.client(OPERATOR, operator.clone());
    let root = root(&fleet)?;
    let c7 = once(
        &mut fleet,
        &operator,
        delegate(
            &root,
            1,
            "c7",
            &[
                "admin:agent-42.c7-*",
                "admin:agent-42.c7.*",
                "stream:agent-42.c7.*",
                "grant:agent-42.c7.*",
            ],
        )?,
    )?;
    let (c7_token, c7_id, _) = delegated(&c7)?;
    fleet.client(
        AGENT,
        Delegations::new(agent_42()?, c7_token, 3, 20, Duration::from_millis(150)),
    );
    let children = |fleet: &Fleet| {
        fleet
            .history()
            .records()
            .iter()
            .filter(|record| {
                record.client() == AGENT
                    && matches!(record.outcome().ack(), Some(Ack::Delegated { .. }))
            })
            .count()
    };
    let extra = usize::try_from(fleet.rng().below(5))?;
    fleet.run_until(Duration::from_secs(60), "some grandchildren", |fleet| {
        children(fleet) >= 3 + extra
    })?;
    let c7_name: fleetring_control::PrincipalName = "agent-42.c7".parse()?;
    let revoke = match how {
        Revocation::FleetToken => Operation::Revoke {
            revoked: Revoked::Token(c7_id.clone().into()),
            until: fleet.host_time().saturating_add(Millis::new(4 * 3_600_000)),
        },
        Revocation::FleetPrincipal => Operation::RevokePrincipal {
            principal: c7_name.clone(),
        },
        Revocation::AncestorPrincipal => Operation::RevokeAs {
            certificate: agent_42()?,
            token: root.clone(),
            principal: Some(c7_name.clone()),
            token_id: None,
        },
        Revocation::AncestorToken => Operation::RevokeAs {
            certificate: agent_42()?,
            token: root.clone(),
            principal: None,
            token_id: Some(c7_id.clone().into()),
        },
    };
    let revoked = once(&mut fleet, &operator, revoke.clone())?;
    assert_eq!(revoked.ack(), Some(&Ack::Revoked), "{how:?}: {revoked:?}");
    let answered_at = fleet
        .history()
        .records()
        .iter()
        .find(|record| *record.operation() == revoke)
        .map(|record| record.completed_at())
        .ok_or("no revocation")?;
    fleet.await_workloads(Duration::from_secs(300))?;

    let history = fleet.history();
    let mut refused_after = 0;
    for record in history
        .records()
        .iter()
        .filter(|record| record.client() == AGENT)
    {
        if record.invoked_at() <= answered_at {
            continue;
        }
        match (record.operation(), record.outcome()) {
            (Operation::Delegate { .. }, Outcome::Acknowledged(ack)) => {
                panic!(
                    "{how:?}: a child of the revoked agent-42.c7 was delegated after its \
                     revocation: {ack}"
                )
            }
            (Operation::CreateAs { database, .. }, Outcome::Acknowledged(_)) => {
                panic!("{how:?}: a token below the revoked agent-42.c7 created {database} after it")
            }
            (Operation::Delegate { .. }, Outcome::Rejected { .. }) => refused_after += 1,
            _ => {}
        }
    }
    assert!(
        refused_after > 0,
        "{how:?}: no delegation ran after the revocation"
    );
    let state = fleet.control_state().ok_or("the control node is down")?;
    let c7_token_id = c7_id.as_token_id();
    assert!(
        state.delegations().all(|(_, record)| {
            !record.descends_from(c7_token_id) && !record.principal().is_at_or_below(&c7_name)
        }),
        "{how:?}: a record below the revoked agent-42.c7 survived it"
    );
    // The root itself is untouched.
    let after = once(
        &mut fleet,
        &operator,
        delegate(&root, 2, "d1", &["admin:agent-42.d1-*"])?,
    )?;
    delegated(&after)?;
    check_delegations(&fleet)?;
    fleet.finish()?;
    Ok(())
}

#[test]
fn revocation_races_delegation() {
    seeds::run(BINARY, &seeds::FIXED, |seed| {
        revocation_races(seed, Revocation::FleetToken)
    });
}

#[test]
fn fleet_principal_revocation_races_delegation() {
    seeds::run(BINARY, &seeds::FIXED, |seed| {
        revocation_races(seed, Revocation::FleetPrincipal)
    });
}

#[test]
fn ancestor_principal_revocation_races_delegation() {
    seeds::run(BINARY, &seeds::FIXED, |seed| {
        revocation_races(seed, Revocation::AncestorPrincipal)
    });
}

#[test]
fn ancestor_token_revocation_races_delegation() {
    seeds::run(BINARY, &seeds::FIXED, |seed| {
        revocation_races(seed, Revocation::AncestorToken)
    });
}

/// The feature gate (ADR-0047, point 5.11): while a member lacks `delegation`, a delegation is
/// refused naming it and nothing is logged; once the member leaves, the first delegation
/// enables the feature, and the member can't come back without it.
fn feature_gate(seed: u64) -> TestResult {
    let mut fleet = fleet(seed, 1)?;
    fleet.await_ring(Duration::from_secs(30))?;
    let token = root(&fleet)?;
    let old = "old".parse()?;
    let first = delegate(&token, 1, "c7", &[])?;
    let second = delegate(&token, 2, "c8", &[])?;
    let without = Operation::JoinWithout {
        node: "old".parse()?,
        features: vec![Feature::Delegation],
    };
    fleet.client(
        AGENT,
        Script::new([
            without.clone(),
            first.clone(),
            Operation::Leave { node: old },
            second.clone(),
            without.clone(),
        ]),
    );
    fleet.await_workloads(Duration::from_secs(120))?;
    let history = fleet.history();
    let outcome = |operation: &Operation, nth: usize| {
        history
            .records()
            .iter()
            .filter(|record| record.operation() == operation)
            .nth(nth)
            .map(|record| record.outcome().clone())
            .ok_or("no record")
    };
    assert_rejected(&outcome(&first, 0)?, Code::FailedPrecondition, "node 'old'");
    delegated(&outcome(&second, 0)?)?;
    assert_rejected(
        &outcome(&without, 1)?,
        Code::FailedPrecondition,
        "delegation",
    );
    let report = fleet.finish()?;
    let records = report
        .commands()
        .iter()
        .filter(|command| matches!(command.action, Action::RecordDelegation { .. }))
        .count();
    assert_eq!(records, 1, "the refused delegation logged nothing");
    Ok(())
}

#[test]
fn member_without_delegation_refuses_naming_it_and_logs_nothing() {
    seeds::run(BINARY, &seeds::FIXED, feature_gate);
}

/// ADR-0047, point 1.6: a control node signs only with a pair it may use. Without one, with
/// one that expired or isn't valid yet, or with one that expires before the token would, a
/// delegation is refused with `FAILED_PRECONDITION` before anything is recorded.
fn issuer_refusals(seed: u64) -> TestResult {
    // No issuer at all.
    let mut bare = Fleet::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .build()?;
    bare.await_ring(Duration::from_secs(30))?;
    let operator = Operator::new();
    bare.client(OPERATOR, operator.clone());
    let token = root(&bare)?;
    let outcome = once(&mut bare, &operator, delegate(&token, 1, "c7", &[])?)?;
    assert_rejected(
        &outcome,
        Code::FailedPrecondition,
        "this fleet has no delegation issuer; run `fleetring cert issuer`",
    );
    bare.finish()?;

    let mut fleet = fleet(seed, 1)?;
    fleet.await_ring(Duration::from_secs(30))?;
    let operator = Operator::new();
    fleet.client(OPERATOR, operator.clone());
    // Expired: the pair the fleet started with lasts 30 days from the epoch.
    fleet.shift_clocks(Duration::from_secs(31 * 24 * 3_600));
    fleet.run_for(Duration::from_secs(3))?;
    let token = root(&fleet)?;
    let outcome = once(&mut fleet, &operator, delegate(&token, 1, "c7", &[])?)?;
    assert_rejected(&outcome, Code::FailedPrecondition, "has expired");
    // A rotation makes it usable again.
    fleet.rotate_issuer(CONTROL)?;
    fleet.run_for(Duration::from_secs(2))?;
    let outcome = once(&mut fleet, &operator, delegate(&token, 2, "c8", &[])?)?;
    delegated(&outcome)?;
    // Not yet valid: it starts in an hour.
    let now = fleet.host_time();
    fleet.replace_issuer(
        CONTROL,
        Validity::new(now.saturating_add(Millis::new(3_600_000)), 30 * 24 * HOUR),
    )?;
    fleet.run_for(Duration::from_secs(2))?;
    let outcome = once(&mut fleet, &operator, delegate(&token, 3, "c9", &[])?)?;
    assert_rejected(&outcome, Code::FailedPrecondition, "isn't valid yet");
    // Short: it expires in 30 minutes, before a default child's 2 hours.
    let start = Timestamp::from_millis(fleet.host_time().as_millis() - 3_600_000);
    fleet.replace_issuer(CONTROL, Validity::new(start, Duration::from_secs(90 * 60)))?;
    fleet.run_for(Duration::from_secs(2))?;
    let outcome = once(&mut fleet, &operator, delegate(&token, 4, "c10", &[])?)?;
    assert_rejected(
        &outcome,
        Code::FailedPrecondition,
        "expires before the token would",
    );
    let state = fleet.control_state().ok_or("the control node is down")?;
    assert_eq!(
        state.delegations().len(),
        1,
        "only the usable pair's delegation is recorded"
    );
    fleet.finish()?;
    Ok(())
}

#[test]
fn no_issuer_expired_issuer_not_yet_valid_issuer_and_short_issuer_are_failed_precondition() {
    seeds::run(BINARY, &seeds::FIXED, issuer_refusals);
}

/// I7 across a rotation and a restart: a resend after both returns the same delegation with
/// the same claims, signed by the new key, and both copies work.
fn resend_after_rotation(seed: u64) -> TestResult {
    let mut fleet = fleet(seed, 1)?;
    fleet.await_ring(Duration::from_secs(30))?;
    let operator = Operator::new();
    fleet.client(OPERATOR, operator.clone());
    let token = root(&fleet)?;
    let request = delegate(&token, 1, "c7", &[])?;
    let (a_token, a_id, a_key) = delegated(&once(&mut fleet, &operator, request.clone())?)?;
    fleet.rotate_issuer(CONTROL)?;
    fleet.crash(CONTROL)?;
    fleet.run_for(Duration::from_secs(1))?;
    fleet.restart(CONTROL)?;
    fleet.await_ring(Duration::from_secs(30))?;
    let resent = loop {
        let outcome = once(&mut fleet, &operator, request.clone())?;
        if outcome.ack().is_some() {
            break outcome;
        }
        fleet.run_for(Duration::from_millis(500))?;
    };
    let (b_token, b_id, b_key) = delegated(&resent)?;
    assert_eq!(b_id, a_id, "the same delegation");
    assert_ne!(b_key, a_key, "signed by the new key");
    assert_ne!(b_token, a_token);
    for (copy, database) in [(&a_token, "agent-42.c7-a"), (&b_token, "agent-42.c7-b")] {
        let created = once(
            &mut fleet,
            &operator,
            Operation::CreateAs {
                certificate: agent_42()?,
                token: copy.clone(),
                database: database.parse()?,
            },
        )?;
        assert_eq!(
            created.ack(),
            Some(&Ack::Created),
            "{database}: {created:?}"
        );
    }
    let state = fleet.control_state().ok_or("the control node is down")?;
    assert_eq!(state.delegations().len(), 1, "one delegation, two copies");
    fleet.finish()?;
    Ok(())
}

#[test]
fn retry_after_rotation_and_restart_returns_the_same_delegation_and_both_copies_work() {
    seeds::run(BINARY, &seeds::FIXED, resend_after_rotation);
}

/// ADR-0047, point 5.4: while control time leads this control node's clock by more than 30
/// seconds, a delegation is refused with `UNAVAILABLE`, and succeeds once the clock catches up.
fn clock_lead(seed: u64) -> TestResult {
    let mut fleet = fleet(seed, 1)?;
    fleet.await_ring(Duration::from_secs(30))?;
    let operator = Operator::new();
    fleet.client(OPERATOR, operator.clone());
    fleet.shift_clocks(Duration::from_secs(120));
    fleet.run_for(Duration::from_secs(3))?;
    fleet.set_clocks_back(Duration::from_secs(120));
    // Signed by the clock the agent's own machine reads, which never ran ahead.
    let token = root(&fleet)?;
    let request = delegate(&token, 1, "c7", &[])?;
    // The harness records `UNAVAILABLE` as an unknown outcome, so the state shows it took none.
    let outcome = once(&mut fleet, &operator, request.clone())?;
    assert_eq!(
        outcome,
        Outcome::Indeterminate {
            code: Some(Code::Unavailable)
        }
    );
    let state = fleet.control_state().ok_or("the control node is down")?;
    assert_eq!(
        state.delegations().len(),
        0,
        "nothing is recorded while control time leads"
    );
    fleet.run_for(Duration::from_secs(100))?;
    delegated(&once(&mut fleet, &operator, request)?)?;
    fleet.finish()?;
    Ok(())
}

#[test]
fn delegation_refused_while_control_time_leads_the_clock() {
    seeds::run(BINARY, &seeds::FIXED, clock_lead);
}

/// I7: a resend of an operation whose delegation has expired is refused, never minted again.
fn expired_retry(seed: u64) -> TestResult {
    let mut fleet = fleet(seed, 1)?;
    fleet.await_ring(Duration::from_secs(30))?;
    let operator = Operator::new();
    fleet.client(OPERATOR, operator.clone());
    let token = root(&fleet)?;
    let request = Operation::Delegate {
        certificate: agent_42()?,
        token: token.clone(),
        operation: OperationId::from_bytes([9; 16]),
        segment: "c7".to_owned(),
        scopes: Vec::new(),
        lifetime: Some(Millis::new(60_000)),
    };
    delegated(&once(&mut fleet, &operator, request.clone())?)?;
    fleet.run_for(Duration::from_secs(65))?;
    let outcome = once(&mut fleet, &operator, request)?;
    assert_rejected(
        &outcome,
        Code::FailedPrecondition,
        "this operation's delegation has expired",
    );
    let state = fleet.control_state().ok_or("the control node is down")?;
    assert_eq!(state.delegations().len(), 1, "never recorded again");
    fleet.finish()?;
    Ok(())
}

#[test]
fn retry_of_an_expired_delegation_is_refused_never_reminted() {
    seeds::run(BINARY, &seeds::FIXED, expired_retry);
}

/// How the release blocker's fleet moves from issuer key A to B (ADR-0047's plan, section 14):
/// in 0.8 a leader change is a control node restart with new issuer files, and a rotation a
/// reload of new files without one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Change {
    Reload,
    Restart,
}

/// The release blocker's six cases (section 14's table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Case {
    /// `Revoked::Key(A)`, by the operator.
    KeyA,
    /// `Revoked::Token(c7)`, by the operator.
    C7ByOperator,
    /// `Revoked::Token(c7)`, by `agent-42` under the ancestor rule.
    C7ByAncestor,
    /// `Revoked::Token(root jti)`, by the operator.
    RootToken,
    /// `Revoked::Principal(agent-42.c7)`, by `agent-42`.
    C7Principal,
    /// `agent remove 42`: `agent-42`'s principal and its token, by itself.
    RemoveRoot,
    /// A reused segment: `c7` revoked by ID, delegated again as `c7′` with `r1′` below it, then
    /// `agent-42.c7`'s principal revoked.
    ReusedSegment,
}

const CASES: [Case; 7] = [
    Case::KeyA,
    Case::C7ByOperator,
    Case::C7ByAncestor,
    Case::RootToken,
    Case::C7Principal,
    Case::RemoveRoot,
    Case::ReusedSegment,
];

/// `agent-42.c7`'s explicit scopes in the release blocker, `grant` included, so it delegates.
const C7_SCOPES: [&str; 5] = [
    "admin:agent-42.c7-*",
    "stream:agent-42.c7-*",
    "admin:agent-42.c7.*",
    "stream:agent-42.c7.*",
    "grant:agent-42.c7.*",
];

/// One token of the lineage, and the database it reads.
#[derive(Debug, Clone)]
struct Holder {
    name: &'static str,
    token: BearerToken,
    database: String,
}

/// The release blocker's lineage once set up.
struct Lineage {
    root: Holder,
    c7_a: Holder,
    c7_b: Holder,
    r1: Holder,
    r2: Holder,
    c7_id: DelegationId,
    a_key: String,
}

/// Pushes `operation` until it is acknowledged, letting the fleet run between tries: a
/// rotation or a restart may still be settling.
fn until_acknowledged(
    fleet: &mut Fleet,
    operator: &Operator,
    operation: &Operation,
) -> Result<Outcome, Box<dyn Error>> {
    for _ in 0..60 {
        let outcome = once(fleet, operator, operation.clone())?;
        if outcome.ack().is_some() {
            return Ok(outcome);
        }
        fleet.run_for(Duration::from_millis(500))?;
    }
    Err(format!("{operation} was never acknowledged").into())
}

/// A delegation by `holder`, on `agent-42`'s certificate.
fn delegate_as(
    holder: &Holder,
    operation: u8,
    segment: &str,
    scopes: &[&str],
) -> Result<Operation, Box<dyn Error>> {
    delegate(&holder.token, operation, segment, scopes)
}

/// `holder`'s database: created with its own token, which proves the token works first.
fn holder(
    fleet: &mut Fleet,
    operator: &Operator,
    name: &'static str,
    token: BearerToken,
    database: &str,
) -> Result<Holder, Box<dyn Error>> {
    let created = until_acknowledged(
        fleet,
        operator,
        &Operation::CreateAs {
            certificate: agent_42()?,
            token: token.clone(),
            database: database.parse()?,
        },
    )?;
    assert_eq!(created.ack(), Some(&Ack::Created), "{name}: {created:?}");
    Ok(Holder {
        name,
        token,
        database: database.to_owned(),
    })
}

/// A read of `holder`'s database on its primary, a data node, with its token.
fn query_as(holder: &Holder) -> Result<Operation, Box<dyn Error>> {
    Ok(Operation::QueryAs {
        certificate: agent_42()?,
        token: holder.token.clone(),
        database: holder.database.parse()?,
    })
}

/// Sets up section 14's lineage, moving from issuer key A to B by `change` after `c7`.
fn lineage(
    fleet: &mut Fleet,
    operator: &Operator,
    change: Change,
) -> Result<Lineage, Box<dyn Error>> {
    let root_token = root(fleet)?;
    let root = holder(fleet, operator, "agent-42", root_token, "agent-42-notes")?;
    // 1. Under A, with an explicit grant.
    let step_1 = delegate_as(&root, 1, "c7", &C7_SCOPES)?;
    let (c7_a, c7_id, a_key) = delegated(&until_acknowledged(fleet, operator, &step_1)?)?;
    // 2. To B.
    fleet.rotate_issuer(CONTROL)?;
    if change == Change::Restart {
        fleet.crash(CONTROL)?;
        fleet.run_for(Duration::from_secs(1))?;
        fleet.restart(CONTROL)?;
        fleet.await_ring(Duration::from_secs(30))?;
    }
    let c7_a = holder(fleet, operator, "c7 (A)", c7_a, "agent-42.c7-a")?;
    // 3. `r1` from c7's A copy, signed by B once the node holds it.
    let step_3 = delegate_as(&c7_a, 2, "r1", &[])?;
    let (r1, r1_id, r1_key) = loop {
        let (token, id, key) = delegated(&until_acknowledged(fleet, operator, &step_3)?)?;
        if key != a_key {
            break (token, id, key);
        }
        fleet.run_for(Duration::from_millis(500))?;
    };
    // 4. `agent-42` resends step 1: the same delegation, signed by B.
    let (c7_b, c7_b_id, b_key) = delegated(&until_acknowledged(fleet, operator, &step_1)?)?;
    assert_eq!(c7_b_id, c7_id, "a resend names the same delegation");
    assert_eq!(b_key, r1_key, "both are B's");
    let c7_b = holder(fleet, operator, "c7 (B)", c7_b, "agent-42.c7-b")?;
    // 5. `r2` from the B copy.
    let (r2, r2_id, _) = delegated(&until_acknowledged(
        fleet,
        operator,
        &delegate_as(&c7_b, 3, "r2", &[])?,
    )?)?;
    // The chains: r1's names c7's A copy, r2's its B copy.
    let state = fleet.control_state().ok_or("the control node is down")?;
    let chain_key = |id: &DelegationId| {
        state
            .delegations()
            .find(|(recorded, _)| *recorded == id)
            .and_then(|(_, record)| record.claims.chain.last().map(|link| link.kid.to_string()))
    };
    assert_eq!(chain_key(&r1_id).as_deref(), Some(a_key.as_str()));
    assert_eq!(chain_key(&r2_id).as_deref(), Some(b_key.as_str()));
    let r1 = holder(fleet, operator, "r1", r1, "agent-42.c7.r1-notes")?;
    let r2 = holder(fleet, operator, "r2", r2, "agent-42.c7.r2-notes")?;
    Ok(Lineage {
        root,
        c7_a,
        c7_b,
        r1,
        r2,
        c7_id,
        a_key,
    })
}

/// The longest a revocation may take to reach a data node in these fleets: one heartbeat
/// interval (200 ms in the harness), plus the time a heartbeat and a query take to travel.
const ONE_HEARTBEAT: Duration = Duration::from_millis(300);

/// Revokes as `case` says, and returns which holders must end and which survive.
fn revoke_for(
    fleet: &mut Fleet,
    operator: &Operator,
    lineage: &Lineage,
    case: Case,
) -> Result<(Vec<Holder>, Vec<Holder>), Box<dyn Error>> {
    let until = fleet.host_time().saturating_add(Millis::new(4 * 3_600_000));
    let by_operator = |revoked: Revoked| Operation::Revoke { revoked, until };
    let by_root = |token_id: Option<fleetring_control::TokenId>,
                   principal: Option<&str>|
     -> Result<Operation, Box<dyn Error>> {
        Ok(Operation::RevokeAs {
            certificate: agent_42()?,
            token: lineage.root.token.clone(),
            token_id,
            principal: principal.map(str::parse).transpose()?,
        })
    };
    let c7_token_id: fleetring_control::TokenId = lineage.c7_id.clone().into();
    let all_below = vec![
        lineage.c7_a.clone(),
        lineage.c7_b.clone(),
        lineage.r1.clone(),
        lineage.r2.clone(),
    ];
    let (operation, ended, survive) = match case {
        Case::KeyA => (
            by_operator(Revoked::Key(lineage.a_key.parse()?)),
            vec![lineage.c7_a.clone(), lineage.r1.clone()],
            vec![
                lineage.c7_b.clone(),
                lineage.r2.clone(),
                lineage.root.clone(),
            ],
        ),
        Case::C7ByOperator => (
            by_operator(Revoked::Token(c7_token_id)),
            all_below,
            vec![lineage.root.clone()],
        ),
        Case::C7ByAncestor => (
            by_root(Some(c7_token_id), None)?,
            all_below,
            vec![lineage.root.clone()],
        ),
        Case::RootToken => {
            let mut ended = all_below;
            ended.push(lineage.root.clone());
            (
                by_operator(Revoked::Token("root-42".parse()?)),
                ended,
                Vec::new(),
            )
        }
        Case::C7Principal => (
            by_root(None, Some("agent-42.c7"))?,
            all_below,
            vec![lineage.root.clone()],
        ),
        Case::RemoveRoot => {
            let mut ended = all_below;
            ended.push(lineage.root.clone());
            (
                by_root(Some("root-42".parse()?), Some("agent-42"))?,
                ended,
                Vec::new(),
            )
        }
        Case::ReusedSegment => {
            // `c7` by ID first; then `c7′` and `r1′` under the same names.
            let revoked = once(fleet, operator, by_root(Some(c7_token_id), None)?)?;
            assert_eq!(revoked.ack(), Some(&Ack::Revoked), "{revoked:?}");
            let (c7_again, _, _) = delegated(&until_acknowledged(
                fleet,
                operator,
                &delegate_as(&lineage.root, 4, "c7", &C7_SCOPES)?,
            )?)?;
            let c7_again = holder(fleet, operator, "c7′", c7_again, "agent-42.c7-again")?;
            let (r1_again, _, _) = delegated(&until_acknowledged(
                fleet,
                operator,
                &delegate_as(&c7_again, 5, "r1", &[])?,
            )?)?;
            let r1_again = holder(fleet, operator, "r1′", r1_again, "agent-42.c7.r1-again")?;
            let mut ended = all_below;
            ended.extend([c7_again, r1_again]);
            (
                by_root(None, Some("agent-42.c7"))?,
                ended,
                vec![lineage.root.clone()],
            )
        }
    };
    let outcome = once(fleet, operator, operation)?;
    assert_eq!(outcome.ack(), Some(&Ack::Revoked), "{case:?}: {outcome:?}");
    Ok((ended, survive))
}

/// Checks that every `ended` holder is refused on a data node within [`ONE_HEARTBEAT`] and
/// can't delegate, and that every survivor still reads.
fn check_case(
    fleet: &mut Fleet,
    operator: &Operator,
    case: Case,
    ended: &[Holder],
    survive: &[Holder],
) -> TestResult {
    fleet.run_for(ONE_HEARTBEAT)?;
    for holder in ended {
        let read = once(fleet, operator, query_as(holder)?)?;
        assert!(
            matches!(&read, Outcome::Rejected { code: Code::Unauthenticated, message } if message.contains("revoked")),
            "{case:?}: {} still reads: {read:?}",
            holder.name
        );
        // I4: an ended token delegates nothing.
        let delegation = once(fleet, operator, delegate_as(holder, 9, "late", &[])?)?;
        assert!(
            delegation.ack().is_none(),
            "{case:?}: {} delegated after its revocation: {delegation:?}",
            holder.name
        );
    }
    for holder in survive {
        let read = once(fleet, operator, query_as(holder)?)?;
        assert_eq!(
            read.ack(),
            Some(&Ack::Queried),
            "{case:?}: {} must survive: {read:?}",
            holder.name
        );
    }
    Ok(())
}

/// One release-blocker case on a fresh fleet: section 14's setup with the move to B made by
/// `change`, then the revocation, then who ends and who survives. Case 5 also mints a new root
/// token more than 30 seconds after `agent-42` removed itself, which works.
fn subtree_case(seed: u64, change: Change, case: Case) -> TestResult {
    let mut fleet = fleet(seed, 3)?;
    fleet.await_ring(Duration::from_secs(30))?;
    let operator = Operator::new();
    fleet.client(OPERATOR, operator.clone());
    let lineage = lineage(&mut fleet, &operator, change)?;
    let (ended, survive) = revoke_for(&mut fleet, &operator, &lineage, case)?;
    check_case(&mut fleet, &operator, case, &ended, &survive)?;
    if case == Case::RemoveRoot {
        fleet.run_for(Duration::from_secs(31))?;
        let renewed = fleet.agent_token("agent-42", "root-42-new", &ROOT_SCOPES, 3 * HOUR)?;
        let renewed = Holder {
            name: "a new agent-42 token",
            token: renewed,
            database: lineage.root.database.clone(),
        };
        let read = once(&mut fleet, &operator, query_as(&renewed)?)?;
        assert_eq!(read.ack(), Some(&Ack::Queried), "{read:?}");
    }
    fleet.finish()?;
    Ok(())
}

/// The release blocker (I5): subtree revocation across issuer rotation and leader changes.
/// Every case of section 14 runs on a fresh fleet, once with a reload of new issuer files and
/// once with a control node restart on them.
fn subtree_revocation(seed: u64) -> TestResult {
    for change in [Change::Reload, Change::Restart] {
        for case in CASES {
            subtree_case(seed, change, case)?;
        }
    }
    Ok(())
}

#[test]
fn subtree_revocation_across_issuers() {
    seeds::run(BINARY, &seeds::FIXED, subtree_revocation);
}
