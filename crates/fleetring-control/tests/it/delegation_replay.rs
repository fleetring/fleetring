//! Delegations replay: a model-based generator builds each command against the current state,
//! so most delegations, resends, and revocations (by token and principal, with `fleet` and
//! under the ancestor rule) are meant to succeed, and the log replays to the identical state and
//! outcomes, from scratch, from snapshots, and through serialization (ADR-0017; ADR-0047, points
//! 5.10, 6.2, 6.3, and 11).

use std::collections::BTreeMap;

use fleetring_control::delegation::{
    AgentName, AncestorTarget, CallerToken, Limit, MAX_ANCESTOR_ENTRIES_PER_LINEAGE, Resent,
    RevocationLimit,
};
use fleetring_control::{Action, Applied, Command, Outcome, Rejection, Revoked, State, Timestamp};

use crate::common::delegation::{
    Ask, HOUR, ISSUER, RECORDER, copy, delegating, fleet, root_token, scopes,
};
use crate::common::{at, id};
use crate::replay::{
    check_replay_from_scratch, check_replay_from_snapshots, check_replay_through_serialization,
};

/// A deterministic stream of 64-bit draws (splitmix64).
struct Draws(u64);

impl Draws {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A draw below `bound`.
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn index(&mut self, len: usize) -> usize {
        usize::try_from(self.below(u64::try_from(len).expect("small"))).expect("small")
    }
}

/// A control log being written by the model: every command applied and kept with its outcome.
struct Model {
    state: State,
    log: Vec<Command>,
    outcomes: Vec<Outcome>,
    clock: u64,
    draws: Draws,
    roots: Vec<CallerToken>,
    /// Every delegation the log accepted as new, to resend.
    sent: Vec<Action>,
    next: u32,
    steps: usize,
}

impl Model {
    fn new(seed: u64) -> Self {
        let state = fleet();
        let mut log = Vec::new();
        let mut outcomes = Vec::new();
        // The joins `fleet` applied, as the log holds them.
        let mut replayed = State::new(crate::common::config());
        for node in ["n0", "n1", "n2"] {
            let member = fleetring_control::Member::new(id(node))
                .with_software(crate::common::current_software());
            let command = Command::new(at(0), Action::Join { member });
            outcomes.push(fleetring_control::apply(&mut replayed, command.clone()));
            log.push(command);
        }
        assert_eq!(replayed, state);
        Self {
            state,
            log,
            outcomes,
            clock: 1_000,
            draws: Draws(seed),
            roots: vec![
                root_token("agent-1", "root-1"),
                root_token("agent-2", "root-2"),
            ],
            sent: Vec::new(),
            next: 0,
            steps: 0,
        }
    }

    fn run(&mut self, command: Command) -> Outcome {
        let outcome = fleetring_control::apply(&mut self.state, command.clone());
        self.log.push(command);
        self.outcomes.push(outcome.clone());
        outcome
    }

    fn stamped(&mut self, action: Action) -> Outcome {
        self.run(Command::stamped(at(self.clock), id(RECORDER), action))
    }

    /// The live delegations' copies, in ID order, with their depth.
    fn live(&self) -> Vec<(usize, CallerToken)> {
        self.state
            .delegations()
            .filter(|(_, record)| record.is_live(at(self.clock), self.state.now()))
            .map(|(_, record)| (record.claims.chain.len(), copy(&record.claims, ISSUER)))
            .collect()
    }

    fn step(&mut self) {
        self.steps += 1;
        self.clock += self.draws.below(400);
        match self.draws.below(1_000) {
            0..=719 => self.delegate(),
            720..=869 => self.resend(),
            870..=899 => self.revoke(),
            900..=929 => self.revoke_as_ancestor(),
            930..=939 => self.revoke_principal(),
            940..=993 => {}
            994..=996 => self.clock += 61_000,
            // Past every `kept_until`, now and then, once the first lineage has had time to
            // reach its 256.
            997 if self.steps > 1_200 => self.clock += 2 * HOUR,
            _ => {
                let _ticked = self.stamped(Action::Tick);
            }
        }
    }

    fn caller(&mut self) -> CallerToken {
        let live = self.live();
        let past_the_depth = self.draws.below(16) == 0;
        match self.draws.below(8) {
            // Mostly the first lineage, so its rates and its 256 are reached.
            0..=1 => self.roots[0].clone(),
            2 => self.roots[1].clone(),
            3..=5 if !live.is_empty() => live[self.draws.index(live.len())].1.clone(),
            // The deepest, so chains reach the depth limit, and now and then pass it.
            6..=7 if !live.is_empty() => live
                .iter()
                .filter(|(depth, _)| *depth < 8 || past_the_depth)
                .max_by_key(|(depth, _)| *depth)
                .map_or_else(|| self.roots[0].clone(), |(_, caller)| caller.clone()),
            _ => self.roots[0].clone(),
        }
    }

    fn delegate(&mut self) {
        let caller = self.caller();
        self.next += 1;
        let n = self.next;
        let segment = format!("s{}", n % 100);
        let child = format!("{}.{segment}", caller.claims.principal);
        let lifetime = [2 * 60_000, HOUR, HOUR][self.draws.index(3)];
        let start = self.clock - self.clock % 1_000;
        let expires_at = (start + lifetime).min(caller.claims.expires_at.as_millis());
        let mut ask = Ask::new(0, &segment, delegating(&child), expires_at);
        let mut bytes = [0xA0; 16];
        bytes[..4].copy_from_slice(&n.to_le_bytes());
        ask.operation = fleetring_control::fork::OperationId::from_bytes(bytes);
        ask.id = crate::common::delegation::drawn(n);
        let mut caller = caller;
        // One in ten is invalid on purpose.
        let mut unstamped = false;
        match self.draws.below(40) {
            0 => ask.expires_at = at(expires_at + 1),
            1 => ask.scopes = scopes(&[&format!("admin:{child}-*"), "read:pub-*"]),
            2 => caller.claims.not_before = at(caller.claims.not_before.as_millis() + 1_000),
            3 => unstamped = true,
            _ => {}
        }
        let action = ask.action(&caller);
        let outcome = if unstamped {
            self.run(Command::new(at(self.clock), action.clone()))
        } else {
            self.stamped(action.clone())
        };
        if matches!(
            outcome,
            Ok(Applied::DelegationRecorded {
                resent: Resent::No,
                ..
            })
        ) {
            self.sent.push(action);
        }
    }

    fn resend(&mut self) {
        if self.sent.is_empty() {
            return;
        }
        // Mostly recent ones, which are still live.
        let len = self.sent.len();
        let back = self.draws.index(len.min(40));
        let action = self.sent[len - 1 - back].clone();
        let _resent = self.stamped(action);
    }

    fn revoke(&mut self) {
        let live = self.live();
        if live.is_empty() {
            return;
        }
        let target = live[self.draws.index(live.len())].1.claims.id.clone();
        let _revoked = self.stamped(Action::RevokeToken {
            revoked: Revoked::Token(target),
            until: Timestamp::from_millis(self.clock + 30 * 24 * HOUR),
        });
    }

    /// A revocation under the ancestor rule of a live delegation, by itself, its root, or a live
    /// ancestor, by token or principal; now and then one the rule refuses.
    fn revoke_as_ancestor(&mut self) {
        let live = self.live();
        if live.is_empty() {
            return;
        }
        let target = live[self.draws.index(live.len())].1.clone();
        let name = target.claims.principal.clone();
        let agent = |name: &fleetring_control::PrincipalName| -> AgentName {
            name.as_str().parse().expect("an agent name")
        };
        let root = self
            .roots
            .iter()
            .find(|root| name.is_at_or_below(&root.claims.principal))
            .expect("every delegation is below a root")
            .clone();
        let ancestors: Vec<CallerToken> = live
            .iter()
            .map(|(_, caller)| caller.clone())
            .filter(|caller| {
                caller.claims.principal != name && name.is_at_or_below(&caller.claims.principal)
            })
            .collect();
        let by_token = AncestorTarget::Token(target.claims.id.clone());
        let by_principal = AncestorTarget::Principal(agent(&name));
        // Every other self or root revocation by principal names the token too, as `agent
        // remove` does. The principal covers it, so the state is the same, and so is every draw.
        let by_principal_or_both = if self.log.len().is_multiple_of(2) {
            AncestorTarget::PrincipalAndToken {
                principal: agent(&name),
                token: target.claims.id.clone(),
            }
        } else {
            by_principal.clone()
        };
        let (caller, target) = match self.draws.below(6) {
            0 => (target, by_token),
            1 => (target, by_principal_or_both),
            2 => (root, by_token),
            3 => (root, by_principal_or_both),
            4 if !ancestors.is_empty() => (
                ancestors[self.draws.index(ancestors.len())].clone(),
                by_principal,
            ),
            // Refused: a delegation can't revoke its parent, nor a root another lineage.
            _ => {
                let parent = name
                    .as_str()
                    .rsplit_once('.')
                    .map(|(parent, _)| parent.to_owned())
                    .expect("a delegated name");
                let other = self
                    .roots
                    .iter()
                    .find(|other| other.claims.id != root.claims.id);
                match other {
                    Some(other) if self.draws.below(2) == 0 => (other.clone(), by_principal),
                    _ => (
                        target,
                        AncestorTarget::Principal(parent.parse().expect("an agent name")),
                    ),
                }
            }
        };
        let _revoked = self.stamped(Action::RevokeAsAncestor { target, caller });
    }

    /// An operator's principal revocation of a live delegation.
    fn revoke_principal(&mut self) {
        let live = self.live();
        if live.is_empty() {
            return;
        }
        let principal = live[self.draws.index(live.len())]
            .1
            .claims
            .principal
            .clone();
        let _revoked = self.stamped(Action::RevokePrincipal { principal });
    }

    fn check(&self) {
        check_replay_from_scratch(&self.log, &self.outcomes, &self.state);
        check_replay_from_snapshots(&self.log, &self.outcomes, &self.state);
        check_replay_through_serialization(&self.log, &self.outcomes, &self.state);
    }
}

/// Counts the log's outcomes by kind, and revocations that pruned a record.
fn counts(model: &Model) -> BTreeMap<&'static str, usize> {
    let mut counts = BTreeMap::new();
    let mut state = State::new(crate::common::config());
    for (command, outcome) in model.log.iter().zip(&model.outcomes) {
        let records = state.delegations().len();
        let replaces = replaces_an_entry(&state, &command.action);
        let _ = fleetring_control::apply(&mut state, command.clone());
        assert_budgets_bounded(&state);
        let kind = match (&command.action, outcome) {
            (
                _,
                Ok(Applied::DelegationRecorded {
                    resent: Resent::No, ..
                }),
            ) => "recorded",
            (
                _,
                Ok(Applied::DelegationRecorded {
                    resent: Resent::Yes,
                    ..
                }),
            ) => "resent",
            (Action::RevokeToken { .. }, Ok(Applied::TokenRevoked { .. }))
                if state.delegations().len() < records =>
            {
                "pruning revocation"
            }
            (Action::RevokeAsAncestor { .. }, Ok(Applied::TokenRevoked { .. })) => "ancestor token",
            (Action::RevokeAsAncestor { .. }, Ok(Applied::PrincipalRevoked { .. })) => {
                "ancestor principal"
            }
            (Action::RevokeAsAncestor { .. }, Ok(Applied::PrincipalAndTokenRevoked { .. })) => {
                "ancestor pair"
            }
            (Action::RevokePrincipal { .. }, Ok(Applied::PrincipalRevoked { .. })) => {
                "fleet principal"
            }
            (
                _,
                Err(Rejection::RevocationLimit {
                    limit: RevocationLimit::Entries { .. },
                }),
            ) if replaces => "cap refused a replacing revocation",
            (_, Err(Rejection::RevocationLimit { .. })) => "revocation limit",
            (_, Ok(_)) => "other",
            (_, Err(Rejection::DelegationLimit { limit })) => match limit {
                Limit::Live => "live limit",
                Limit::CallerRate => "caller rate",
                Limit::LineageRate => "lineage rate",
                Limit::Depth => "depth",
                Limit::Backstop => "backstop",
                _ => "other limit",
            },
            (_, Err(_)) => "refused",
        };
        *counts.entry(kind).or_default() += 1;
    }
    counts
}

/// Whether `action` is a principal revocation under the ancestor rule that covers a live
/// ancestor-rule entry below it in the caller's own budget, which it would replace.
fn replaces_an_entry(state: &State, action: &Action) -> bool {
    let (q, caller) = match action {
        Action::RevokeAsAncestor {
            target:
                AncestorTarget::Principal(q) | AncestorTarget::PrincipalAndToken { principal: q, .. },
            caller,
        } => (q, &caller.claims.principal),
        _ => return false,
    };
    let caller_is_root = !caller.as_str().contains('.');
    let q = q.as_principal();
    state.revocations().iter().any(|(revoked, entry)| {
        entry.until > state.now()
            && matches!(
                &entry.added,
                fleetring_control::Added::Ancestor { root, by } if (by == root) == caller_is_root
            )
            && match revoked {
                Revoked::Principal { principal, .. } => {
                    principal != q && principal.is_at_or_below(q)
                }
                Revoked::Token(_) => entry
                    .of
                    .as_ref()
                    .is_some_and(|of| of.as_principal().is_at_or_below(q)),
                _ => false,
            }
    })
}

/// I11 as amended 2026-11-13: in every lineage, each budget, the root's own entries and its
/// delegated agents' between them, holds at most [`MAX_ANCESTOR_ENTRIES_PER_LINEAGE`] live
/// entries the cap counts, so the lineage at most twice that plus the exempt entries.
fn assert_budgets_bounded(state: &State) {
    let mut held: BTreeMap<(String, bool), usize> = BTreeMap::new();
    for (revoked, entry) in state.revocations().iter() {
        let fleetring_control::Added::Ancestor { root, by } = &entry.added else {
            continue;
        };
        let exempt = match revoked {
            Revoked::Principal { principal, .. } => principal == root.as_principal(),
            Revoked::Token(_) => entry.of.is_none(),
            _ => false,
        };
        if entry.until > state.now() && !exempt {
            *held.entry((root.to_string(), by == root)).or_default() += 1;
        }
    }
    for ((root, roots_own), count) in held {
        assert!(
            count <= MAX_ANCESTOR_ENTRIES_PER_LINEAGE,
            "{root}'s {} budget holds {count}",
            if roots_own { "own" } else { "descendants'" }
        );
    }
}

fn generate(seed: u64, steps: usize) -> Model {
    let mut model = Model::new(seed);
    for _ in 0..steps {
        model.step();
    }
    model
}

/// Checks the log reached what it must: at least 100 new delegations, 20 resends answered, and
/// 20 revocations that pruned; and, for [`Limits::Each`], a refusal of every limit but the
/// backstop. The backstop never fires.
fn assert_exercised(counts: &BTreeMap<&'static str, usize>, limits: Limits) {
    let mut wanted = vec![
        ("recorded", 100),
        ("resent", 20),
        ("pruning revocation", 20),
        ("ancestor token", 10),
        ("ancestor principal", 10),
        ("ancestor pair", 5),
        ("fleet principal", 5),
        ("refused", 1),
    ];
    if limits == Limits::Each {
        wanted.extend([
            ("live limit", 1),
            ("caller rate", 1),
            ("lineage rate", 1),
            ("depth", 1),
        ]);
    }
    for (kind, least) in wanted {
        assert!(
            counts.get(kind).copied().unwrap_or(0) >= least,
            "fewer than {least} '{kind}' in {counts:?}"
        );
    }
    assert_eq!(
        counts.get("backstop"),
        None,
        "the backstop fired: {counts:?}"
    );
    assert_eq!(
        counts.get("cap refused a replacing revocation"),
        None,
        "{counts:?}"
    );
}

/// Whether the counts must show a refusal of every limit, or only the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Limits {
    Each,
    Some,
}

#[test]
fn a_generated_delegation_log_replays_identically_and_exercises_every_limit() {
    let model = generate(0xDE1E_0001, 2_500);
    assert_exercised(&counts(&model), Limits::Each);
    model.check();
}

/// The same over more seeds and longer logs.
#[test]
#[ignore = "heavy: many seeds of long logs; run in the release statistics job"]
fn generated_delegation_logs_replay_identically_over_many_seeds() {
    let mut together = BTreeMap::new();
    for seed in 0..8 {
        let model = generate(0xDE1E_1000 + seed, 6_000);
        let counts = counts(&model);
        assert_exercised(&counts, Limits::Some);
        for (kind, count) in counts {
            *together.entry(kind).or_insert(0) += count;
        }
        model.check();
    }
    assert_exercised(&together, Limits::Each);
}
