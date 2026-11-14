//! The pure parts of `Control.DelegateToken` (ADR-0047, point 5): the child's name, its scopes,
//! its times, and the claims `apply` will record for it, each resolved from the request, the
//! caller's verified token, and the control node's time, with the status a refusal answers.
//!
//! Nothing here reads a clock, draws randomness, or touches control state: the handler in
//! [`service`](super::service) passes the time in and proposes what this resolves. Every
//! refusal's message names what was asked and why it can't be, and never quotes a token.

use fleetring_control::delegation::{
    AgentName, CallerToken, ChainLink, DelegationId, MAX_DELEGATED_LIFETIME, MAX_NOT_BEFORE_LEAD,
    ParseAgentNameError, TokenClaims, TokenKind, tier,
};
use fleetring_control::matcher::DatabaseMatcher;
use fleetring_control::scope::{Action, GrantStepError, MAX_SCOPES, Scope, Scopes};
use fleetring_control::{Millis, Timestamp};
use tonic::Status;

/// A delegated token's lifetime when the request names none: 2 hours, cut short by the caller's
/// token's expiry (ADR-0047, point 5.4).
pub(crate) const DEFAULT_LIFETIME: Millis = Millis::new(2 * 60 * 60 * 1_000);

/// The shortest lifetime a request may name (ADR-0047, point 5.4).
pub(crate) const MIN_LIFETIME: Millis = Millis::new(60 * 1_000);

/// How far in the past a request's `not_before` may be (ADR-0047, point 5.4): the leeway every
/// node gives a token's `nbf`.
pub(crate) const MAX_NOT_BEFORE_LAG: Millis = Millis::new(30 * 1_000);

/// How far control time may lead this control node's clock before the node stops delegating
/// (ADR-0047, point 5.4): further, and every node would refuse the token it signed as not yet
/// valid.
pub(crate) const MAX_CONTROL_LEAD: Millis = Millis::new(30 * 1_000);

/// One second, the grain of every recorded time (the plan's principle P4).
const SECOND: u64 = 1_000;

/// The time a delegation is resolved at: the later of this control node's `clock` and the
/// control plane's time, `control`, the last applied command's (ADR-0047, point 5.4).
///
/// # Errors
///
/// `UNAVAILABLE` when `control` leads `clock` by more than [`MAX_CONTROL_LEAD`]: a leader whose
/// clock ran fast was replaced, and a token resolved against control time would not yet be
/// valid on any node. A retry succeeds once this node's clock catches up.
pub(crate) fn resolve_now(clock: Timestamp, control: Timestamp) -> Result<Timestamp, Status> {
    let lead = control.as_millis().saturating_sub(clock.as_millis());
    if lead > MAX_CONTROL_LEAD.get() {
        return Err(Status::unavailable(format!(
            "control time leads this control node's clock by {} s; retry once its clock catches \
             up",
            lead / SECOND
        )));
    }
    Ok(clock.max(control))
}

/// The child's name: the caller's followed by `.<segment>` (ADR-0047, point 5.2).
///
/// # Errors
///
/// `RESOURCE_EXHAUSTED` for a child more than 8 levels below its root, the depth limit of point
/// 5.10; `INVALID_ARGUMENT` for a segment that isn't 1 to 32 of `a-z`, `0-9`, and `_`, or a
/// child's name longer than 64 bytes.
pub(crate) fn child_name(caller: &AgentName, segment: &str) -> Result<AgentName, Status> {
    caller.child(segment).map_err(|error| match error {
        ParseAgentNameError::TooDeep { .. } => Status::resource_exhausted(format!(
            "too many delegations: at most 8 levels below the root, and {caller}.{segment} would \
             be {}",
            caller.depth() + 1
        )),
        ParseAgentNameError::InvalidSegment => Status::invalid_argument(format!(
            "invalid segment {segment:?}: a segment is 1 to 32 of a-z, 0-9, and '_'"
        )),
        ParseAgentNameError::Principal(_)
        | ParseAgentNameError::NotAgent { .. }
        | ParseAgentNameError::Segment { .. } => Status::invalid_argument(format!(
            "the child's name {caller}.{segment} isn't an agent name: at most 64 bytes, and each \
             segment 1 to 32 of a-z, 0-9, and '_'"
        )),
        // The enum may grow; whatever else a child's name breaks is the request's fault.
        _ => Status::invalid_argument(format!("invalid child name: {error}")),
    })
}

/// The scopes a request asks for, parsed: `None` for none, which asks for the defaults.
///
/// # Errors
///
/// `INVALID_ARGUMENT` for a scope that doesn't parse, or more than [`MAX_SCOPES`].
pub(crate) fn parse_scopes(texts: &[String]) -> Result<Option<Scopes>, Status> {
    if texts.is_empty() {
        return Ok(None);
    }
    if texts.len() > MAX_SCOPES {
        return Err(Status::invalid_argument(format!(
            "a delegation asks for at most {MAX_SCOPES} scopes, not {}",
            texts.len()
        )));
    }
    texts
        .iter()
        .map(|text| {
            text.parse::<Scope>().map_err(|error| {
                Status::invalid_argument(format!("invalid scope {text:?}: {error}"))
            })
        })
        .collect::<Result<Scopes, Status>>()
        .map(Some)
}

/// The scopes a child gets by default (ADR-0047, point 5.3): `admin` and `stream` on its own
/// prefix, `<child>-*`, before the grant step narrows them to what the caller holds.
///
/// # Panics
///
/// Never: an agent name followed by `-*` is a valid prefix pattern.
pub(crate) fn default_request(child: &AgentName) -> Scopes {
    let prefix: DatabaseMatcher = format!("{child}-*")
        .parse()
        .expect("an agent name followed by '-*' is a valid prefix pattern");
    Scopes::new([
        Scope::new(Action::Admin, prefix.clone()),
        Scope::new(Action::Stream, prefix),
    ])
}

/// The child's scopes (ADR-0047, point 5.3), from the caller's effective scopes `held`:
///
/// - **explicit** ones, `asked`, pass the strict grant step: each is wholly within `held` and
///   its `grant` patterns, or the request is refused, naming it;
/// - **the defaults** ([`default_request`]) pass the narrowing grant step, so a read-only
///   parent's child gets `read` and `stream`; nothing left is refused;
/// - either way, the result must be in the agent tier for the child ([`tier::check`]).
///
/// The result is in canonical form, as `apply` requires.
///
/// # Errors
///
/// `PERMISSION_DENIED` for a caller with no `grant`, an explicit scope the grant step would
/// narrow or drop, or defaults it leaves empty; `INVALID_ARGUMENT` for scopes outside the agent
/// tier, naming the first, or too many to narrow.
pub(crate) fn resolve_scopes(
    held: &Scopes,
    child: &AgentName,
    asked: Option<&Scopes>,
) -> Result<Scopes, Status> {
    let no_grant = || Status::permission_denied(format!("your token holds no grant over {child}-"));
    let scopes = match asked {
        Some(asked) => held.grant_step_strict(asked).map_err(|error| match error {
            GrantStepError::NoGrant => {
                Status::permission_denied("your token holds no grant scope, so it can't delegate")
            }
            GrantStepError::NotCovered { scope } => Status::permission_denied(format!(
                "your token doesn't cover {scope} with both its scopes and its grant"
            )),
            GrantStepError::TooMany { limit } => Status::invalid_argument(format!(
                "a set of scopes to delegate holds at most {limit}"
            )),
            GrantStepError::Empty => Status::invalid_argument("no scopes were asked for"),
            // The enum may grow; a refusal it adds is still the caller's lack of cover.
            other => Status::permission_denied(other.to_string()),
        })?,
        None => held
            .grant_step(&default_request(child))
            .map_err(|error| match error {
                GrantStepError::NoGrant | GrantStepError::Empty => no_grant(),
                GrantStepError::TooMany { limit } => Status::invalid_argument(format!(
                    "a set of scopes to delegate holds at most {limit}"
                )),
                GrantStepError::NotCovered { .. } => no_grant(),
                other => Status::permission_denied(other.to_string()),
            })?,
    };
    tier::check(child, &scopes)
        .map_err(|violation| Status::invalid_argument(format!("can't delegate: {violation}")))?;
    Ok(scopes)
}

/// A delegation's resolved times, in whole seconds (the plan's principle P4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Times {
    /// The child's `nbf`, before `apply` raises it to the command's effective time.
    pub(crate) not_before: Timestamp,
    /// The child's `exp`.
    pub(crate) expires_at: Timestamp,
}

/// The child's `nbf` and `exp` (ADR-0047, point 5.4), resolved at `now` from the request's
/// `not_before` and `lifetime`, for a caller whose token expires at `parent_expires`:
///
/// - `not_before` defaults to `now`; an explicit one may be at most 30 seconds in the past, at
///   most 24 hours ahead, and must be before the caller's token expires;
/// - the default lifetime is the shorter of [`DEFAULT_LIFETIME`] and what remains of the
///   caller's token from `not_before`, capped silently; an explicit one is refused, never
///   capped, when it is longer than what remains, longer than 24 hours, or shorter than
///   [`MIN_LIFETIME`];
/// - both times are rounded down to the second, so `exp` is never after the caller's.
///
/// # Errors
///
/// `INVALID_ARGUMENT` for a `not_before` or lifetime the rules above refuse;
/// `FAILED_PRECONDITION` when the caller's token expires before the child's could start.
pub(crate) fn resolve_times(
    now: Timestamp,
    parent_expires: Timestamp,
    not_before: Option<Timestamp>,
    lifetime: Option<Millis>,
) -> Result<Times, Status> {
    let start = match not_before {
        None => now,
        Some(at) => {
            if at.as_millis().saturating_add(MAX_NOT_BEFORE_LAG.get()) < now.as_millis() {
                return Err(Status::invalid_argument(
                    "not_before may be at most 30 s in the past",
                ));
            }
            if at > now.saturating_add(MAX_NOT_BEFORE_LEAD) {
                return Err(Status::invalid_argument(
                    "not_before may be at most 24 h ahead",
                ));
            }
            if at >= parent_expires {
                return Err(Status::invalid_argument(
                    "not_before must be before your token expires",
                ));
            }
            at
        }
    };
    let start = floor_to_second(start);
    let remaining = parent_expires.as_millis().saturating_sub(start.as_millis());
    let lifetime = match lifetime {
        None => DEFAULT_LIFETIME.get().min(remaining),
        Some(asked) => {
            let asked = asked.get();
            if asked > MAX_DELEGATED_LIFETIME.get() {
                return Err(Status::invalid_argument(
                    "a delegated token lasts at most 24 h",
                ));
            }
            if asked < MIN_LIFETIME.get() {
                return Err(Status::invalid_argument(
                    "a delegated token lasts at least 60 s",
                ));
            }
            if asked > remaining {
                return Err(Status::invalid_argument(format!(
                    "a lifetime of {} s would outlast your token, which expires {} s after \
                     not_before; ask for at most that",
                    asked / SECOND,
                    remaining / SECOND
                )));
            }
            asked
        }
    };
    let expires_at = floor_to_second(Timestamp::from_millis(
        start.as_millis().saturating_add(lifetime),
    ));
    if expires_at <= start {
        return Err(Status::failed_precondition(
            "your token expires before a child's token could start",
        ));
    }
    Ok(Times {
        not_before: start,
        expires_at,
    })
}

/// Whether a delegation's default lifetime was cut short by the caller's token (ADR-0047, point
/// 5.4): it asked for no lifetime, and its token, `child_expires`, ends with the caller's,
/// `parent_expires`.
///
/// It is read from what was recorded rather than from the request's resolution, so a resend
/// answers the same as the request that recorded the delegation.
pub(crate) fn capped(
    lifetime: Option<Millis>,
    child_expires: Timestamp,
    parent_expires: Timestamp,
) -> bool {
    lifetime.is_none() && child_expires >= parent_expires
}

/// The claims `apply` records for a child of `caller` named `child`, with `id`, `scopes`, and
/// `times`, by a command whose effective time is `effective` (ADR-0047, points 2.2, 2.3, and
/// 5.6): `iat` is `effective` rounded down to the second and `nbf` is raised to it; the chain is
/// the caller's followed by the copy it presented; the owner is the caller's.
///
/// The handler signs these once before it records anything, to refuse a token too large for
/// any node (the plan's principle P13); the recorded claims differ from them only in `iat`, and
/// so possibly `nbf`, by the time between.
pub(crate) fn child_claims(
    caller: &CallerToken,
    child: &AgentName,
    id: &DelegationId,
    scopes: Scopes,
    times: Times,
    effective: Timestamp,
) -> TokenClaims {
    let presented = &caller.claims;
    let issued_at = floor_to_second(effective);
    let mut chain = presented.chain.clone();
    chain.push(ChainLink {
        id: presented.id.clone(),
        kid: caller.kid.clone(),
    });
    TokenClaims {
        fleet: presented.fleet.clone(),
        principal: child.as_principal().clone(),
        id: id.as_token_id().clone(),
        issued_at: Some(issued_at),
        not_before: times.not_before.max(issued_at),
        expires_at: times.expires_at,
        scopes,
        kind: Some(TokenKind::Agent),
        owner: presented.owner.clone(),
        parent: Some(presented.principal.clone()),
        chain,
    }
}

/// `time` rounded down to the second.
pub(crate) fn floor_to_second(time: Timestamp) -> Timestamp {
    Timestamp::from_millis(time.as_millis() - time.as_millis() % SECOND)
}

#[cfg(test)]
mod tests {
    use fleetring_control::scope::GrantStepError;
    use tonic::Code;

    use super::*;

    fn scopes(texts: &[&str]) -> Scopes {
        texts
            .iter()
            .map(|text| text.parse().expect("a scope"))
            .collect()
    }

    fn name(text: &str) -> AgentName {
        text.parse().expect("an agent name")
    }

    const HOUR: u64 = 3_600_000;
    const NOW: u64 = 1_800_000_000_000;

    fn at(millis: u64) -> Timestamp {
        Timestamp::from_millis(millis)
    }

    #[test]
    fn root_default_child_gets_admin_and_stream_on_its_prefix() {
        let child = name("agent-42.c7");
        let held = scopes(&[
            "admin:agent-42-*",
            "admin:agent-42.*",
            "stream:agent-42.*",
            "grant:agent-42.*",
        ]);
        assert_eq!(
            resolve_scopes(&held, &child, None).expect("resolves"),
            scopes(&["admin:agent-42.c7-*", "stream:agent-42.c7-*"]).normalized()
        );
        // `admin` doesn't imply `stream`, so a root without `stream` hands on none.
        let held = scopes(&["admin:agent-42-*", "admin:agent-42.*", "grant:agent-42.*"]);
        assert_eq!(
            resolve_scopes(&held, &child, None).expect("resolves"),
            scopes(&["admin:agent-42.c7-*"])
        );
    }

    #[test]
    fn read_only_root_default_child_gets_read_and_stream() {
        let held = scopes(&[
            "read:agent-42-*",
            "read:agent-42.*",
            "stream:agent-42.*",
            "grant:agent-42.*",
        ]);
        assert_eq!(
            resolve_scopes(&held, &name("agent-42.c7"), None).expect("resolves"),
            scopes(&["read:agent-42.c7-*", "stream:agent-42.c7-*"]).normalized()
        );
    }

    #[test]
    fn no_grant_is_refused_naming_the_childs_prefix() {
        let held = scopes(&["admin:agent-42-*", "admin:agent-42.*"]);
        let refused = resolve_scopes(&held, &name("agent-42.c7"), None).expect_err("no grant");
        assert_eq!(refused.code(), Code::PermissionDenied);
        assert_eq!(
            refused.message(),
            "your token holds no grant over agent-42.c7-"
        );
        // A grant over another subtree leaves the defaults empty, refused the same way.
        let elsewhere = scopes(&["admin:agent-42.*", "grant:agent-42.d1.*"]);
        let refused = resolve_scopes(&elsewhere, &name("agent-42.c7"), None).expect_err("empty");
        assert_eq!(
            refused.message(),
            "your token holds no grant over agent-42.c7-"
        );
        let explicit = resolve_scopes(
            &held,
            &name("agent-42.c7"),
            Some(&scopes(&["admin:agent-42.c7-*"])),
        )
        .expect_err("no grant");
        assert_eq!(explicit.code(), Code::PermissionDenied);
    }

    #[test]
    fn explicit_scopes_outside_the_grant_are_refused_naming_the_first() {
        let held = scopes(&["read:agent-42-*", "read:agent-42.*", "grant:agent-42.*"]);
        let child = name("agent-42.c7");
        let refused = resolve_scopes(&held, &child, Some(&scopes(&["admin:agent-42.c7-*"])))
            .expect_err("admin isn't held");
        assert_eq!(refused.code(), Code::PermissionDenied);
        assert!(
            refused.message().contains("admin:agent-42.c7-*"),
            "{refused:?}"
        );
        let held = scopes(&["admin:agent-42-*", "admin:agent-42.*", "grant:agent-42.*"]);
        let refused = resolve_scopes(&held, &child, Some(&scopes(&["write:agent-42-memory"])))
            .expect_err("outside the grant");
        assert!(
            refused.message().contains("write:agent-42-memory"),
            "{refused:?}"
        );
        let refused = resolve_scopes(&held, &child, Some(&scopes(&["fleet"])))
            .expect_err("fleet is never handed on");
        assert_eq!(refused.code(), Code::PermissionDenied);
        assert!(refused.message().contains("fleet"), "{refused:?}");
    }

    #[test]
    fn a_grant_reaching_beyond_the_childs_own_children_is_refused_by_the_tier() {
        let held = scopes(&["admin:agent-42.*", "grant:agent-42.*"]);
        let refused = resolve_scopes(
            &held,
            &name("agent-42.c7"),
            Some(&scopes(&["grant:agent-42.*"])),
        )
        .expect_err("outside the tier");
        assert_eq!(refused.code(), Code::InvalidArgument);
        assert!(
            refused.message().contains("grant:agent-42.*"),
            "{refused:?}"
        );
        // An operator-chosen scope outside the root's namespace is refused the same way.
        let held = scopes(&["admin:shared-*", "grant:shared-*"]);
        let refused = resolve_scopes(
            &held,
            &name("agent-42.c7"),
            Some(&scopes(&["admin:shared-*"])),
        )
        .expect_err("outside the root");
        assert_eq!(refused.code(), Code::InvalidArgument);
    }

    #[test]
    fn segments_with_dot_dash_or_uppercase_are_refused() {
        let parent = name("agent-42");
        for bad in ["c.7", "c-7", "C7", "", &"x".repeat(33)] {
            let refused = child_name(&parent, bad).expect_err(bad);
            assert_eq!(refused.code(), Code::InvalidArgument, "{bad:?}");
        }
        assert_eq!(
            child_name(&parent, "c_7").expect("valid"),
            name("agent-42.c_7")
        );
    }

    #[test]
    fn child_name_over_64_bytes_is_refused() {
        let parent = name(&format!("agent-{}", "a".repeat(30)));
        let segment = "b".repeat(30);
        let refused = child_name(&parent, &segment).expect_err("too long");
        assert_eq!(refused.code(), Code::InvalidArgument);
    }

    #[test]
    fn depth_nine_is_refused_as_a_limit() {
        let deep = name("agent-1.a.b.c.d.e.f.g.h");
        assert_eq!(deep.depth(), 8);
        let refused = child_name(&deep, "i").expect_err("depth nine");
        assert_eq!(refused.code(), Code::ResourceExhausted);
        assert!(child_name(&name("agent-1.a.b.c.d.e.f.g"), "h").is_ok());
    }

    #[test]
    fn the_default_lifetime_is_two_hours_capped_by_the_parent() {
        let now = at(NOW);
        let times = resolve_times(now, at(NOW + 10 * HOUR), None, None).expect("resolves");
        assert_eq!(times.not_before, now);
        assert_eq!(times.expires_at, at(NOW + 2 * HOUR));
        assert!(!capped(None, times.expires_at, at(NOW + 10 * HOUR)));
        let parent = at(NOW + HOUR);
        let times = resolve_times(now, parent, None, None).expect("resolves");
        assert_eq!(times.expires_at, parent);
        assert!(capped(None, times.expires_at, parent));
        // An explicit lifetime is never reported capped.
        assert!(!capped(Some(Millis::new(HOUR)), parent, parent));
    }

    #[test]
    fn lifetime_remaining_plus_one_second_25h_and_59s_are_refused() {
        let now = at(NOW);
        let parent = at(NOW + 30 * HOUR);
        let short_parent = at(NOW + HOUR);
        let refused = |parent, lifetime| {
            resolve_times(now, parent, None, Some(Millis::new(lifetime)))
                .expect_err("refused")
                .code()
        };
        assert_eq!(refused(short_parent, HOUR + 1_000), Code::InvalidArgument);
        assert_eq!(refused(parent, 25 * HOUR), Code::InvalidArgument);
        assert_eq!(refused(parent, 59_000), Code::InvalidArgument);
        assert!(resolve_times(now, short_parent, None, Some(Millis::new(HOUR))).is_ok());
        assert!(resolve_times(now, parent, None, Some(Millis::new(24 * HOUR))).is_ok());
        assert!(resolve_times(now, parent, None, Some(Millis::new(60_000))).is_ok());
    }

    #[test]
    fn not_before_31s_past_24h_and_1s_ahead_or_at_parent_exp_is_refused() {
        let now = at(NOW);
        let parent = at(NOW + 30 * HOUR);
        let refused = |not_before: u64, parent| {
            resolve_times(now, parent, Some(at(not_before)), None)
                .expect_err("refused")
                .code()
        };
        assert_eq!(refused(NOW - 31_000, parent), Code::InvalidArgument);
        assert_eq!(
            refused(NOW + 24 * HOUR + 1_000, parent),
            Code::InvalidArgument
        );
        assert_eq!(refused(NOW + HOUR, at(NOW + HOUR)), Code::InvalidArgument);
        assert_eq!(
            refused(NOW + 2 * HOUR, at(NOW + HOUR)),
            Code::InvalidArgument
        );
        let past = resolve_times(now, parent, Some(at(NOW - 30_000)), None).expect("in leeway");
        assert_eq!(past.not_before, at(NOW - 30_000));
        let ahead = resolve_times(now, parent, Some(at(NOW + 24 * HOUR)), None).expect("ahead");
        assert_eq!(ahead.expires_at, at(NOW + 26 * HOUR));
    }

    #[test]
    fn times_are_whole_seconds_and_never_outlive_the_parent() {
        let now = at(NOW + 999);
        let parent = at(NOW + HOUR);
        let times = resolve_times(now, parent, None, Some(Millis::new(HOUR - 1))).expect("fits");
        assert_eq!(times.not_before, at(NOW));
        assert_eq!(times.expires_at.as_millis() % 1_000, 0);
        assert!(times.expires_at <= parent);
    }

    #[test]
    fn a_parent_expiring_now_leaves_no_child() {
        let now = at(NOW + 500);
        let refused = resolve_times(now, at(NOW + 900), None, None).expect_err("no time left");
        assert_eq!(refused.code(), Code::FailedPrecondition);
    }

    #[test]
    fn control_time_leading_the_clock_by_more_than_30_s_is_unavailable() {
        let clock = at(NOW);
        assert_eq!(
            resolve_now(clock, at(NOW + 30_000)).expect("within"),
            at(NOW + 30_000)
        );
        assert_eq!(resolve_now(clock, at(NOW - 5_000)).expect("behind"), clock);
        let refused = resolve_now(clock, at(NOW + 31_000)).expect_err("leads");
        assert_eq!(refused.code(), Code::Unavailable);
        assert_eq!(
            refused.message(),
            "control time leads this control node's clock by 31 s; retry once its clock catches \
             up"
        );
    }

    #[test]
    fn child_claims_extend_the_callers_chain_and_raise_nbf_to_iat() {
        let caller = CallerToken {
            claims: TokenClaims {
                fleet: "test".into(),
                principal: "agent-42".parse().expect("a name"),
                id: "root-42".parse().expect("an id"),
                issued_at: Some(at(NOW - HOUR)),
                not_before: at(NOW - HOUR),
                expires_at: at(NOW + 10 * HOUR),
                scopes: scopes(&["admin:agent-42.*", "grant:agent-42.*"]),
                kind: Some(TokenKind::Agent),
                owner: Some("alice".parse().expect("a name")),
                parent: None,
                chain: Vec::new(),
            },
            kid: "authority-key".parse().expect("a key id"),
        };
        let id = DelegationId::from_bytes([7; 16]);
        let times = Times {
            not_before: at(NOW),
            expires_at: at(NOW + HOUR),
        };
        let claims = child_claims(
            &caller,
            &name("agent-42.c7"),
            &id,
            scopes(&["admin:agent-42.c7-*"]),
            times,
            at(NOW + 1_500),
        );
        assert_eq!(claims.issued_at, Some(at(NOW + 1_000)));
        assert_eq!(claims.not_before, at(NOW + 1_000));
        assert_eq!(
            claims.chain,
            vec![ChainLink {
                id: caller.claims.id.clone(),
                kid: caller.kid.clone(),
            }]
        );
        assert_eq!(claims.owner, caller.claims.owner);
        assert_eq!(claims.check_recordable(), Ok(name("agent-42.c7")));
    }

    /// The actions and patterns the property draws scopes from: the child's own prefixes, its
    /// parent's, a sibling's, another root's, and wider ones.
    const ACTIONS: [&str; 6] = ["read", "write", "fork", "admin", "stream", "grant"];
    const PATTERNS: [&str; 9] = [
        "agent-42-*",
        "agent-42.*",
        "agent-42.c7-*",
        "agent-42.c7.*",
        "agent-42.c7-notes",
        "agent-42.d1-*",
        "agent-43-*",
        "*",
        "agent-*",
    ];

    /// A small deterministic generator (xorshift64*), so the property runs the same cases on
    /// every machine, emulated ones included.
    struct Cases(u64);

    impl Cases {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, bound: usize) -> usize {
            usize::try_from(self.next() % u64::try_from(bound).expect("small"))
                .expect("below a usize")
        }

        fn scopes(&mut self, at_least: usize, at_most: usize) -> Scopes {
            let count = at_least + self.below(at_most - at_least + 1);
            (0..count)
                .map(|_| {
                    let action = ACTIONS[self.below(ACTIONS.len())];
                    let pattern = PATTERNS[self.below(PATTERNS.len())];
                    format!("{action}:{pattern}").parse().expect("a scope")
                })
                .collect()
        }
    }

    /// I1: the default scopes are exactly the narrowing grant step of the default request,
    /// whenever the result is in the agent tier; explicit scopes are exactly what was asked, in
    /// canonical form, whenever they pass at all, and pass whenever the strict grant step and
    /// the tier do.
    #[test]
    fn prop_resolved_scopes_equal_the_grant_step() {
        let child = name("agent-42.c7");
        let mut cases = Cases(0x9E37_79B9_7F4A_7C15);
        for case in 0..2_000 {
            let held = cases.scopes(0, 6);
            let asked = cases.scopes(1, 3);
            let stepped = held.grant_step(&default_request(&child));
            match resolve_scopes(&held, &child, None) {
                Ok(resolved) => assert_eq!(Ok(resolved), stepped, "case {case}: {held:?}"),
                Err(_) => assert!(
                    stepped
                        .as_ref()
                        .map_or(true, |scopes| tier::check(&child, scopes).is_err()),
                    "case {case}: refused defaults the grant step and tier allow: {held:?}"
                ),
            }
            let strict = held
                .grant_step_strict(&asked)
                .ok()
                .filter(|scopes| tier::check(&child, scopes).is_ok());
            match resolve_scopes(&held, &child, Some(&asked)) {
                Ok(resolved) => {
                    assert_eq!(resolved, asked.normalized(), "case {case}");
                    assert_eq!(Some(resolved), strict, "case {case}");
                }
                Err(refused) => assert_eq!(strict, None, "case {case}: {refused:?}"),
            }
        }
    }

    #[test]
    fn the_strict_step_error_for_fleet_is_not_covered() {
        let held = scopes(&["admin:agent-42.*", "grant:agent-42.*"]);
        assert!(matches!(
            held.grant_step_strict(&scopes(&["fleet"])),
            Err(GrantStepError::NotCovered { .. })
        ));
    }
}
