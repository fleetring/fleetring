//! Wall-clock cost bounds for the scope lattice (ADR-0044 §2.4).
//!
//! The lattice lives in `fleetring_control::scope`, which reads no clock. These tests time it from
//! here, where `Instant::now` is allowed.

use fleetring_control::scope::{GrantStepError, MAX_SCOPES, Scope, Scopes};

fn scope(text: &str) -> Scope {
    text.parse()
        .unwrap_or_else(|e| panic!("'{text}' is a scope: {e}"))
}

fn scopes(texts: &[&str]) -> Scopes {
    texts.iter().map(|text| scope(text)).collect()
}

/// A list of `count` distinct scopes no one of which covers another, over `action`.
fn distinct(action: &str, tag: &str, count: usize) -> Scopes {
    (0..count)
        .map(|i| scope(&format!("{action}:{tag}-{i}-*")))
        .collect()
}

#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "a cost test measures wall time; the lattice it measures reads no clock"
)]
fn an_oversized_side_is_refused_before_any_work() {
    let holder = {
        let mut held = distinct("admin", "a", MAX_SCOPES - 1);
        held.extend([scope("grant:*")]);
        held
    };
    let too_many = distinct("read", "a", MAX_SCOPES + 1);
    let fine = distinct("read", "a", MAX_SCOPES);
    let refused = GrantStepError::TooMany { limit: MAX_SCOPES };
    // A side far over the limit is refused as fast as one just over it: the length is
    // checked before anything is compared.
    let huge = distinct("read", "a", 20_000);
    let started = std::time::Instant::now();
    for asked in [&too_many, &huge] {
        assert_eq!(holder.grant_step(asked), Err(refused.clone()));
        assert_eq!(holder.grant_step_strict(asked), Err(refused.clone()));
        assert_eq!(asked.grant_step(&fine), Err(refused.clone()));
        // `TooManyScopes` is `#[non_exhaustive]`, so compare its limit.
        assert_eq!(
            holder.intersect(asked).map_err(|e| e.limit),
            Err(MAX_SCOPES)
        );
        // `TooManyScopes` is `#[non_exhaustive]`, so compare its limit.
        assert_eq!(
            asked.intersect(&holder).map_err(|e| e.limit),
            Err(MAX_SCOPES)
        );
    }
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "refusing took {:?}",
        started.elapsed()
    );
    assert_eq!(
        refused.to_string(),
        "a set of scopes to narrow holds at most 256"
    );
    // Exactly the limit is accepted, on either side.
    assert_eq!(
        scopes(&["admin:*", "grant:*"]).grant_step_strict(&fine),
        Ok(fine.normalized())
    );
    assert_eq!(holder.len(), MAX_SCOPES);
    assert_eq!(
        fine.intersect(&holder).map(|both| both.len()),
        Ok(MAX_SCOPES - 1)
    );
}

/// The narrowing's worst sides at the limit finish well within a generous bound, even in a
/// debug build: 256 effective scopes, half of them `grant`, against 256 requested ones whose
/// meets with them are all distinct.
#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "a cost test measures wall time; the lattice it measures reads no clock"
)]
fn narrowing_at_the_limit_stays_cheap() {
    let half = MAX_SCOPES / 2;
    let mut effective = distinct("admin", "p", half);
    effective.extend(distinct("grant", "p", half).as_slice().iter().cloned());
    let mut requested: Scopes = (0..MAX_SCOPES)
        .map(|i| scope(&format!("fork:p-{}-x{i}", i % half)))
        .collect();
    let started = std::time::Instant::now();
    let both = effective.intersect(&requested).expect("within the limit");
    assert_eq!(both.len(), MAX_SCOPES);
    let narrowed = effective.grant_step(&requested).expect("a grant step");
    assert_eq!(narrowed, requested.normalized());
    assert_eq!(effective.grant_step_strict(&requested), Ok(narrowed));
    // And the pathological shape: every pair of sides meets in a scope of its own.
    requested = distinct("read", "q", MAX_SCOPES);
    let wide: Scopes = (0..MAX_SCOPES)
        .map(|i| scope(&format!("admin:q-{i}*")))
        .collect();
    let both = wide.intersect(&requested).expect("within the limit");
    assert!(both.len() <= MAX_SCOPES);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "narrowing at the limit took {:?}",
        started.elapsed()
    );
}
