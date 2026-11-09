//! The scope lattice: which scope covers which, and the two exact operations that narrow scopes,
//! the intersection and the grant step (ADR-0044, point 2.4).
//!
//! Everything here is a pure function of its arguments: no I/O, clock, randomness, or runtime.
//! Patterns, prefixes, and separators are compared byte for byte, so nothing widens through a
//! shorter prefix, another separator, a `*` elsewhere than at the end, or an action that implies
//! another. A pattern is `Exact(n)`, matching the name `n`, or `Prefix(p)`, written `p*`,
//! matching every name whose bytes start with `p` (`*` alone is `Prefix("")`). A `*` anywhere
//! but last is an ordinary byte of the name.
//!
//! The results are canonical: [`Scopes::normalized`] drops every scope another one covers and
//! sorts the rest by their text, byte for byte, so equal sets of scopes compare equal.

use crate::changes::{DatabaseMatcher, Pattern};

use super::{Action, Rule, Scope, Scopes};

impl Action {
    /// The actions a scope for `self` covers, itself included, strongest first (ADR-0044,
    /// point 2's table).
    fn implied(self) -> &'static [Self] {
        match self {
            Self::Admin => &[Self::Admin, Self::Write, Self::Fork, Self::Read],
            Self::Write => &[Self::Write, Self::Read],
            Self::Fork => &[Self::Fork, Self::Read],
            Self::Read => &[Self::Read],
            Self::Stream => &[Self::Stream],
            Self::Grant => &[Self::Grant],
        }
    }

    /// Whether a scope for `self` also covers `wanted`: `admin` covers `write`, `fork`, and
    /// `read`; `write` and `fork` each cover `read`; `stream` and `grant` cover only themselves.
    ///
    /// This is the lattice's order. Whether a request may take an action is
    /// [`Scope::allows`], which never lets `grant` authorize one.
    #[must_use]
    pub fn covers(self, wanted: Self) -> bool {
        self.implied().contains(&wanted)
    }

    /// The strongest action both `self` and `other` cover, or `None` if they cover none in
    /// common: `admin ∧ x = x` for `x` in `read`, `write`, `fork`, `admin`; `write ∧ fork =
    /// read`; `stream` and `grant` meet only themselves.
    #[must_use]
    pub fn meet(self, other: Self) -> Option<Self> {
        let common = || {
            self.implied()
                .iter()
                .copied()
                .filter(move |action| other.covers(*action))
        };
        common().find(|candidate| common().all(|action| candidate.covers(action)))
    }
}

/// Whether `held` matches every name `wanted` matches: a prefix covers a longer prefix and an
/// exact name that start with its bytes, and an exact name covers only itself.
fn pattern_covers(held: &DatabaseMatcher, wanted: &DatabaseMatcher) -> bool {
    match (held.pattern(), wanted.pattern()) {
        (Pattern::Prefix(held), Pattern::Prefix(wanted) | Pattern::Exact(wanted)) => {
            wanted.as_bytes().starts_with(held.as_bytes())
        }
        (Pattern::Exact(held), Pattern::Exact(wanted)) => held.as_bytes() == wanted.as_bytes(),
        (Pattern::Exact(_), Pattern::Prefix(_)) => false,
    }
}

/// The pattern matching exactly the names both `a` and `b` match, or `None` if no name matches
/// both. It is always one of the two: the exact name under the prefix, or the longer prefix.
fn pattern_meet<'a>(a: &'a DatabaseMatcher, b: &'a DatabaseMatcher) -> Option<&'a DatabaseMatcher> {
    if pattern_covers(a, b) {
        Some(b)
    } else if pattern_covers(b, a) {
        Some(a)
    } else {
        None
    }
}

impl Scope {
    /// Whether this scope covers `other`: its action covers `other`'s and its pattern matches
    /// every name `other`'s matches. `fleet` covers only `fleet`.
    ///
    /// ```
    /// use fleetringd::auth::Scope;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let held: Scope = "admin:agent-42-*".parse()?;
    /// assert!(held.covers(&"fork:agent-42-mem-*".parse()?));
    /// assert!(!held.covers(&"admin:agent-42*".parse()?));
    /// assert!(!held.covers(&"admin:agent-42.c1-*".parse()?));
    /// assert!(!held.covers(&"grant:agent-42-*".parse()?));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn covers(&self, other: &Self) -> bool {
        match (&self.rule, &other.rule) {
            (Rule::Fleet, Rule::Fleet) => true,
            (
                Rule::Databases { action, pattern },
                Rule::Databases {
                    action: wanted,
                    pattern: names,
                },
            ) => action.covers(*wanted) && pattern_covers(pattern, names),
            (Rule::Fleet, Rule::Databases { .. }) | (Rule::Databases { .. }, Rule::Fleet) => false,
        }
    }

    /// The scope allowing exactly what both scopes allow, or `None` if they share nothing:
    /// the meet of the actions over the meet of the patterns. `fleet` meets only `fleet`.
    #[must_use]
    pub fn meet(&self, other: &Self) -> Option<Self> {
        match (&self.rule, &other.rule) {
            (Rule::Fleet, Rule::Fleet) => Some(Self::fleet()),
            (
                Rule::Databases { action, pattern },
                Rule::Databases {
                    action: other_action,
                    pattern: other_pattern,
                },
            ) => {
                let action = action.meet(*other_action)?;
                let pattern = pattern_meet(pattern, other_pattern)?;
                Some(Self::new(action, pattern.clone()))
            }
            (Rule::Fleet, Rule::Databases { .. }) | (Rule::Databases { .. }, Rule::Fleet) => None,
        }
    }
}

/// The canonical set of `scopes`: every scope another one covers dropped, duplicates included,
/// and the rest sorted by their text.
fn normalized(scopes: impl IntoIterator<Item = Scope>) -> Scopes {
    let mut scopes: Vec<(String, Scope)> = scopes
        .into_iter()
        .map(|scope| (scope.to_string(), scope))
        .collect();
    scopes.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
    scopes.dedup_by(|(_, a), (_, b)| a == b);
    // Distinct scopes never cover each other both ways, so this keeps one of every equal pair
    // and drops only what something else still covers.
    let kept: Vec<Scope> = scopes
        .iter()
        .filter(|(_, scope)| {
            !scopes
                .iter()
                .any(|(_, other)| other != scope && other.covers(scope))
        })
        .map(|(_, scope)| scope.clone())
        .collect();
    Scopes::new(kept)
}

impl Scopes {
    /// The same scopes in canonical form: any scope another one covers dropped, and the rest
    /// sorted by their text. It allows exactly what `self` allows.
    #[must_use]
    pub fn normalized(&self) -> Self {
        normalized(self.scopes.iter().cloned())
    }

    /// The scopes allowing exactly what both `self` and `other` allow: every non-empty
    /// pairwise [`Scope::meet`], normalized. It is commutative and idempotent, and never wider
    /// than either side (ADR-0044, point 2.4).
    ///
    /// The work is the product of the two sets' sizes, then the square of the result's, so
    /// callers bound what they pass (a token is at most 8 KiB).
    ///
    /// ```
    /// use fleetringd::auth::Scopes;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let held: Scopes = ["write:agent-42-*"].iter().map(|s| s.parse()).collect::<Result<_, _>>()?;
    /// let asked: Scopes = ["fork:agent-42-*"].iter().map(|s| s.parse()).collect::<Result<_, _>>()?;
    /// assert_eq!(held.intersect(&asked).as_slice(), ["read:agent-42-*".parse()?]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        normalized(
            self.scopes
                .iter()
                .flat_map(|a| other.scopes.iter().filter_map(move |b| a.meet(b))),
        )
    }

    /// The patterns of the `grant` scopes.
    fn grant_patterns(&self) -> Vec<&DatabaseMatcher> {
        self.scopes
            .iter()
            .filter_map(|scope| match &scope.rule {
                Rule::Databases {
                    action: Action::Grant,
                    pattern,
                } => Some(pattern),
                Rule::Databases { .. } | Rule::Fleet => None,
            })
            .collect()
    }

    /// The grant step without its refusals: the intersection, each pattern met with each
    /// `grant` pattern, and `fleet` dropped.
    fn narrowed(&self, requested: &Self, grants: &[&DatabaseMatcher]) -> Self {
        let intersection = self.intersect(requested);
        normalized(intersection.scopes.iter().flat_map(|scope| {
            let held = match &scope.rule {
                Rule::Databases { action, pattern } => Some((*action, pattern)),
                Rule::Fleet => None,
            };
            held.into_iter().flat_map(move |(action, pattern)| {
                grants.iter().filter_map(move |grant| {
                    pattern_meet(pattern, grant).map(|met| Scope::new(action, met.clone()))
                })
            })
        }))
    }

    /// What a holder with these effective scopes may hand a new holder that asks for
    /// `requested`, narrowed silently (ADR-0044, point 2.4): the intersection with
    /// `requested`, then each pattern met with each pattern the `grant` scopes hold, keeping
    /// what isn't empty and dropping `fleet`, normalized.
    ///
    /// The result is covered both by `self` and by the `grant` patterns. It may be narrower than
    /// `requested`; [`grant_step_strict`](Self::grant_step_strict) refuses instead.
    ///
    /// # Errors
    ///
    /// [`GrantStepError::NoGrant`] if `self` holds no `grant` scope, and
    /// [`GrantStepError::Empty`] if nothing remains.
    pub fn grant_step(&self, requested: &Self) -> Result<Self, GrantStepError> {
        let grants = self.grant_patterns();
        if grants.is_empty() {
            return Err(GrantStepError::NoGrant);
        }
        let narrowed = self.narrowed(requested, &grants);
        if narrowed.is_empty() {
            return Err(GrantStepError::Empty);
        }
        Ok(narrowed)
    }

    /// The grant step that never narrows: exactly `requested`, normalized, if the grant step
    /// keeps every requested scope whole, and a refusal naming the first scope it would narrow
    /// or drop otherwise. `fleet` is never kept, so asking for it is refused.
    ///
    /// ```
    /// use fleetringd::auth::{GrantStepError, Scopes};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let parse = |texts: &[&str]| texts.iter().map(|s| s.parse()).collect::<Result<Scopes, _>>();
    /// let held = parse(&["admin:agent-42-*", "admin:agent-42.*", "grant:agent-42.*"])?;
    /// let child = parse(&["admin:agent-42.c7-*"])?;
    /// assert_eq!(held.grant_step_strict(&child)?, child);
    /// let wider = parse(&["admin:agent-42.c7-*", "read:agent-42-memory"])?;
    /// assert_eq!(
    ///     held.grant_step_strict(&wider),
    ///     Err(GrantStepError::NotCovered { scope: "read:agent-42-memory".parse()? })
    /// );
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`GrantStepError::NoGrant`] if `self` holds no `grant` scope,
    /// [`GrantStepError::Empty`] if `requested` is empty, and [`GrantStepError::NotCovered`]
    /// naming the first requested scope, in canonical order, that the grant step would narrow
    /// or drop.
    pub fn grant_step_strict(&self, requested: &Self) -> Result<Self, GrantStepError> {
        let grants = self.grant_patterns();
        if grants.is_empty() {
            return Err(GrantStepError::NoGrant);
        }
        let wanted = requested.normalized();
        if wanted.is_empty() {
            return Err(GrantStepError::Empty);
        }
        let narrowed = self.narrowed(requested, &grants);
        if let Some(scope) = wanted
            .scopes
            .iter()
            .find(|wanted| !narrowed.scopes.iter().any(|kept| kept.covers(wanted)))
        {
            return Err(GrantStepError::NotCovered {
                scope: scope.clone(),
            });
        }
        Ok(wanted)
    }
}

/// Why the grant step refuses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum GrantStepError {
    /// The effective scopes hold no `grant` scope, so they can name no new holder.
    #[error("the scopes hold no grant, so they can't name a new holder")]
    NoGrant,
    /// Nothing requested is within both the effective scopes and the `grant` patterns.
    #[error("nothing requested is within both the scopes held and their grant")]
    Empty,
    /// The strict grant step would narrow or drop this requested scope.
    #[error("the scope {scope} isn't wholly within both the scopes held and their grant")]
    NotCovered {
        /// The requested scope, as normalized.
        scope: Scope,
    },
}

#[cfg(test)]
mod tests {
    use fleetring_control::DatabaseName;

    use super::*;

    fn scope(text: &str) -> Scope {
        text.parse()
            .unwrap_or_else(|e| panic!("'{text}' is a scope: {e}"))
    }

    fn scopes(texts: &[&str]) -> Scopes {
        texts.iter().map(|text| scope(text)).collect()
    }

    fn texts(scopes: &Scopes) -> Vec<String> {
        scopes.as_slice().iter().map(Scope::to_string).collect()
    }

    #[test]
    fn the_actions_imply_as_point_two_says() {
        use Action::{Admin, Fork, Grant, Read, Stream, Write};
        let implied = [
            (Admin, vec![Admin, Fork, Read, Write]),
            (Fork, vec![Fork, Read]),
            (Grant, vec![Grant]),
            (Read, vec![Read]),
            (Stream, vec![Stream]),
            (Write, vec![Read, Write]),
        ];
        for (held, covered) in implied {
            for wanted in Action::ALL {
                assert_eq!(
                    held.covers(wanted),
                    covered.contains(&wanted),
                    "{held} covers {wanted}"
                );
            }
        }
        // `read` doesn't imply `fork`, `fork` doesn't imply `write`, and nothing implies `grant`.
        assert!(!Read.covers(Fork));
        assert!(!Write.covers(Fork));
        assert!(!Fork.covers(Write));
        for held in Action::ALL {
            assert_eq!(held.covers(Grant), held == Grant, "{held}");
            assert_eq!(Grant.covers(held), held == Grant, "{held}");
        }
    }

    #[test]
    fn the_meet_of_two_actions_is_the_strongest_both_cover() {
        use Action::{Admin, Fork, Grant, Read, Stream, Write};
        let table = [
            (Admin, Admin, Some(Admin)),
            (Admin, Write, Some(Write)),
            (Admin, Fork, Some(Fork)),
            (Admin, Read, Some(Read)),
            (Admin, Stream, None),
            (Admin, Grant, None),
            (Write, Write, Some(Write)),
            (Write, Fork, Some(Read)),
            (Write, Read, Some(Read)),
            (Write, Stream, None),
            (Write, Grant, None),
            (Fork, Fork, Some(Fork)),
            (Fork, Read, Some(Read)),
            (Fork, Stream, None),
            (Fork, Grant, None),
            (Read, Read, Some(Read)),
            (Read, Stream, None),
            (Read, Grant, None),
            (Stream, Stream, Some(Stream)),
            (Stream, Grant, None),
            (Grant, Grant, Some(Grant)),
        ];
        assert_eq!(table.len(), 21, "every unordered pair of six actions");
        for (a, b, meet) in table {
            assert_eq!(a.meet(b), meet, "{a} ∧ {b}");
            assert_eq!(b.meet(a), meet, "{b} ∧ {a}");
        }
        // The meet covers exactly what both cover.
        for a in Action::ALL {
            for b in Action::ALL {
                for x in Action::ALL {
                    let both = a.covers(x) && b.covers(x);
                    assert_eq!(a.meet(b).is_some_and(|m| m.covers(x)), both, "{a} {b} {x}");
                }
            }
        }
    }

    /// Covering cases that a careless comparison would get wrong. Each row is the held scope, the
    /// wanted scope, and whether the held one covers it.
    const TRICKY: &[(&str, &str, bool)] = &[
        // A shorter prefix, or none, is wider.
        ("admin:agent-42-*", "admin:agent-42*", false),
        ("admin:agent-42*", "admin:agent-42-*", true),
        ("admin:agent-42-*", "admin:agent-4*", false),
        ("admin:agent-4*", "admin:agent-42-*", true),
        ("admin:agent-42-*", "admin:agent-42", false),
        ("admin:agent-42-*", "admin:agent-42-", true),
        ("admin:agent-42-*", "admin:agent-420-x", false),
        ("admin:agent-42", "admin:agent-42*", false),
        ("admin:agent-42", "admin:agent-42-*", false),
        ("admin:agent-42", "admin:agent-42", true),
        ("admin:agent-42", "admin:agent-420", false),
        // Separators are bytes: `-` and `.` never stand for each other.
        ("admin:agent-42-*", "admin:agent-42.c1-*", false),
        ("admin:agent-42.*", "admin:agent-42-*", false),
        ("admin:agent-42.*", "admin:agent-42.c1-*", true),
        ("admin:agent-42.c1-*", "admin:agent-42.c1*", false),
        ("admin:agent-42.c1-*", "admin:agent-42.c10-*", false),
        ("admin:agent-42.c1-*", "admin:agent-42.c1-x", true),
        ("admin:agent-42.c1*", "admin:agent-42.c10-*", true),
        // Dotted names: each level is its own prefix.
        ("admin:agent-42.*", "admin:agent-42.c7.*", true),
        ("admin:agent-42.c7.*", "admin:agent-42.c7-*", false),
        ("admin:agent-42.c7-*", "admin:agent-42.c7.*", false),
        ("admin:a.b-*", "admin:a.b.c-*", false),
        ("admin:a.b*", "admin:a.b.c-*", true),
        ("grant:agent-42.*", "grant:agent-42.c7.*", true),
        ("grant:agent-42.c7.*", "grant:agent-42.*", false),
        // A `*` anywhere but last is a byte of the name.
        ("admin:agent-*-x", "admin:agent-1-x", false),
        ("admin:agent-*-x", "admin:agent-*-x", true),
        ("admin:agent-*", "admin:agent-*-x", true),
        ("admin:agent-**", "admin:agent-1", false),
        ("admin:agent-**", "admin:agent-*x", true),
        ("admin:agent-**", "admin:agent-*", false),
        ("admin:agent-*", "admin:agent-**", true),
        // `*` alone is every name, and nothing narrower covers it.
        ("read:*", "read:x", true),
        ("read:*", "read:*", true),
        ("read:x", "read:*", false),
        ("read:x-*", "read:*", false),
        ("read:*", "read:_fleetring/identity", true),
        // `fleet` covers only `fleet`, and no database scope covers it.
        ("fleet", "fleet", true),
        ("fleet", "read:*", false),
        ("fleet", "read:_fleetring/identity", false),
        ("admin:*", "fleet", false),
        ("grant:*", "fleet", false),
        // Names reserved for FleetRing compare like any other bytes.
        ("admin:_fleetring*", "admin:_fleetring/identity", true),
        ("admin:_fleetring-*", "admin:_fleetring*", false),
        ("admin:_fleetring/identity", "admin:_fleetring/*", false),
        ("admin:_fleetring/*", "admin:_fleetring/identity", true),
        ("admin:_fleetring/*", "admin:_fleetringx", false),
        // Actions: nothing widens through an implication.
        ("admin:x", "fork:x", true),
        ("admin:x", "write:x", true),
        ("read:x", "fork:x", false),
        ("write:x", "fork:x", false),
        ("fork:x", "write:x", false),
        ("fork:x", "read:x", true),
        ("fork:x", "admin:x", false),
        ("admin:x", "stream:x", false),
        ("admin:x", "grant:x", false),
        ("admin:*", "grant:x", false),
        ("grant:x-*", "grant:x-1-*", true),
        ("grant:x-*", "read:x-1", false),
        ("grant:*", "admin:x", false),
        ("stream:*", "read:x", false),
        ("read:x-*", "stream:x-1", false),
    ];

    #[test]
    fn tricky_patterns_cover_exactly() {
        for &(held, wanted, covers) in TRICKY {
            assert_eq!(
                scope(held).covers(&scope(wanted)),
                covers,
                "{held} covers {wanted}"
            );
            // Covering both ways is equality.
            if covers && scope(wanted).covers(&scope(held)) {
                assert_eq!(held, wanted);
            }
            // And the strict grant step agrees: a holder of `held` with `grant:*` hands over
            // `wanted` whole or refuses, naming it.
            let holder = scopes(&[held, "grant:*"]);
            let asked = scopes(&[wanted]);
            let strict = holder.grant_step_strict(&asked);
            let kept = wanted != "fleet" && (covers || wanted.starts_with("grant:"));
            assert_eq!(strict.is_ok(), kept, "{held} grants {wanted}: {strict:?}");
            if let Err(error) = strict {
                assert_eq!(
                    error,
                    GrantStepError::NotCovered {
                        scope: scope(wanted)
                    },
                    "{held} grants {wanted}"
                );
            }
        }
        // An empty pattern isn't a scope at all.
        for empty in ["read:", "grant:", "fork:", "admin:"] {
            assert!(empty.parse::<Scope>().is_err(), "{empty}");
        }
    }

    #[test]
    fn the_meet_of_two_patterns_is_exact() {
        for (a, b, meet) in [
            ("read:a", "read:a", Some("read:a")),
            ("read:a", "read:b", None),
            ("read:a-1", "read:a-*", Some("read:a-1")),
            ("read:a-*", "read:a-1", Some("read:a-1")),
            ("read:a1", "read:a-*", None),
            ("read:a-*", "read:a-b-*", Some("read:a-b-*")),
            ("read:a-*", "read:b-*", None),
            ("read:agent-42-*", "read:agent-42.*", None),
            ("read:agent-42*", "read:agent-42.*", Some("read:agent-42.*")),
            ("read:*", "read:x-*", Some("read:x-*")),
            ("read:x", "stream:x", None),
            ("fleet", "fleet", Some("fleet")),
            ("fleet", "admin:*", None),
        ] {
            assert_eq!(scope(a).meet(&scope(b)), meet.map(scope), "{a} ∧ {b}");
        }
    }

    /// A grant step's row: the effective scopes, the requested ones, and the result.
    type GrantStepRow = (
        &'static [&'static str],
        &'static [&'static str],
        Result<&'static [&'static str], GrantStepError>,
    );

    /// ADR-0044 point 2.4's golden vectors, each row as the ADR states it.
    #[test]
    fn the_golden_vectors_of_point_two_four_hold() {
        let intersections: &[(&[&str], &[&str], &[&str])] = &[
            (
                &["read:agent-42-*"],
                &["write:agent-42-memory"],
                &["read:agent-42-memory"],
            ),
            (
                &["admin:agent-42-*"],
                &["fork:agent-42-mem-*"],
                &["fork:agent-42-mem-*"],
            ),
            (&["read:agent-42-mem"], &["read:agent-42-memory-*"], &[]),
            (&["read:agent-42-memory-*"], &["read:agent-42-mem"], &[]),
            (
                &["read:agent-42-memory"],
                &["read:agent-42-*"],
                &["read:agent-42-memory"],
            ),
            (
                &["write:agent-42-*"],
                &["fork:agent-42-*"],
                &["read:agent-42-*"],
            ),
            (&["stream:t-*"], &["read:t-*"], &[]),
            (
                &["admin:a-*", "admin:b-*"],
                &["read:b-x-*", "read:c-*"],
                &["read:b-x-*"],
            ),
            (&["fleet", "read:a-*"], &["fleet"], &["fleet"]),
        ];
        for (effective, block, result) in intersections {
            let got = scopes(effective).intersect(&scopes(block));
            assert_eq!(texts(&got), *result, "{effective:?} ∩ {block:?}");
        }

        let steps: &[GrantStepRow] = &[
            (
                &["fleet", "admin:agent-42.*", "grant:agent-42.*"],
                &["fleet", "admin:agent-42.c7-*"],
                Ok(&["admin:agent-42.c7-*"]),
            ),
            (
                &["admin:agent-42-*", "admin:agent-42.*", "grant:agent-42.*"],
                &["admin:agent-42.c7-*", "read:agent-42-memory"],
                Ok(&["admin:agent-42.c7-*"]),
            ),
            (
                &[
                    "admin:agent-42-*",
                    "admin:agent-42.*",
                    "grant:agent-42-*",
                    "grant:agent-42.*",
                ],
                &[
                    "admin:agent-42.c7-*",
                    "fork:agent-42-memory",
                    "grant:agent-42.c7.*",
                ],
                Ok(&[
                    "admin:agent-42.c7-*",
                    "fork:agent-42-memory",
                    "grant:agent-42.c7.*",
                ]),
            ),
            (
                &["admin:agent-42.*"],
                &["admin:agent-42.c7-*"],
                Err(GrantStepError::NoGrant),
            ),
        ];
        for (effective, block, result) in steps {
            let got = scopes(effective).grant_step(&scopes(block));
            let want = result.clone().map(scopes);
            assert_eq!(got, want, "{effective:?} grant step {block:?}");
        }

        // The strict step on the same rows: the memory isn't under the `grant`, and `fleet` is
        // never handed over.
        let memory = scopes(&["admin:agent-42-*", "admin:agent-42.*", "grant:agent-42.*"])
            .grant_step_strict(&scopes(&["admin:agent-42.c7-*", "read:agent-42-memory"]));
        assert_eq!(
            memory,
            Err(GrantStepError::NotCovered {
                scope: scope("read:agent-42-memory")
            })
        );
        assert_eq!(
            memory.expect_err("refused").to_string(),
            "the scope read:agent-42-memory isn't wholly within both the scopes held and their \
             grant"
        );
        let fleet = scopes(&["fleet", "admin:agent-42.*", "grant:agent-42.*"])
            .grant_step_strict(&scopes(&["fleet", "admin:agent-42.c7-*"]));
        assert_eq!(
            fleet,
            Err(GrantStepError::NotCovered {
                scope: Scope::fleet()
            })
        );
        let whole = scopes(&[
            "admin:agent-42-*",
            "admin:agent-42.*",
            "grant:agent-42-*",
            "grant:agent-42.*",
        ]);
        let asked = scopes(&[
            "grant:agent-42.c7.*",
            "fork:agent-42-memory",
            "admin:agent-42.c7-*",
        ]);
        assert_eq!(whole.grant_step_strict(&asked), Ok(asked.normalized()));
        assert_eq!(
            scopes(&["admin:agent-42.*"]).grant_step_strict(&scopes(&["admin:agent-42.c7-*"])),
            Err(GrantStepError::NoGrant)
        );
    }

    #[test]
    fn the_grant_step_refuses_empty_results() {
        let holder = scopes(&["admin:agent-42-*", "grant:agent-42.*"]);
        assert_eq!(
            holder.grant_step(&scopes(&["read:agent-42-memory"])),
            Err(GrantStepError::Empty)
        );
        assert_eq!(
            holder.grant_step(&Scopes::none()),
            Err(GrantStepError::Empty)
        );
        assert_eq!(
            holder.grant_step_strict(&Scopes::none()),
            Err(GrantStepError::Empty)
        );
        // Asked for nothing it holds, the strict step names the scope rather than "empty".
        assert_eq!(
            holder.grant_step_strict(&scopes(&["read:agent-42-memory"])),
            Err(GrantStepError::NotCovered {
                scope: scope("read:agent-42-memory")
            })
        );
        // A `grant` alone gives nothing to hand over: it narrows, it doesn't authorize.
        assert_eq!(
            scopes(&["grant:x-*"]).grant_step(&scopes(&["read:x-1"])),
            Err(GrantStepError::Empty)
        );
        // `admin` doesn't imply `grant`.
        assert_eq!(
            scopes(&["admin:*"]).grant_step(&scopes(&["read:x"])),
            Err(GrantStepError::NoGrant)
        );
    }

    #[test]
    fn normalizing_drops_covered_scopes_and_sorts_by_text() {
        let messy = scopes(&[
            "read:b-1",
            "write:b-*",
            "fleet",
            "admin:a",
            "read:b-1",
            "fork:a",
            "stream:a",
            "grant:z-*",
            "grant:z-1-*",
        ]);
        assert_eq!(
            texts(&messy.normalized()),
            ["admin:a", "fleet", "grant:z-*", "stream:a", "write:b-*"]
        );
        assert_eq!(Scopes::none().normalized(), Scopes::none());
    }

    /// A fixed-seed generator (SplitMix64), so the property tests below never flake.
    struct Generator(u64);

    impl Generator {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: usize) -> usize {
            let n = u64::try_from(n).expect("a small bound");
            usize::try_from(self.next() % n).expect("below a usize bound")
        }

        fn pick<T: Copy>(&mut self, items: &[T]) -> T {
            items[self.below(items.len())]
        }

        /// A short string over few bytes, so that patterns overlap often: separators, digits,
        /// a letter, and `*`.
        fn text(&mut self, max: usize) -> String {
            const BYTES: &[char] = &['a', '4', '2', '-', '.', '_', '*'];
            let len = self.below(max + 1);
            (0..len).map(|_| self.pick(BYTES)).collect()
        }

        fn scope(&mut self) -> Scope {
            if self.below(10) == 0 {
                return Scope::fleet();
            }
            let action = self.pick(&Action::ALL);
            let body = self.text(4);
            let pattern = if body.is_empty() || self.below(2) == 0 {
                format!("{body}*")
            } else {
                body
            };
            scope(&format!("{action}:{pattern}"))
        }

        fn scopes(&mut self, max: usize) -> Scopes {
            let len = self.below(max + 1);
            (0..len).map(|_| self.scope()).collect()
        }

        /// Names to test the scopes against: random ones, and each pattern's own bytes with a
        /// little after them, where a careless comparison would go wrong.
        fn names(&mut self, sets: &[&Scopes]) -> Vec<DatabaseName> {
            let mut names: Vec<String> = (0..12).map(|_| self.text(6)).collect();
            for set in sets {
                for held in set.as_slice() {
                    let Rule::Databases { pattern, .. } = &held.rule else {
                        continue;
                    };
                    let bytes = match pattern.pattern() {
                        Pattern::Exact(bytes) | Pattern::Prefix(bytes) => bytes,
                    };
                    for suffix in ["", "0", "-", ".", "4", "*"] {
                        names.push(format!("{bytes}{suffix}"));
                    }
                }
            }
            names.iter().filter_map(|name| name.parse().ok()).collect()
        }
    }

    /// Whether `scopes` hold `action` on `name` in the lattice's meaning, `grant` included.
    fn holds(scopes: &Scopes, action: Action, name: &DatabaseName) -> bool {
        scopes.as_slice().iter().any(|held| match &held.rule {
            Rule::Databases {
                action: held,
                pattern,
            } => held.covers(action) && pattern.matches(name),
            Rule::Fleet => false,
        })
    }

    fn holds_fleet(scopes: &Scopes) -> bool {
        scopes.as_slice().contains(&Scope::fleet())
    }

    fn assert_canonical(scopes: &Scopes) {
        let all = scopes.as_slice();
        for (i, a) in all.iter().enumerate() {
            for (j, b) in all.iter().enumerate() {
                assert!(i == j || !a.covers(b), "{a} covers {b} in {scopes:?}");
            }
        }
        let text = texts(scopes);
        let mut sorted = text.clone();
        sorted.sort_unstable();
        assert_eq!(text, sorted);
    }

    /// The number of generated cases, a few thousand, well under a second in debug.
    const CASES: usize = 3_000;

    #[test]
    fn covering_is_sound_on_generated_scopes() {
        let mut generator = Generator(0x5eed_0001);
        for _ in 0..CASES {
            let held = generator.scope();
            let wanted = generator.scope();
            let held_set = Scopes::new([held.clone()]);
            let wanted_set = Scopes::new([wanted.clone()]);
            if held.covers(&wanted) {
                assert!(!holds_fleet(&wanted_set) || holds_fleet(&held_set));
                for name in generator.names(&[&held_set, &wanted_set]) {
                    for action in Action::ALL {
                        assert!(
                            !holds(&wanted_set, action, &name) || holds(&held_set, action, &name),
                            "{held} covers {wanted}, but not {action} on {name}"
                        );
                    }
                }
            }
            // A scope covers itself, and two that cover each other are the same.
            assert!(held.covers(&held));
            if held.covers(&wanted) && wanted.covers(&held) {
                assert_eq!(held, wanted);
            }
        }
    }

    #[test]
    fn intersect_is_exact_commutative_idempotent_and_never_wider() {
        let mut generator = Generator(0x5eed_0002);
        for _ in 0..CASES {
            let a = generator.scopes(4);
            let b = generator.scopes(4);
            let both = a.intersect(&b);
            assert_canonical(&both);
            assert_eq!(both, b.intersect(&a), "{a:?} ∩ {b:?} commutes");
            assert_eq!(a.intersect(&a), a.normalized(), "{a:?} ∩ itself");
            assert_eq!(both.intersect(&b), both, "{a:?} ∩ {b:?} twice");
            for scope in both.as_slice() {
                assert!(
                    a.as_slice().iter().any(|held| held.covers(scope)),
                    "{scope}"
                );
                assert!(
                    b.as_slice().iter().any(|held| held.covers(scope)),
                    "{scope}"
                );
            }
            assert_eq!(holds_fleet(&both), holds_fleet(&a) && holds_fleet(&b));
            for name in generator.names(&[&a, &b]) {
                for action in Action::ALL {
                    assert_eq!(
                        holds(&both, action, &name),
                        holds(&a, action, &name) && holds(&b, action, &name),
                        "{a:?} ∩ {b:?} = {both:?}, {action} on {name}"
                    );
                }
            }
            let normal = a.normalized();
            assert_canonical(&normal);
            assert_eq!(normal.normalized(), normal);
            for name in generator.names(&[&a]) {
                for action in Action::ALL {
                    assert_eq!(holds(&normal, action, &name), holds(&a, action, &name));
                }
            }
        }
    }

    #[test]
    fn the_grant_step_stays_within_the_scopes_and_the_grant() {
        let mut generator = Generator(0x5eed_0003);
        let mut kept = 0;
        let mut strict_kept = 0;
        for _ in 0..CASES {
            let mut effective = generator.scopes(4);
            if generator.below(4) != 0 {
                effective.extend([generator.scope()].into_iter().map(|scope| {
                    let pattern = match &scope.rule {
                        Rule::Databases { pattern, .. } => pattern.clone(),
                        Rule::Fleet => "*".parse().expect("a pattern"),
                    };
                    Scope::new(Action::Grant, pattern)
                }));
            }
            let requested = generator.scopes(4);
            let grants = Scopes::new(
                effective
                    .as_slice()
                    .iter()
                    .filter(|scope| scope.action() == Some(Action::Grant))
                    .cloned(),
            );
            let step = effective.grant_step(&requested);
            match &step {
                Err(GrantStepError::NoGrant) => assert!(grants.is_empty()),
                Err(GrantStepError::Empty) => {
                    assert!(!grants.is_empty());
                }
                Err(error) => panic!("the narrowing step refused with {error}"),
                Ok(result) => {
                    kept += 1;
                    assert_canonical(result);
                    assert!(!result.is_empty());
                    assert!(!holds_fleet(result), "fleet is never handed over");
                    for scope in result.as_slice() {
                        let Rule::Databases { pattern, .. } = &scope.rule else {
                            panic!("{scope}");
                        };
                        let as_grant = Scope::new(Action::Grant, pattern.clone());
                        assert!(
                            effective.as_slice().iter().any(|held| held.covers(scope)),
                            "{scope} isn't within {effective:?}"
                        );
                        assert!(
                            grants
                                .as_slice()
                                .iter()
                                .any(|grant| grant.covers(&as_grant)),
                            "{scope} isn't within the grant of {effective:?}"
                        );
                        assert!(
                            requested.as_slice().iter().any(|asked| asked.covers(scope)),
                            "{scope} wasn't requested in {requested:?}"
                        );
                    }
                }
            }
            // Exactly what the scopes, the request, and some grant pattern all hold.
            if let Ok(result) = &step {
                for name in generator.names(&[&effective, &requested]) {
                    let granted = holds(&grants, Action::Grant, &name);
                    for action in Action::ALL {
                        assert_eq!(
                            holds(result, action, &name),
                            granted
                                && holds(&effective, action, &name)
                                && holds(&requested, action, &name),
                            "{effective:?} grant step {requested:?}: {action} on {name}"
                        );
                    }
                }
            }
            // The strict step returns exactly the request, normalized, or refuses.
            match effective.grant_step_strict(&requested) {
                Ok(exact) => {
                    strict_kept += 1;
                    assert_eq!(exact, requested.normalized());
                    assert_eq!(Ok(exact), step, "the strict step agrees when it keeps all");
                }
                Err(GrantStepError::NotCovered { scope }) => {
                    assert!(requested.normalized().as_slice().contains(&scope));
                    let narrowed = step.clone().unwrap_or_default();
                    assert!(
                        !narrowed.as_slice().iter().any(|kept| kept.covers(&scope)),
                        "{scope} was refused but kept whole"
                    );
                }
                Err(GrantStepError::NoGrant) => assert!(grants.is_empty()),
                Err(GrantStepError::Empty) => assert!(requested.normalized().is_empty()),
            }
        }
        // The generator reaches every outcome often enough to mean something.
        assert!(kept > CASES / 20, "only {kept} grant steps kept anything");
        assert!(
            strict_kept > CASES / 50,
            "only {strict_kept} strict steps kept all"
        );
    }
}
