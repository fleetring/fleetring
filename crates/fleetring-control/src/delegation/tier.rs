//! The agent tier: the only scopes a delegation issuer may sign (ADR-0047, point 3.4 to 3.7).
//!
//! An issuer-signed token names a delegated agent, `agent-<root>.<…>`, and every scope it holds
//! must stay inside its root's namespace, with any `grant` inside the agent's own children. The
//! checks are coverage in the scope lattice ([`Scope::covers`]), not text comparisons, so they
//! can't disagree with the grant step that narrows scopes, and the separator rule is the one
//! [`Scope::separator_warning`] states, applied as a refusal (ADR-0044, point 2.3).
//!
//! Nodes, the signer, and [`apply`](crate::apply) run [`check`] through
//! [`TokenClaims::check_issuer_signed`](super::TokenClaims::check_issuer_signed), on every
//! issuer-signed token and every delegation signed or recorded, whoever proposed it.

use serde::{Deserialize, Serialize};

use crate::matcher::DatabaseMatcher;
use crate::scope::{Action, Scope, Scopes};

use super::AgentName;

/// Checks that `scopes` lie in the agent tier for the delegated agent `child`, naming the first
/// scope, in the order given, that doesn't.
///
/// `child` must be a delegated agent name (point 3.1): a root agent's name has no agent tier.
/// Then each scope, in turn, must:
///
/// 1. not be `fleet` (point 3.4);
/// 2. be covered by `<action>:agent-<root>-*` or `<action>:agent-<root>.*`, where
///    `agent-<root>` is `child`'s root (point 3.5);
/// 3. if it is a `grant`, be covered by `grant:<child>.*` (point 3.6);
/// 4. end any prefix, before its `*`, in a byte that isn't an ASCII letter or digit (point 3.7).
///
/// An exact name passes the separator rule. An empty set passes; whether a token may hold no
/// scope is decided elsewhere.
///
/// # Examples
///
/// ```
/// use fleetring_control::delegation::{tier, AgentName};
/// use fleetring_control::scope::Scopes;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let child: AgentName = "agent-42.c7".parse()?;
/// let parse = |texts: &[&str]| texts.iter().map(|s| s.parse()).collect::<Result<Scopes, _>>();
/// assert_eq!(tier::check(&child, &parse(&["admin:agent-42.c7-*", "grant:agent-42.c7.*"])?), Ok(()));
/// assert_eq!(
///     tier::check(&child, &parse(&["admin:agent-420-*"])?),
///     Err(tier::TierViolation::OutsideRoot { scope: "admin:agent-420-*".parse()? })
/// );
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// [`TierViolation::NotDelegated`] if `child` is a root agent's name; otherwise the
/// [`TierViolation`] of the first scope that breaks a rule, the first rule it breaks in the order
/// above.
pub fn check(child: &AgentName, scopes: &Scopes) -> Result<(), TierViolation> {
    if !child.is_delegated() {
        return Err(TierViolation::NotDelegated);
    }
    let root = child.root();
    let under_root = [
        DatabaseMatcher::from_prefix(format!("{root}-")),
        DatabaseMatcher::from_prefix(format!("{root}.")),
    ];
    let grant = Scope::new(
        Action::Grant,
        DatabaseMatcher::from_prefix(format!("{child}.")),
    );
    for scope in scopes.as_slice() {
        let Some(action) = scope.action() else {
            return Err(TierViolation::Fleet);
        };
        if !under_root
            .iter()
            .any(|pattern| Scope::new(action, pattern.clone()).covers(scope))
        {
            return Err(TierViolation::OutsideRoot {
                scope: scope.clone(),
            });
        }
        if action == Action::Grant && !grant.covers(scope) {
            return Err(TierViolation::GrantOutsideChild {
                scope: scope.clone(),
            });
        }
        if scope.separator_warning().is_some() {
            return Err(TierViolation::NoSeparator {
                scope: scope.clone(),
            });
        }
    }
    Ok(())
}

/// Why a set of scopes isn't in the agent tier for a delegated agent (ADR-0047, point 3.4 to
/// 3.7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TierViolation {
    /// The name the scopes were checked for is a root agent's, which no issuer signs for.
    #[error("only a delegated agent's token is in the agent tier, and a root agent isn't one")]
    NotDelegated,

    /// The scopes hold `fleet`.
    #[error("a delegated agent's token can't hold the fleet scope")]
    Fleet,

    /// The scope reaches beyond the root's namespace, `agent-<root>-` and `agent-<root>.`.
    #[error(
        "the scope {scope} isn't within its root agent's databases, agent-<root>- or \
         agent-<root>."
    )]
    OutsideRoot {
        /// The scope.
        scope: Scope,
    },

    /// A `grant` scope reaches beyond the delegated agent's own children, `<child>.`.
    #[error("the scope {scope} grants beyond the delegated agent's own children, <child>.")]
    GrantOutsideChild {
        /// The scope.
        scope: Scope,
    },

    /// The scope's prefix ends in an ASCII letter or digit before its `*`, so it would also
    /// cover names it doesn't mean to, such as `agent-42.c70…` for `agent-42.c7*`.
    #[error(
        "the scope {scope} ends its prefix in a letter or digit; end it with a separator, such \
         as '-' or '.', before the '*'"
    )]
    NoSeparator {
        /// The scope.
        scope: Scope,
    },
}
