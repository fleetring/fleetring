//! Deliberate bugs for FR-087's mutation check: the fork simulation matrix runs once with each,
//! and a history-checker property must fail every time, or the matrix has a gap.
//!
//! A mutation is chosen at compile time with the `FLEETRING_MUTATION` environment variable,
//! which `scripts/fork-mutations.sh` sets to one [`Mutation`]'s name. It can never reach a
//! build that ships: a build without debug assertions, such as `--release`, refuses to compile
//! with the variable set, and [`Mutation::is_active`] is `false` in every such build whatever
//! it says. Without the variable, every check is `false` and the code behaves as designed.

/// The mutation this build carries, if `FLEETRING_MUTATION` was set when it was compiled.
const CHOSEN: Option<&str> = option_env!("FLEETRING_MUTATION");

// A mutation never reaches an optimized build, which is what ships.
#[cfg(not(debug_assertions))]
const _: () = assert!(
    CHOSEN.is_none(),
    "FLEETRING_MUTATION is for the fork mutation check only and can't be built without debug \
     assertions: unset it for a release build"
);

/// One deliberate bug of the fork mutation check (FR-087, ADR-0046 point 12).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Mutation {
    /// The source's primary pins its published tip, not the position its write quorum
    /// acknowledged.
    PinAtTip,
    /// A fork completes on one holder: the completion quorum is 1.
    CompleteOnOneHolder,
    /// A seeded fork's first lease goes to any member that asks, holder or not.
    NonHolderFirstLease,
    /// Nothing compares a fork's source incarnation: not the binding, the source's open copy,
    /// the seeder's stamp check, `CompleteFork`, nor the driver's watch for a recreated source.
    SkipSourceIncarnation,
    /// `apply` finds a fork by its name alone, whatever incarnation and operation a command
    /// names.
    SkipTargetIdentityApply,
    /// The `seeding` binding finds a fork by its name alone, whatever incarnation and
    /// operation a request names.
    SkipTargetIdentityBinding,
    /// Every new primary of a fork runs the fork-reset write, marker or not.
    UnconditionalReset,
    /// A fork's primary serves clients as soon as it holds the lease, before its reset commits.
    ServeAtSeeded,
    /// Disk admission compares the free space alone, without the other copies' reservations.
    DiskWithoutReservations,
}

impl Mutation {
    /// Every mutation, in the order the mutation check runs them.
    pub const ALL: [Self; 9] = [
        Self::PinAtTip,
        Self::CompleteOnOneHolder,
        Self::NonHolderFirstLease,
        Self::SkipSourceIncarnation,
        Self::SkipTargetIdentityApply,
        Self::SkipTargetIdentityBinding,
        Self::UnconditionalReset,
        Self::ServeAtSeeded,
        Self::DiskWithoutReservations,
    ];

    /// The name `FLEETRING_MUTATION` gives it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::PinAtTip => "pin_at_tip",
            Self::CompleteOnOneHolder => "complete_on_one_holder",
            Self::NonHolderFirstLease => "non_holder_first_lease",
            Self::SkipSourceIncarnation => "skip_source_incarnation",
            Self::SkipTargetIdentityApply => "skip_target_identity_apply",
            Self::SkipTargetIdentityBinding => "skip_target_identity_binding",
            Self::UnconditionalReset => "unconditional_reset",
            Self::ServeAtSeeded => "serve_at_seeded",
            Self::DiskWithoutReservations => "disk_without_reservations",
        }
    }

    /// Whether this build carries the mutation: never in a build without debug assertions.
    #[must_use]
    pub fn is_active(self) -> bool {
        cfg!(debug_assertions) && CHOSEN == Some(self.name())
    }

    /// The mutation this build carries, if any.
    #[must_use]
    pub fn active() -> Option<Self> {
        Self::ALL.into_iter().find(|mutation| mutation.is_active())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_unique() {
        let mut names: Vec<&str> = Mutation::ALL.iter().map(|m| m.name()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), Mutation::ALL.len());
    }

    #[test]
    fn an_unset_variable_carries_none() {
        if CHOSEN.is_none() {
            assert_eq!(Mutation::active(), None);
        }
    }
}
