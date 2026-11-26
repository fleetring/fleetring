//! The group gate: the feature gate and the release window over the control group's members as
//! well as the fleet's (ADR-0040 point 4).
//!
//! A one-voter group's only member is the leader, which runs this build and so supports every
//! feature it can log, so the fleet's own gate in `State::check_admission` decides alone. The
//! gate over the group's other members, with the software their Raft messages carry, comes with
//! the `Raft` service.

use fleetring_control::{Action, Feature, Rejection};

use crate::control::screen::Gate;

/// The group gate of a control node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupGate;

impl GroupGate {
    /// Returns the gate of a group whose only member is this node.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Whether every member of the group supports `feature`, so that its first use may be
    /// logged.
    #[must_use]
    pub fn supports(&self, feature: Feature) -> bool {
        self.check_features(&[feature]).is_ok()
    }

    fn check_features(&self, _features: &[Feature]) -> Result<(), Rejection> {
        Ok(())
    }
}

impl Gate for GroupGate {
    fn check(&self, action: &Action) -> Result<(), Rejection> {
        self.check_features(action.features())
    }
}
