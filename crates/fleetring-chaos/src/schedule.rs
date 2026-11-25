//! Seeded fault schedules: which faults a run injects, when, and on which nodes (ADR-0036,
//! point 4).

/// A run's scenario: the fault kinds its schedule draws from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, clap::ValueEnum)]
pub enum Scenario {
    /// The four partition shapes.
    Partitions,
    /// Kills and restarts.
    Kills,
    /// Pauses and continues.
    Pauses,
    /// Clock skew and strobe on the control node.
    Clock,
    /// Every fault kind.
    Mixed,
}

impl Scenario {
    /// Returns the scenario's name as the command line writes it.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Partitions => "partitions",
            Self::Kills => "kills",
            Self::Pauses => "pauses",
            Self::Clock => "clock",
            Self::Mixed => "mixed",
        }
    }
}
