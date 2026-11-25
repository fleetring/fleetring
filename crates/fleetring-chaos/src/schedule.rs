//! Seeded fault schedules: which faults a run injects, when, and on which nodes (ADR-0036,
//! point 4).
//!
//! Every choice comes from [`SimRng`] seeded with the run's seed, so a seed always gives the
//! same schedule. Only [`Fault::IsolatePrimary`] resolves its target when it starts, from the
//! database's primary at that moment.

use std::fmt;
use std::time::Duration;

use fleetringd::sim::SimRng;

use crate::Mode;
use crate::node::{FLEET, Node};

/// When the first fault may start, after the workload starts.
pub const FIRST_FAULT: Duration = Duration::from_secs(10);
/// How often a clock strobe flips the control node's offset.
pub const STROBE_PERIOD: Duration = Duration::from_millis(500);
/// The largest clock offset, in seconds, either way.
pub const MAX_CLOCK_OFFSET_S: u64 = 60;
/// How many databases the workload writes to, and so how many a primary isolation picks from.
pub const DATABASES: usize = 8;

/// The shortest quiet period between two faults, in milliseconds.
const QUIET_MIN_MS: u64 = 5_000;
/// How much longer than [`QUIET_MIN_MS`] a quiet period may last, in milliseconds.
const QUIET_SPREAD_MS: u64 = 5_000;
/// The shortest fault, in milliseconds.
const FAULT_MIN_MS: u64 = 5_000;
/// How much longer than [`FAULT_MIN_MS`] a fault may last, in milliseconds.
const FAULT_SPREAD_MS: u64 = 15_000;
/// The shortest primary isolation of a self-test, in milliseconds: long enough for the lease to
/// expire and a follower to be promoted.
const SELF_TEST_MIN_MS: u64 = 15_000;
/// How much longer than [`SELF_TEST_MIN_MS`] a self-test's isolation may last, in milliseconds.
const SELF_TEST_SPREAD_MS: u64 = 5_000;

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
    /// Every scenario.
    pub const ALL: [Scenario; 5] = [
        Scenario::Partitions,
        Scenario::Kills,
        Scenario::Pauses,
        Scenario::Clock,
        Scenario::Mixed,
    ];

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

    /// Returns the fault kinds the scenario draws from, in [`FaultKind`] order. A self-test
    /// draws only primary isolations, whatever the scenario.
    #[must_use]
    pub fn kinds(self, mode: Mode) -> &'static [FaultKind] {
        use FaultKind::*;
        if mode == Mode::SelfTest {
            return &[IsolatePrimary];
        }
        match self {
            Self::Partitions => &[PartitionHalves, IsolatePrimary, IsolateControl, Bridge],
            Self::Kills => &[Kill],
            Self::Pauses => &[Pause],
            Self::Clock => &[ClockSkew, ClockStrobe],
            Self::Mixed => &FaultKind::ALL,
        }
    }
}

impl fmt::Display for Scenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A kind of fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultKind {
    /// Two random nodes cut from the other three.
    PartitionHalves,
    /// A database's current primary cut from every other node.
    IsolatePrimary,
    /// The control node cut from every data node.
    IsolateControl,
    /// Two halves of two nodes each, and a fifth node that reaches both.
    Bridge,
    /// A node killed with `SIGKILL`, and started again at the fault's end.
    Kill,
    /// A node stopped with `SIGSTOP`, and continued at the fault's end.
    Pause,
    /// The control node's wall clock held off by an offset.
    ClockSkew,
    /// The control node's wall clock flipped between two offsets.
    ClockStrobe,
}

impl FaultKind {
    /// Every kind, in order.
    pub const ALL: [FaultKind; 8] = [
        FaultKind::PartitionHalves,
        FaultKind::IsolatePrimary,
        FaultKind::IsolateControl,
        FaultKind::Bridge,
        FaultKind::Kill,
        FaultKind::Pause,
        FaultKind::ClockSkew,
        FaultKind::ClockStrobe,
    ];

    /// Returns the kind's name as `schedule.txt` prints it, in kebab case.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::PartitionHalves => "partition-halves",
            Self::IsolatePrimary => "isolate-primary",
            Self::IsolateControl => "isolate-control",
            Self::Bridge => "bridge",
            Self::Kill => "kill",
            Self::Pause => "pause",
            Self::ClockSkew => "clock-skew",
            Self::ClockStrobe => "clock-strobe",
        }
    }
}

impl fmt::Display for FaultKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A fault, with every random choice already made. Only `IsolatePrimary` resolves its target
/// when it starts, from the database's current primary.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Fault {
    /// Drops traffic between every pair in `cut`, both ways.
    Partition {
        /// [`FaultKind::PartitionHalves`], [`FaultKind::IsolateControl`], or
        /// [`FaultKind::Bridge`].
        kind: FaultKind,
        /// The groups the partition makes, each sorted: two sides, or for a bridge, one side,
        /// the bridge, and the other side.
        sides: Vec<Vec<Node>>,
        /// Every pair of nodes that can't reach each other, each pair ordered and the list
        /// sorted.
        cut: Vec<(Node, Node)>,
    },
    /// Isolates the primary of the database with this index (0..8) from every other node.
    IsolatePrimary {
        /// The database's index.
        database: usize,
    },
    /// Kills a node's process, and starts its container again at the end.
    Kill {
        /// The node.
        node: Node,
    },
    /// Stops a node's process, and continues it at the end.
    Pause {
        /// The node.
        node: Node,
    },
    /// Holds the control node's wall clock `offset_s` seconds off.
    ClockSkew {
        /// The offset, in seconds: nonzero, within ±60.
        offset_s: i64,
    },
    /// Flips the control node's wall clock between `+offset_s` and `-offset_s` every 500 ms.
    ClockStrobe {
        /// The offset's magnitude, in seconds: 1 to 60.
        offset_s: i64,
    },
}

impl Fault {
    /// Returns the fault's kind.
    #[must_use]
    pub fn kind(&self) -> FaultKind {
        match self {
            Self::Partition { kind, .. } => *kind,
            Self::IsolatePrimary { .. } => FaultKind::IsolatePrimary,
            Self::Kill { .. } => FaultKind::Kill,
            Self::Pause { .. } => FaultKind::Pause,
            Self::ClockSkew { .. } => FaultKind::ClockSkew,
            Self::ClockStrobe { .. } => FaultKind::ClockStrobe,
        }
    }

    /// Returns the fault's targets as `schedule.txt` prints them, before a primary isolation
    /// resolves its primary.
    #[must_use]
    pub fn targets(&self) -> String {
        match self {
            Self::Partition { sides, .. } => sides
                .iter()
                .map(|side| {
                    side.iter()
                        .map(|node| node.name())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect::<Vec<_>>()
                .join(" | "),
            Self::IsolatePrimary { database } => database_label(*database),
            Self::Kill { node } | Self::Pause { node } => node.name().to_owned(),
            Self::ClockSkew { offset_s } => {
                format!(
                    "{} {}",
                    Node::CONTROL,
                    crate::config::faketime_offset(*offset_s)
                )
            }
            Self::ClockStrobe { offset_s } => format!("{} ±{offset_s}s", Node::CONTROL),
        }
    }
}

/// The name of the database with index `index`, as the workload creates it: `chaos/db-<index>`.
#[must_use]
pub fn database_label(index: usize) -> String {
    format!("{FLEET}/db-{index}")
}

/// A fault and when it runs, counted from the workload's start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedFault {
    /// When it starts.
    pub start: Duration,
    /// When it ends.
    pub end: Duration,
    /// What it does.
    pub fault: Fault,
}

impl PlannedFault {
    /// Returns the line `schedule.txt` prints for the fault with `targets`: its start and end in
    /// milliseconds, its kind, and the targets.
    #[must_use]
    pub fn line(&self, targets: &str) -> String {
        format!(
            "{:>8}ms {:>8}ms {} {targets}",
            self.start.as_millis(),
            self.end.as_millis(),
            self.fault.kind()
        )
    }
}

impl fmt::Display for PlannedFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.line(&self.fault.targets()))
    }
}

/// A run's faults, in time order, as its seed decides them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    /// The scenario the faults come from.
    pub scenario: Scenario,
    /// The seed.
    pub seed: u64,
    /// The faults, in start order; none overlap.
    pub faults: Vec<PlannedFault>,
}

impl Schedule {
    /// Returns the faults of a run of `scenario` whose fault phase lasts `time_limit`: quiet
    /// periods of 5 to 10 s alternate with faults of 5 to 20 s (15 to 20 s in a self-test), from
    /// [`FIRST_FAULT`] until the next fault would end after `time_limit`.
    #[must_use]
    pub fn generate(scenario: Scenario, seed: u64, time_limit: Duration, mode: Mode) -> Self {
        let mut root = SimRng::new(seed);
        let mut rng = root.fork();
        let kinds = scenario.kinds(mode);
        let mut faults = Vec::new();
        let mut t = FIRST_FAULT;
        loop {
            let quiet = QUIET_MIN_MS + rng.below(QUIET_SPREAD_MS + 1);
            let length = match mode {
                Mode::SelfTest => SELF_TEST_MIN_MS + rng.below(SELF_TEST_SPREAD_MS + 1),
                Mode::Normal => FAULT_MIN_MS + rng.below(FAULT_SPREAD_MS + 1),
            };
            let start = t + Duration::from_millis(quiet);
            let end = start + Duration::from_millis(length);
            if end > time_limit {
                break;
            }
            let kind = kinds[rng.index(kinds.len())];
            let fault = resolve(kind, &mut rng);
            faults.push(PlannedFault { start, end, fault });
            t = end;
        }
        Self {
            scenario,
            seed,
            faults,
        }
    }
}

/// Returns the generator the workload forks its clients from: the seed's root generator, after
/// the fork [`Schedule::generate`] takes, so the schedule and the workload never share a stream.
#[must_use]
pub fn workload_rng(seed: u64) -> SimRng {
    let mut root = SimRng::new(seed);
    let _schedule = root.fork();
    root
}

/// Makes every random choice of a fault of `kind`.
fn resolve(kind: FaultKind, rng: &mut SimRng) -> Fault {
    match kind {
        FaultKind::PartitionHalves => {
            let nodes = shuffled(rng);
            partition(kind, vec![nodes[..2].to_vec(), nodes[2..].to_vec()])
        }
        FaultKind::Bridge => {
            let nodes = shuffled(rng);
            partition(
                kind,
                vec![nodes[..2].to_vec(), vec![nodes[2]], nodes[3..].to_vec()],
            )
        }
        FaultKind::IsolateControl => {
            partition(kind, vec![vec![Node::CONTROL], Node::ALL[1..].to_vec()])
        }
        FaultKind::IsolatePrimary => Fault::IsolatePrimary {
            database: rng.index(DATABASES),
        },
        FaultKind::Kill => Fault::Kill {
            node: Node::ALL[rng.index(Node::ALL.len())],
        },
        FaultKind::Pause => Fault::Pause {
            node: Node::ALL[rng.index(Node::ALL.len())],
        },
        FaultKind::ClockSkew | FaultKind::ClockStrobe => {
            let magnitude = 1 + rng.below(MAX_CLOCK_OFFSET_S);
            let magnitude = i64::try_from(magnitude).unwrap_or(1);
            if kind == FaultKind::ClockStrobe {
                // The draw keeps the generator in step with a skew's.
                let _negative = rng.chance(1, 2);
                Fault::ClockStrobe {
                    offset_s: magnitude,
                }
            } else {
                let offset_s = if rng.chance(1, 2) {
                    -magnitude
                } else {
                    magnitude
                };
                Fault::ClockSkew { offset_s }
            }
        }
    }
}

/// Every node, shuffled with Fisher–Yates.
fn shuffled(rng: &mut SimRng) -> [Node; 5] {
    let mut nodes = Node::ALL;
    for i in (1..nodes.len()).rev() {
        let j = rng.index(i + 1);
        nodes.swap(i, j);
    }
    nodes
}

/// A partition into `sides`: for a bridge, the middle side reaches both others, so only the
/// first and last sides are cut from each other; otherwise every pair across two sides is cut.
fn partition(kind: FaultKind, mut sides: Vec<Vec<Node>>) -> Fault {
    for side in &mut sides {
        side.sort_unstable();
    }
    let cut_sides: Vec<&Vec<Node>> = if kind == FaultKind::Bridge {
        vec![&sides[0], &sides[2]]
    } else {
        sides.iter().collect()
    };
    let mut cut = Vec::new();
    for (index, side) in cut_sides.iter().enumerate() {
        for other in &cut_sides[index + 1..] {
            for &a in side.iter() {
                for &b in other.iter() {
                    cut.push((a.min(b), a.max(b)));
                }
            }
        }
    }
    cut.sort_unstable();
    Fault::Partition { kind, sides, cut }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    const TEN_MINUTES: Duration = Duration::from_secs(600);

    #[test]
    fn a_seed_gives_the_same_schedule() {
        let first = Schedule::generate(Scenario::Mixed, 7, TEN_MINUTES, Mode::Normal);
        let second = Schedule::generate(Scenario::Mixed, 7, TEN_MINUTES, Mode::Normal);
        assert_eq!(first, second);
        assert!(!first.faults.is_empty());
        let other = Schedule::generate(Scenario::Mixed, 8, TEN_MINUTES, Mode::Normal);
        assert_ne!(first.faults, other.faults);
    }

    #[test]
    fn faults_stay_inside_the_window_and_never_overlap() {
        for seed in 0..200 {
            for scenario in Scenario::ALL {
                let schedule = Schedule::generate(scenario, seed, TEN_MINUTES, Mode::Normal);
                let mut previous_end = FIRST_FAULT;
                for planned in &schedule.faults {
                    assert!(planned.start >= FIRST_FAULT, "{seed} {planned}");
                    assert!(planned.start < planned.end, "{seed} {planned}");
                    assert!(planned.end <= TEN_MINUTES, "{seed} {planned}");
                    assert!(
                        planned.start >= previous_end + Duration::from_secs(5),
                        "{seed} {planned}"
                    );
                    let length = planned.end - planned.start;
                    assert!(
                        length >= Duration::from_secs(5) && length <= Duration::from_secs(20),
                        "{seed} {planned}"
                    );
                    previous_end = planned.end;
                }
            }
        }
    }

    #[test]
    fn each_scenario_uses_only_its_fault_kinds() {
        for seed in 0..200 {
            for scenario in Scenario::ALL {
                let schedule = Schedule::generate(scenario, seed, TEN_MINUTES, Mode::Normal);
                for planned in &schedule.faults {
                    assert!(
                        scenario.kinds(Mode::Normal).contains(&planned.fault.kind()),
                        "{scenario} {seed} {planned}"
                    );
                }
            }
        }
    }

    #[test]
    fn mixed_uses_every_kind() {
        let kinds: BTreeSet<FaultKind> = (0..100)
            .flat_map(|seed| {
                Schedule::generate(Scenario::Mixed, seed, TEN_MINUTES, Mode::Normal).faults
            })
            .map(|planned| planned.fault.kind())
            .collect();
        assert_eq!(kinds, FaultKind::ALL.into_iter().collect());
    }

    #[test]
    fn the_self_test_isolates_primaries_long_enough_to_fail_over() {
        for seed in 0..200 {
            for scenario in Scenario::ALL {
                let schedule = Schedule::generate(scenario, seed, TEN_MINUTES, Mode::SelfTest);
                assert!(schedule.faults.len() >= 20, "{scenario} {seed}");
                for planned in &schedule.faults {
                    assert!(
                        matches!(planned.fault, Fault::IsolatePrimary { database } if database < DATABASES),
                        "{planned}"
                    );
                    let length = planned.end - planned.start;
                    assert!(
                        length >= Duration::from_secs(15) && length <= Duration::from_secs(20),
                        "{planned}"
                    );
                }
            }
        }
    }

    #[test]
    fn partitions_cut_only_across_their_sides() {
        for seed in 0..200 {
            let schedule =
                Schedule::generate(Scenario::Partitions, seed, TEN_MINUTES, Mode::Normal);
            for planned in &schedule.faults {
                let Fault::Partition { kind, sides, cut } = &planned.fault else {
                    continue;
                };
                let side_of = |node: Node| sides.iter().position(|side| side.contains(&node));
                let all: BTreeSet<Node> = sides.iter().flatten().copied().collect();
                assert_eq!(all.len(), 5, "{planned}");
                for &(a, b) in cut {
                    assert!(a < b, "{planned}");
                    assert_ne!(side_of(a), side_of(b), "{planned}");
                }
                match kind {
                    FaultKind::PartitionHalves => {
                        assert_eq!(cut.len(), 6, "{planned}");
                        assert_eq!(sides[0].len(), 2, "{planned}");
                    }
                    FaultKind::Bridge => {
                        assert_eq!(cut.len(), 4, "{planned}");
                        let bridge = sides[1][0];
                        assert!(
                            cut.iter().all(|&(a, b)| a != bridge && b != bridge),
                            "{planned}"
                        );
                    }
                    FaultKind::IsolateControl => {
                        assert_eq!(cut.len(), 4, "{planned}");
                        assert!(cut.iter().all(|&(a, _)| a == Node::CONTROL), "{planned}");
                    }
                    other => panic!("a partition of kind {other}"),
                }
            }
        }
    }

    #[test]
    fn clock_offsets_stay_within_a_minute() {
        for seed in 0..200 {
            let schedule = Schedule::generate(Scenario::Clock, seed, TEN_MINUTES, Mode::Normal);
            for planned in &schedule.faults {
                let (Fault::ClockSkew { offset_s } | Fault::ClockStrobe { offset_s }) =
                    planned.fault
                else {
                    panic!("{planned}");
                };
                assert!(offset_s != 0 && offset_s.abs() <= 60, "{planned}");
                if let Fault::ClockStrobe { offset_s } = planned.fault {
                    assert!(offset_s > 0, "{planned}");
                }
            }
        }
    }

    #[test]
    fn schedule_lines_name_the_kind_and_targets() {
        let planned = PlannedFault {
            start: Duration::from_millis(15_250),
            end: Duration::from_millis(30_000),
            fault: partition(
                FaultKind::Bridge,
                vec![
                    vec![Node::N4, Node::N1],
                    vec![Node::N2],
                    vec![Node::N5, Node::N3],
                ],
            ),
        };
        assert_eq!(
            planned.to_string(),
            "   15250ms    30000ms bridge n1,n4 | n2 | n3,n5"
        );
        let skew = PlannedFault {
            start: Duration::from_secs(1),
            end: Duration::from_secs(2),
            fault: Fault::ClockSkew { offset_s: -42 },
        };
        assert_eq!(skew.to_string(), "    1000ms     2000ms clock-skew n1 -42s");
        let strobe = PlannedFault {
            fault: Fault::ClockStrobe { offset_s: 42 },
            ..skew.clone()
        };
        assert_eq!(
            strobe.to_string(),
            "    1000ms     2000ms clock-strobe n1 ±42s"
        );
        let isolation = PlannedFault {
            fault: Fault::IsolatePrimary { database: 3 },
            ..skew
        };
        assert_eq!(
            isolation.to_string(),
            "    1000ms     2000ms isolate-primary chaos/db-3"
        );
    }
}
