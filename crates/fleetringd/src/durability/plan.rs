//! Restore planning (ADR-0029, point 10): which snapshot to start from and which segments to
//! apply, from the names in a [`Listing`] alone.
//!
//! A plan starts from a candidate snapshot at or before the target and walks segments by
//! position: from where the copy is, it takes the segment that starts there, preferring the
//! highest epoch, since a later primary's log is the one that won (ADR-0022). A plan never
//! skips: a walk that finds no segment where the copy is stops there, and if that falls short
//! of the target, the next candidate is tried, and then the plan is refused, naming the
//! furthest position the archive reaches.
//!
//! A time target selects only which acknowledged prefix to restore, by each segment's
//! `archived_at_ms`: the walk stops before the first segment archived after the target. Clocks
//! never decide what the copy holds beyond that; every byte is still verified against the chain.

use std::fmt;

use super::listing::{Listing, SegmentEntry, SnapshotEntry};
use crate::engine::Position;

/// What a restore rebuilds.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RestoreTarget {
    /// The furthest point the archive holds.
    Latest,
    /// Exactly the log up to a position, which must be a transaction boundary.
    Position(Position),
    /// Every segment archived at or before a time, in milliseconds since the Unix epoch by the
    /// archiving nodes' clocks.
    Time(u64),
}

impl fmt::Display for RestoreTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Latest => f.write_str("the latest archived point"),
            Self::Position(position) => write!(f, "position {position}"),
            Self::Time(ms) => write!(f, "time {ms} ms"),
        }
    }
}

/// A snapshot and the segments that bring it to the target.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RestorePlan {
    /// The snapshot to start from.
    pub snapshot: SnapshotEntry,
    /// The segments to apply, in order, each starting where the one before ends.
    pub segments: Vec<SegmentEntry>,
    /// Where to cut the copy back to after the last segment, for a position target inside it.
    pub cut: Option<Position>,
    /// The position the restored copy will be at.
    pub position: Option<Position>,
}

/// The error returned when an archive can't be restored to a target.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PlanError {
    /// The archive holds no complete snapshot of the database.
    #[error("the archive holds no complete snapshot of the database")]
    NoSnapshot,
    /// The archive's segments don't reach the target from any snapshot: one is missing.
    #[error("the archive doesn't reach the target without a gap; it reaches {} at most", position_text(furthest.as_ref()))]
    Unreachable {
        /// The furthest position a walk without gaps reaches.
        furthest: Option<Position>,
    },
    /// The target is before the archive's earliest snapshot.
    #[error("the target is before the archive's earliest snapshot, at {}", position_text(earliest.as_ref()))]
    BeforeEarliest {
        /// The earliest snapshot's position.
        earliest: Option<Position>,
    },
}

fn position_text(position: Option<&Position>) -> String {
    position.map_or_else(|| "the log's beginning".to_owned(), ToString::to_string)
}

/// Plans a restore of `listing`'s database to `target`.
///
/// # Errors
///
/// [`PlanError::NoSnapshot`] without a complete snapshot, [`PlanError::BeforeEarliest`] if every
/// snapshot is past the target, and [`PlanError::Unreachable`] if no walk reaches it.
pub fn plan(listing: &Listing, target: &RestoreTarget) -> Result<RestorePlan, PlanError> {
    if listing.snapshots.is_empty() {
        return Err(PlanError::NoSnapshot);
    }
    let earliest = || PlanError::BeforeEarliest {
        earliest: listing
            .snapshots
            .iter()
            .map(|snapshot| snapshot.position.clone())
            .min()
            .flatten(),
    };
    let (goal, by_time) = match target {
        RestoreTarget::Latest => (listing.latest().0, None),
        RestoreTarget::Position(position) => (Some(position.clone()), None),
        RestoreTarget::Time(ms) => (None, Some(*ms)),
    };
    let mut candidates: Vec<&SnapshotEntry> = listing
        .snapshots
        .iter()
        .filter(|snapshot| match by_time {
            Some(ms) => snapshot.descriptor.taken_at_ms <= ms,
            None => snapshot.position <= goal,
        })
        .collect();
    if candidates.is_empty() {
        return Err(earliest());
    }
    // The candidate closest to the target first: by position for a position, by time for a time.
    match by_time {
        Some(_) => candidates.sort_by(|a, b| {
            (b.descriptor.taken_at_ms, &b.position, b.epoch).cmp(&(
                a.descriptor.taken_at_ms,
                &a.position,
                a.epoch,
            ))
        }),
        None => candidates.sort_by(|a, b| (&b.position, b.epoch).cmp(&(&a.position, a.epoch))),
    }
    let mut furthest: Option<Option<Position>> = None;
    for snapshot in candidates {
        let walk = walk(listing, snapshot, goal.as_ref(), by_time);
        if walk.reached {
            return Ok(RestorePlan {
                snapshot: snapshot.clone(),
                position: walk.cut.clone().or(walk.position),
                segments: walk.segments,
                cut: walk.cut,
            });
        }
        if furthest.as_ref().is_none_or(|far| walk.position > *far) {
            furthest = Some(walk.position);
        }
    }
    Err(PlanError::Unreachable {
        furthest: furthest.flatten(),
    })
}

/// Where a walk from one snapshot got.
struct Walk {
    segments: Vec<SegmentEntry>,
    position: Option<Position>,
    cut: Option<Position>,
    reached: bool,
}

/// Walks segments from `snapshot` towards `goal`, a position, or towards the last segment
/// archived by `by_time`.
fn walk(
    listing: &Listing,
    snapshot: &SnapshotEntry,
    goal: Option<&Position>,
    by_time: Option<u64>,
) -> Walk {
    let mut segments = Vec::new();
    let mut position = snapshot.position.clone();
    loop {
        if by_time.is_none() && position.as_ref() >= goal {
            return Walk {
                segments,
                position,
                cut: None,
                reached: true,
            };
        }
        let next = listing
            .segments
            .iter()
            .filter(|segment| {
                segment.name.start == position && Some(&segment.name.end) > position.as_ref()
            })
            .max_by(|a, b| {
                (a.name.epoch, &a.name.end, a.name.archived_at_ms).cmp(&(
                    b.name.epoch,
                    &b.name.end,
                    b.name.archived_at_ms,
                ))
            });
        let Some(next) = next else {
            // No segment continues the copy. A time target is reached here only if nothing
            // archived by then lies beyond: otherwise a segment is missing.
            let reached = by_time.is_some_and(|ms| {
                !listing.segments.iter().any(|segment| {
                    segment.name.archived_at_ms <= ms && Some(&segment.name.end) > position.as_ref()
                })
            });
            return Walk {
                segments,
                position,
                cut: None,
                reached,
            };
        };
        if by_time.is_some_and(|ms| next.name.archived_at_ms > ms) {
            return Walk {
                segments,
                position,
                cut: None,
                reached: true,
            };
        }
        position = Some(next.name.end.clone());
        segments.push(next.clone());
        if let Some(goal) = goal
            && by_time.is_none()
            && position.as_ref() > Some(goal)
        {
            return Walk {
                segments,
                position,
                cut: Some(goal.clone()),
                reached: true,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use fleetring_control::LeaseEpoch;

    use super::*;
    use crate::durability::format::{FORMAT, SnapshotDescriptor};
    use crate::durability::keys::SegmentName;
    use crate::replication::ChainHash;

    fn at(n: u8) -> Position {
        Position::new([0, n])
    }

    fn snapshot(position: u8, epoch: u64, taken_at_ms: u64) -> SnapshotEntry {
        SnapshotEntry {
            id: format!("{position}_{epoch}"),
            key: format!("{position}_{epoch}/snapshot.json"),
            position: Some(at(position)),
            epoch: LeaseEpoch::new(epoch),
            chain: ChainHash::GENESIS,
            descriptor: SnapshotDescriptor {
                format: FORMAT,
                database: "db".to_owned(),
                engine: "sqlite".to_owned(),
                epoch,
                position: None,
                chain: ChainHash::GENESIS.to_string(),
                taken_at_ms,
                chunks: 0,
                bytes: 0,
                checksum: String::new(),
                chunk_checksums: Vec::new(),
            },
            chunks: Vec::new(),
        }
    }

    fn segment(start: u8, end: u8, epoch: u64, archived_at_ms: u64) -> SegmentEntry {
        let name = SegmentName {
            start: Some(at(start)),
            end: at(end),
            epoch: LeaseEpoch::new(epoch),
            archived_at_ms,
        };
        SegmentEntry {
            key: name.file_name(),
            name,
            size: 10,
        }
    }

    fn listing(snapshots: Vec<SnapshotEntry>, segments: Vec<SegmentEntry>) -> Listing {
        Listing {
            snapshots,
            segments,
            ..Listing::default()
        }
    }

    /// The `(start, end)` of each planned segment.
    fn spans(plan: &RestorePlan) -> Vec<(u8, u8)> {
        plan.segments
            .iter()
            .map(|segment| {
                let start = segment.name.start.as_ref().map_or(0, |p| p.as_bytes()[1]);
                (start, segment.name.end.as_bytes()[1])
            })
            .collect()
    }

    #[test]
    fn latest_walks_every_segment_from_the_newest_snapshot() {
        let archive = listing(
            vec![snapshot(1, 1, 100), snapshot(4, 1, 400)],
            vec![
                segment(1, 2, 1, 200),
                segment(2, 4, 1, 300),
                segment(4, 6, 1, 500),
                segment(6, 9, 1, 600),
            ],
        );
        let plan = plan(&archive, &RestoreTarget::Latest).expect("a plan");
        assert_eq!(plan.snapshot.position, Some(at(4)));
        assert_eq!(spans(&plan), [(4, 6), (6, 9)]);
        assert_eq!((plan.cut, plan.position), (None, Some(at(9))));
    }

    #[test]
    fn a_time_target_stops_before_the_first_segment_archived_after_it() {
        let archive = listing(
            vec![snapshot(1, 1, 100)],
            vec![
                segment(1, 2, 1, 200),
                segment(2, 3, 1, 300),
                segment(3, 4, 1, 400),
            ],
        );
        let plan = plan(&archive, &RestoreTarget::Time(350)).expect("a plan");
        assert_eq!(spans(&plan), [(1, 2), (2, 3)]);
        assert_eq!(plan.position, Some(at(3)));
        let exact = super::plan(&archive, &RestoreTarget::Time(300)).expect("a plan");
        assert_eq!(exact.position, Some(at(3)), "at or before the time");
        let all = super::plan(&archive, &RestoreTarget::Time(10_000)).expect("a plan");
        assert_eq!(all.position, Some(at(4)));
        assert_eq!(
            super::plan(&archive, &RestoreTarget::Time(50)),
            Err(PlanError::BeforeEarliest {
                earliest: Some(at(1))
            })
        );
    }

    #[test]
    fn a_position_target_inside_a_segment_sets_the_cut() {
        let archive = listing(
            vec![snapshot(1, 1, 100)],
            vec![segment(1, 3, 1, 200), segment(3, 8, 1, 300)],
        );
        let plan = plan(&archive, &RestoreTarget::Position(at(5))).expect("a plan");
        assert_eq!(spans(&plan), [(1, 3), (3, 8)]);
        assert_eq!(plan.cut, Some(at(5)));
        assert_eq!(plan.position, Some(at(5)));
        let boundary = super::plan(&archive, &RestoreTarget::Position(at(3))).expect("a plan");
        assert_eq!((spans(&boundary), boundary.cut), (vec![(1, 3)], None));
        let snapshot_only = super::plan(&archive, &RestoreTarget::Position(at(1))).expect("a plan");
        assert!(snapshot_only.segments.is_empty());
    }

    #[test]
    fn the_highest_epoch_wins_where_two_primaries_archived_from_one_position() {
        // The old primary archived 3..7; the new one resumed from its mark at 3.
        let archive = listing(
            vec![snapshot(1, 1, 100)],
            vec![
                segment(1, 3, 1, 200),
                segment(3, 7, 1, 300),
                segment(3, 5, 2, 310),
                segment(5, 9, 2, 400),
            ],
        );
        let plan = plan(&archive, &RestoreTarget::Latest).expect("a plan");
        assert_eq!(spans(&plan), [(1, 3), (3, 5), (5, 9)]);
        assert!(
            plan.segments[1..]
                .iter()
                .all(|s| s.name.epoch == LeaseEpoch::new(2))
        );
    }

    #[test]
    fn a_gap_names_the_furthest_reachable_position() {
        let archive = listing(
            vec![snapshot(1, 1, 100)],
            vec![segment(1, 3, 1, 200), segment(4, 6, 1, 300)],
        );
        let unreachable = Err(PlanError::Unreachable {
            furthest: Some(at(3)),
        });
        assert_eq!(plan(&archive, &RestoreTarget::Latest), unreachable);
        assert_eq!(plan(&archive, &RestoreTarget::Position(at(5))), unreachable);
        assert_eq!(plan(&archive, &RestoreTarget::Time(1_000)), unreachable);
        // Before the gap, every target is reachable.
        assert!(plan(&archive, &RestoreTarget::Time(250)).is_ok());
        assert!(plan(&archive, &RestoreTarget::Position(at(3))).is_ok());
    }

    #[test]
    fn an_older_snapshot_is_used_when_the_newest_is_past_the_target() {
        let archive = listing(
            vec![snapshot(1, 1, 100), snapshot(6, 1, 600)],
            vec![
                segment(1, 3, 1, 200),
                segment(3, 6, 1, 300),
                segment(6, 8, 1, 700),
            ],
        );
        let plan = plan(&archive, &RestoreTarget::Position(at(3))).expect("a plan");
        assert_eq!(plan.snapshot.position, Some(at(1)));
        let by_time = super::plan(&archive, &RestoreTarget::Time(400)).expect("a plan");
        assert_eq!(by_time.snapshot.position, Some(at(1)));
        assert_eq!(by_time.position, Some(at(6)));
        // A newer snapshot whose own walk is cut short is passed over for one that reaches.
        let gapped = listing(
            vec![snapshot(1, 1, 100), snapshot(4, 1, 400)],
            vec![segment(1, 3, 1, 200), segment(3, 9, 1, 300)],
        );
        let plan = super::plan(&gapped, &RestoreTarget::Position(at(9))).expect("a plan");
        assert_eq!(plan.snapshot.position, Some(at(1)));
    }

    #[test]
    fn incomplete_snapshots_are_ignored() {
        let mut archive = listing(vec![snapshot(1, 1, 100)], vec![segment(1, 2, 1, 200)]);
        archive
            .incomplete
            .push(crate::durability::listing::IncompleteSnapshot {
                id: "x".to_owned(),
                position: Some(at(2)),
                chunks: vec!["k".to_owned()],
            });
        let plan = plan(&archive, &RestoreTarget::Latest).expect("a plan");
        assert_eq!(plan.snapshot.position, Some(at(1)));
        assert_eq!(
            super::plan(&Listing::default(), &RestoreTarget::Latest),
            Err(PlanError::NoSnapshot)
        );
    }
}
