//! ADR-0040 point 5's field rule: a `State` field, or a field of an existing `Action`, that the
//! previous line doesn't decode must be set only by commands that need a fleet feature, or be
//! derivable from what the previous line does read. Under Raft, a newer leader sends an older
//! voter snapshots and entries whose unknown fields the older voter drops.
//!
//! `fields-0.9.0.txt` lists every JSON field path of the 0.9 line, written at the
//! `fleetringd-v0.9.0` tag by the same computation this test runs (`writer`).

use std::collections::BTreeSet;

use fleetring_control::{Command, Feature};

#[path = "../../../fleetringd/tests/fixtures/control-writers/fields_0_9_0.rs"]
mod writer;

/// Why a field the previous line lacks can't make its voters diverge.
#[derive(Debug)]
#[expect(
    dead_code,
    reason = "the register starts empty; this release's new fields join it"
)]
enum Why {
    /// Only commands that need this feature set it, so it keeps its default until every member
    /// and every control group member reads it.
    Feature(Feature),
    /// `apply` recomputes it from the log and from these fields, which the previous line reads.
    Derived { from: &'static [&'static str] },
}

/// Every field path of this build that the previous line (0.9) lacks, with why it is safe.
const NEW_FIELDS: &[(&str, Why)] = &[];

#[test]
fn every_field_the_previous_line_lacks_is_gated_or_derived() {
    // Embedded at compile time, so the test runs where it has no file system (wasm32-wasip1).
    let previous: BTreeSet<String> = include_str!("fixtures/fields-0.9.0.txt")
        .lines()
        .map(str::to_owned)
        .collect();
    let commands = include_str!("../../../fleetringd/tests/fixtures/control-0.9.0/commands.json");
    let commands: Vec<Command> = serde_json::from_str(commands).expect("the commands decode");
    let current = writer::field_paths(&commands);
    assert!(
        current.len() > 100,
        "the sample covers the state and the actions"
    );

    let registered: BTreeSet<&str> = NEW_FIELDS.iter().map(|(path, _)| *path).collect();
    let unregistered: Vec<&String> = current
        .difference(&previous)
        .filter(|path| !registered.contains(path.as_str()))
        .collect();
    assert!(
        unregistered.is_empty(),
        "fields 0.9 doesn't decode, missing from NEW_FIELDS with their feature or derivation: \
         {unregistered:#?}"
    );
    for (path, why) in NEW_FIELDS {
        assert!(
            current.contains(*path) && !previous.contains(*path),
            "NEW_FIELDS lists {path} ({why:?}), which isn't a field new since 0.9"
        );
        if let Why::Derived { from } = why {
            for source in *from {
                assert!(
                    previous.contains(*source),
                    "{path} derives from {source}, which 0.9 doesn't decode"
                );
            }
        }
    }
}
