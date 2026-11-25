//! Opens a format-2 control log with 0.9.0's own `CommandLog::open`, as a rollback that bypassed
//! `upgrade`'s check would (FR-064, ADR-0040 point 5).
//!
//! Not compiled in this tree. `scripts/write-control-fixtures.sh stray` copies it into a worktree
//! at `fleetringd-v0.9.0` as `crates/fleetringd/tests/control_fixture_stray.rs`, with a copy of
//! `control-0.9.0-migrated/control.db` in `$FIXTURE_OUT`, and runs it there. 0.9.0 runs its
//! `CREATE TABLE IF NOT EXISTS` schema before it reads `format`, so it refuses the file and
//! leaves an empty `commands` table behind; the script saves the file as
//! `control-format2-opened-by-0.9.0/control.db`.

use std::path::PathBuf;

use fleetring_control::ControlConfig;
use fleetringd::control::{CommandLog, LogError};

#[test]
#[ignore = "opens a format-2 file with 0.9.0's code; scripts/write-control-fixtures.sh runs it at the tag"]
fn open() {
    let out = PathBuf::from(std::env::var("FIXTURE_OUT").expect("FIXTURE_OUT names a directory"));
    let path = out.join("control.db");
    match CommandLog::open(&path, ControlConfig::new()) {
        Err(LogError::UnsupportedFormat { found, supported }) => {
            assert_eq!((found.as_str(), supported), ("2", 1));
        }
        other => panic!("0.9.0 must refuse a format-2 file: {other:?}"),
    }
    assert!(
        !out.join("control.db-wal").exists(),
        "closing the refused log checkpointed its WAL"
    );
}
