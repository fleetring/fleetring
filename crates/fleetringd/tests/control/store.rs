//! `control.db` format 2 and the migration from format 1 (FR-064, ADR-0040 points 3, 5, 7, and
//! 8).

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use fleetring_control::{Action, Command, ControlConfig, Timestamp};
use fleetringd::control::raft::codec::EntryWire;
use fleetringd::control::raft::{
    ControlPeer, ControlRequest, Entry, LeaderRef, LogId, Membership, RaftId, SnapshotMeta,
    StoredMembership, Vote, raft_id,
};
use fleetringd::control::{CommandLog, ControlStore, LogError};
use openraft::EntryPayload;
use openraft::impls::leader_id_adv::LeaderId;

use crate::fixtures;

const OWN: &str = "c1";

fn own() -> (RaftId, ControlPeer) {
    (raft_id(OWN), ControlPeer::new(OWN, "127.0.0.1:7400"))
}

fn log_id(term: u64, index: u64) -> LogId {
    LogId::new(
        LeaderId {
            term,
            node_id: raft_id(OWN),
        },
        index,
    )
}

fn command_entry(term: u64, index: u64) -> Entry {
    let command = Command::stamped(
        Timestamp::from_millis(index),
        OWN.parse().expect("a node id"),
        Action::Tick,
    );
    let screened = LeaderRef {
        term,
        node: raft_id(OWN),
    };
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Normal(
            ControlRequest::new(screened, command).expect("the command encodes"),
        ),
    }
}

fn membership() -> Membership {
    let (id, peer) = own();
    Membership::new(vec![BTreeSet::from([id])], BTreeMap::from([(id, peer)]))
        .expect("the voter has a node")
}

fn blank(term: u64, index: u64) -> Entry {
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Blank,
    }
}

/// Entries compare through their encoding: `Entry` has no `PartialEq` for every payload.
fn encoded(entries: &[Entry]) -> String {
    let wires: Vec<EntryWire> = entries.iter().map(EntryWire::from).collect();
    serde_json::to_string(&wires).expect("entries encode")
}

fn open(path: &Path) -> ControlStore {
    ControlStore::open(path, ControlConfig::new(), Some(own())).expect("the store opens")
}

fn new_store() -> (tempfile::TempDir, PathBuf, ControlStore) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("control.db");
    let store = open(&path);
    (dir, path, store)
}

fn execute(path: &Path, sql: &str) {
    let connection = rusqlite::Connection::open(path).expect("the file opens");
    connection.execute_batch(sql).expect("the statement runs");
}

fn query<T: rusqlite::types::FromSql>(path: &Path, sql: &str) -> T {
    let connection = rusqlite::Connection::open(path).expect("the file opens");
    connection
        .query_row(sql, [], |row| row.get(0))
        .expect("the query runs")
}

fn has_table(path: &Path, table: &str) -> bool {
    query::<i64>(
        path,
        &format!("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '{table}'"),
    ) > 0
}

#[test]
fn appended_entries_survive_reopening() {
    let (_dir, path, mut store) = new_store();
    assert_eq!(store.last_log_id().expect("readable"), None);
    let entries = vec![
        Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Membership(membership()),
        },
        blank(1, 2),
        command_entry(1, 3),
    ];
    store.append_entries(&entries).expect("the entries append");
    drop(store);
    let store = open(&path);
    assert_eq!(store.last_log_id().expect("readable"), Some(log_id(1, 3)));
    assert_eq!(
        encoded(&store.entries(..).expect("readable")),
        encoded(&entries)
    );
    assert_eq!(
        encoded(&store.entries(2..3).expect("readable")),
        encoded(&entries[1..2])
    );
    // The stored kinds are readable in the sqlite3 shell.
    assert_eq!(
        query::<String>(&path, "SELECT group_concat(kind, ',') FROM log"),
        "membership,blank,command"
    );
    assert_eq!(
        query::<String>(&path, "SELECT leader_node FROM log WHERE log_index = 3"),
        format!("{:016x}", raft_id(OWN))
    );
}

#[test]
fn truncation_removes_the_tail_and_purging_the_head() {
    let (_dir, _path, mut store) = new_store();
    let entries: Vec<Entry> = (1..=10).map(|index| command_entry(1, index)).collect();
    store.append_entries(&entries).expect("the entries append");
    store.truncate_from(8).expect("the tail truncates");
    assert_eq!(store.last_log_id().expect("readable"), Some(log_id(1, 7)));
    store.purge_through(&log_id(1, 3)).expect("the head purges");
    assert_eq!(store.purged().expect("readable"), Some(log_id(1, 3)));
    assert_eq!(
        encoded(&store.entries(..).expect("readable")),
        encoded(&entries[3..7])
    );
    store
        .truncate_from(4)
        .expect("everything after the purge truncates");
    assert_eq!(
        store.last_log_id().expect("readable"),
        Some(log_id(1, 3)),
        "an empty log's last log ID is the last purged"
    );
}

#[test]
fn vote_and_committed_survive_reopening() {
    let (_dir, path, mut store) = new_store();
    assert_eq!(store.vote().expect("readable"), None);
    assert_eq!(store.committed().expect("readable"), None);
    let mut vote = Vote::new(3, raft_id(OWN));
    vote.committed = true;
    store.save_vote(&vote).expect("the vote saves");
    store
        .save_committed(Some(&log_id(3, 12)))
        .expect("committed saves");
    drop(store);
    let mut store = open(&path);
    assert_eq!(store.vote().expect("readable"), Some(vote));
    assert_eq!(store.committed().expect("readable"), Some(log_id(3, 12)));
    store.save_committed(None).expect("committed clears");
    assert_eq!(store.committed().expect("readable"), None);
}

#[test]
fn only_the_newest_snapshots_are_kept() {
    let (_dir, path, mut store) = new_store();
    let state = fleetring_control::State::new(ControlConfig::new());
    let retain = NonZeroUsize::new(2).expect("nonzero");
    for index in [10, 20, 30] {
        let meta = SnapshotMeta {
            last_log_id: Some(log_id(1, index)),
            last_membership: StoredMembership::new(Some(log_id(1, 1)), membership()),
        };
        store
            .save_control_snapshot(&meta, &state, retain)
            .expect("the snapshot saves");
    }
    drop(store);
    let store = open(&path);
    let kept: Vec<u64> = store
        .control_snapshots()
        .expect("readable")
        .iter()
        .map(|snapshot| snapshot.meta.last_log_id.expect("a last log ID").index)
        .collect();
    assert_eq!(kept, [20, 30]);
    let latest = store
        .latest_control_snapshot()
        .expect("readable")
        .expect("a snapshot");
    assert_eq!(latest.meta.last_log_id, Some(log_id(1, 30)));
    assert_eq!(latest.meta.last_membership.membership(), &membership());
    assert_eq!(latest.state, state);
}

#[test]
fn committed_commands_skip_raft_entries_and_stop_at_committed() {
    let (_dir, _path, mut store) = new_store();
    let entries = vec![
        Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Membership(membership()),
        },
        blank(1, 2),
        command_entry(1, 3),
        command_entry(1, 4),
        blank(2, 5),
        command_entry(2, 6),
        command_entry(2, 7),
    ];
    store.append_entries(&entries).expect("the entries append");
    assert!(
        store
            .committed_commands_after(0)
            .expect("readable")
            .is_empty(),
        "nothing is committed yet"
    );
    store
        .save_committed(Some(&log_id(2, 6)))
        .expect("committed saves");
    let indexes = |after| -> Vec<u64> {
        store
            .committed_commands_after(after)
            .expect("readable")
            .iter()
            .map(|(log_id, _)| log_id.index)
            .collect()
    };
    assert_eq!(indexes(0), [3, 4, 6]);
    assert_eq!(indexes(3), [4, 6]);
    assert_eq!(indexes(6), Vec::<u64>::new());
    let (_, request) = store
        .committed_commands_after(5)
        .expect("readable")
        .remove(0);
    assert_eq!(request.decode().expect("a command").action, Action::Tick);
}

#[test]
fn the_0_9_0_fixture_migrates_to_a_snapshot_of_its_replay() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = fixtures::copy_into(dir.path());
    let store = open(&path);
    let last = u64::try_from(fixtures::commands().len()).expect("a small log");
    let migrated = LogId::new(
        LeaderId {
            term: 0,
            node_id: 0,
        },
        last,
    );
    let snapshots = store.control_snapshots().expect("readable");
    assert_eq!(snapshots.len(), 1, "migration writes one snapshot");
    let snapshot = &snapshots[0];
    assert_eq!(snapshot.meta.last_log_id, Some(migrated));
    assert_eq!(
        snapshot.meta.last_membership.membership(),
        &membership(),
        "this node is the only voter"
    );
    assert_eq!(snapshot.state, fixtures::replayed());
    fixtures::assert_holds_the_writers_history(&snapshot.state);
    assert_eq!(store.committed().expect("readable"), Some(migrated));
    assert_eq!(store.purged().expect("readable"), Some(migrated));
    assert_eq!(store.last_log_id().expect("readable"), Some(migrated));
    assert!(store.entries(..).expect("readable").is_empty());
    assert_eq!(store.genesis(), ControlConfig::new());
    drop(store);
    assert_eq!(
        query::<String>(&path, "SELECT value FROM meta WHERE key = 'format'"),
        "2"
    );
    assert!(has_table(&path, "commands_v1") && has_table(&path, "snapshots_v1"));
    assert!(!has_table(&path, "commands"));
    // Reopening finds format 2 and changes nothing.
    let store = open(&path);
    assert_eq!(store.control_snapshots().expect("readable"), snapshots);
}

#[test]
fn a_snapshot_that_disagrees_with_the_replay_refuses_migration() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = fixtures::copy_into(dir.path());
    let (index, text): (i64, String) = {
        let connection = rusqlite::Connection::open(&path).expect("the file opens");
        connection
            .query_row(
                "SELECT log_index, state FROM snapshots ORDER BY log_index DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("the fixture has a snapshot")
    };
    // Move one member to another zone: still a valid state, but not the one the log replays to.
    let mut state: serde_json::Value = serde_json::from_str(&text).expect("the snapshot decodes");
    state["members"]["n1"]["member"]["zone"] = "z".into();
    let edited = serde_json::to_string(&state).expect("the snapshot encodes");
    let connection = rusqlite::Connection::open(&path).expect("the file opens");
    connection
        .execute(
            "UPDATE snapshots SET state = ?1 WHERE log_index = ?2",
            rusqlite::params![edited, index],
        )
        .expect("the snapshot is edited");
    drop(connection);

    let error = ControlStore::open(&path, ControlConfig::new(), Some(own()))
        .expect_err("migration refuses");
    assert!(
        matches!(&error, LogError::Migration { reason } if reason.contains(&format!(
            "the snapshot at {index} doesn't match replaying the log from genesis; \
             the first difference is at {index}"
        ))),
        "{error}"
    );
    assert_eq!(
        query::<String>(&path, "SELECT value FROM meta WHERE key = 'format'"),
        "1",
        "the file is left as format 1"
    );
}

#[test]
fn an_empty_format_one_log_migrates_to_an_empty_store() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("control.db");
    drop(CommandLog::open(&path, ControlConfig::new()).expect("an empty format-1 log"));
    let store = open(&path);
    assert_eq!(store.last_log_id().expect("readable"), None);
    assert_eq!(store.committed().expect("readable"), None);
    assert!(store.control_snapshots().expect("readable").is_empty());
    drop(store);
    assert_eq!(
        query::<String>(&path, "SELECT value FROM meta WHERE key = 'format'"),
        "2"
    );
}

#[test]
fn a_format_one_log_without_an_id_is_refused() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = fixtures::copy_into(dir.path());
    let error =
        ControlStore::open(&path, ControlConfig::new(), None).expect_err("migration refuses");
    assert!(
        matches!(&error, LogError::Migration { reason } if reason == "the node's raft id is needed"),
        "{error}"
    );
    assert_eq!(
        query::<String>(&path, "SELECT value FROM meta WHERE key = 'format'"),
        "1"
    );
}

#[test]
fn a_newer_format_is_refused() {
    let (_dir, path, store) = new_store();
    drop(store);
    execute(&path, "UPDATE meta SET value = '3' WHERE key = 'format'");
    let error = ControlStore::open(&path, ControlConfig::new(), Some(own()))
        .expect_err("format 3 is refused");
    assert!(
        matches!(&error, LogError::UnsupportedFormat { found, supported: 2 } if found == "3"),
        "{error}"
    );
}

#[test]
fn the_empty_table_0_9_0_leaves_is_dropped() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("control.db");
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/control-format2-opened-by-0.9.0/control.db");
    std::fs::copy(fixture, &path).expect("the fixture can be copied");
    assert!(has_table(&path, "commands"), "0.9.0 left a commands table");
    let store = open(&path);
    let snapshot = store
        .latest_control_snapshot()
        .expect("readable")
        .expect("the migrated snapshot");
    assert_eq!(snapshot.state, fixtures::replayed());
    drop(store);
    assert!(!has_table(&path, "commands"), "the empty table is dropped");
}

#[test]
fn a_non_empty_stray_table_is_refused() {
    let (_dir, path, store) = new_store();
    drop(store);
    execute(
        &path,
        "CREATE TABLE commands (log_index INTEGER PRIMARY KEY, command TEXT NOT NULL) STRICT;
         INSERT INTO commands VALUES (1, '{}'), (2, '{}');",
    );
    let error = ControlStore::open(&path, ControlConfig::new(), Some(own()))
        .expect_err("a non-empty stray table is refused");
    assert!(
        matches!(&error, LogError::StrayTable { rows: 2, .. }),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .ends_with("has a format-1 `commands` table with 2 rows"),
        "{error}"
    );
}

/// Writes `tests/fixtures/control-0.9.0-migrated/control.db`: the 0.9.0 fixture migrated by this
/// build, which `scripts/write-control-fixtures.sh stray` then opens with 0.9.0's code.
#[test]
#[ignore = "writes the migrated fixture; run once by hand"]
fn write_the_migrated_fixture() {
    let out =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/control-0.9.0-migrated");
    std::fs::create_dir_all(&out).expect("the directory can be created");
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = fixtures::copy_into(dir.path());
    drop(open(&path));
    assert!(
        !dir.path().join("control.db-wal").exists(),
        "closing checkpointed the WAL"
    );
    std::fs::copy(&path, out.join("control.db")).expect("the migrated fixture can be written");
}
