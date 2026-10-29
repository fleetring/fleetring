//! The format checked against WAL files written by real SQLite.

use std::fs;

use fleetring_wal::{
    AppendError, Batch, ChecksumOrder, LogEnd, Mirror, MirrorError, Position, ReadError, WalHeader,
    read_committed, read_committed_file,
};
use proptest::prelude::*;
use rusqlite::{Connection, params};

use crate::common::sqlite::{
    Checkpoint, Pair, checkpoint, integrity, open_primary, rows, wal_path,
};

/// Inserts `count` rows of `len` bytes each in one transaction.
fn insert(conn: &Connection, count: usize, len: usize, fill: u8) {
    let tx = conn.unchecked_transaction().expect("begin");
    for i in 0..count {
        tx.execute(
            "INSERT INTO t (v) VALUES (?1)",
            params![vec![fill.wrapping_add(i.to_le_bytes()[0]); len]],
        )
        .expect("insert");
    }
    tx.commit().expect("commit");
}

/// The offsets just past each commit frame in the WAL file at `path`, found by reading one
/// committed transaction at a time.
fn commit_ends(wal: &[u8]) -> Vec<u64> {
    let mut ends = Vec::new();
    let mut after = None;
    while let Some(batch) = read_committed(wal, after).expect("valid") {
        let mut offset = batch.start().offset();
        for frame in batch.frame_headers() {
            offset += 24 + u64::from(batch.page_size());
            if frame.is_commit() {
                ends.push(offset);
            }
        }
        after = Some(batch.end());
    }
    ends
}

#[test]
fn reading_a_file_from_a_position_matches_reading_it_whole() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("p.db");
    let conn = open_primary(&db, 512);
    checkpoint(&conn, Checkpoint::Truncate);
    insert(&conn, 1, 40, 1);
    insert(&conn, 3, 100, 2);
    insert(&conn, 2, 700, 3);
    insert(&conn, 1, 10, 4);
    let path = wal_path(&db);
    let wal = fs::read(&path).expect("read wal");
    let header = WalHeader::parse(&wal).expect("header");
    let generation = header.generation();

    let ends = commit_ends(&wal);
    assert_eq!(ends.len(), 4);
    for (index, end) in ends.iter().enumerate() {
        let position = Position::new(generation, *end);
        let from_file = read_committed_file(&path, Some(position)).expect("read file");
        let whole = read_committed(&wal, Some(position)).expect("read whole");
        assert_eq!(
            from_file.as_ref().map(Batch::to_bytes),
            whole.as_ref().map(Batch::to_bytes),
            "after commit {index}"
        );
        assert_eq!(from_file.is_none(), index + 1 == ends.len());
    }
    let start = Position::new(generation, 32);
    assert_eq!(
        read_committed_file(&path, Some(start))
            .expect("read file")
            .map(|batch| batch.to_bytes()),
        read_committed(&wal, Some(start))
            .expect("read whole")
            .map(|batch| batch.to_bytes()),
        "from the generation's start"
    );

    let frame = 24 + 512;
    assert!(
        ends[2] - ends[1] > frame,
        "the third transaction spans several frames"
    );
    let inside = Position::new(generation, ends[2] - frame);
    assert!(matches!(
        read_committed_file(&path, Some(inside)),
        Err(fleetring_wal::ReadFileError::Read(
            ReadError::NotCommitBoundary { .. }
        ))
    ));
    let unaligned = Position::new(generation, ends[0] + 1);
    assert!(matches!(
        read_committed_file(&path, Some(unaligned)),
        Err(fleetring_wal::ReadFileError::Read(
            ReadError::NotCommitBoundary { .. }
        ))
    ));
    let past = Position::new(generation, ends[3] + 10 * (24 + 512));
    assert!(matches!(
        read_committed_file(&path, Some(past)),
        Err(fleetring_wal::ReadFileError::Read(
            ReadError::PastEnd { .. }
        ))
    ));

    // Once the WAL restarts, an older position gets the new generation with its header.
    checkpoint(&conn, Checkpoint::Passive);
    insert(&conn, 1, 10, 5);
    let batch = read_committed_file(&path, Some(Position::new(generation, ends[3])))
        .expect("read file")
        .expect("a new generation");
    assert!(batch.header().is_some());
}

/// A primary in a temporary directory with three small transactions in its WAL.
fn small_primary() -> (tempfile::TempDir, Vec<u8>) {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("p.db");
    let conn = open_primary(&db, 512);
    checkpoint(&conn, Checkpoint::Truncate);
    insert(&conn, 1, 40, 1);
    insert(&conn, 3, 100, 2);
    insert(&conn, 2, 700, 3);
    let wal = fs::read(wal_path(&db)).expect("read wal");
    drop(conn);
    (dir, wal)
}

#[test]
fn frames_written_by_sqlite_validate() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("p.db");
    let conn = open_primary(&db, 4096);
    for round in 0..5 {
        insert(&conn, 10, 300 * round + 1, round.to_le_bytes()[0]);
    }
    let wal = fs::read(wal_path(&db)).expect("read wal");
    let header = WalHeader::parse(&wal).expect("sqlite wrote a valid header");
    let native = if cfg!(target_endian = "big") {
        ChecksumOrder::BigEndian
    } else {
        ChecksumOrder::LittleEndian
    };
    assert_eq!(header.checksum_order(), native);
    assert_eq!(header.page_size(), 4096);

    let batch = read_committed_file(wal_path(&db), None)
        .expect("read")
        .expect("frames");
    assert_eq!(batch.header(), Some(&header));
    assert_eq!(
        batch.end().offset(),
        wal.len() as u64,
        "every frame sqlite wrote is committed"
    );
    assert_eq!(batch.frame_count(), (wal.len() - 32) / (24 + 4096));
    assert_eq!(commit_ends(&wal).len(), 6, "schema plus five transactions");
    assert_eq!(Batch::from_bytes(&batch.to_bytes()), Ok(batch.clone()));
    let end = LogEnd::recover(&wal).expect("valid").expect("header");
    assert_eq!(end.position(), batch.end());
}

#[test]
fn a_corrupted_byte_anywhere_in_a_frame_is_detected() {
    let (_dir, wal) = small_primary();
    let ends = commit_ends(&wal);
    assert_eq!(ends.last().copied(), Some(wal.len() as u64));
    let frame_len = 24 + 512;
    for at in 32..wal.len() {
        let frame_start = (32 + (at - 32) / frame_len * frame_len) as u64;
        let expected = ends
            .iter()
            .copied()
            .filter(|&end| end <= frame_start)
            .max()
            .unwrap_or(32);
        let mut bad = wal.clone();
        bad[at] ^= 0x01;
        let end = read_committed(&bad, None)
            .expect("header intact")
            .map_or(32, |batch| batch.end().offset());
        assert_eq!(end, expected, "corrupted byte {at}");
    }
    for at in 0..32 {
        let mut bad = wal.clone();
        bad[at] ^= 0x01;
        assert!(
            matches!(read_committed(&bad, None), Err(ReadError::Header(_))),
            "header byte {at}"
        );
    }
}

#[test]
fn a_torn_final_transaction_is_never_returned() {
    let (_dir, wal) = small_primary();
    let ends = commit_ends(&wal);
    for len in 0..=wal.len() {
        let expected = ends.iter().copied().filter(|&end| end <= len as u64).max();
        let got = read_committed(&wal[..len], None)
            .expect("valid")
            .map(|batch| batch.end().offset());
        assert_eq!(got, expected, "WAL cut at {len} bytes");
    }
}

#[test]
fn frames_of_an_open_transaction_are_never_returned() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("p.db");
    let conn = open_primary(&db, 512);
    insert(&conn, 2, 50, 1);
    let committed = read_committed_file(wal_path(&db), None)
        .expect("read")
        .expect("frames");

    // A tiny page cache makes SQLite spill the open transaction's pages into the WAL.
    conn.execute_batch("PRAGMA cache_size = 2;")
        .expect("cache size");
    let tx = conn.unchecked_transaction().expect("begin");
    for i in 0..200u8 {
        tx.execute("INSERT INTO t (v) VALUES (?1)", params![vec![i; 400]])
            .expect("insert");
    }
    let wal = fs::read(wal_path(&db)).expect("read wal");
    assert!(
        wal.len() as u64 > committed.end().offset(),
        "the open transaction spilled frames"
    );
    let during = read_committed(&wal, None).expect("valid").expect("frames");
    assert_eq!(during.end(), committed.end());
    assert_eq!(read_committed(&wal, Some(committed.end())), Ok(None));
    tx.rollback().expect("rollback");
}

#[test]
fn a_mirrored_follower_opens_with_the_primarys_contents() {
    let mut pair = Pair::new(4096);
    insert(&pair.primary, 5, 100, 1);
    pair.ship().expect("first batch");
    insert(&pair.primary, 3, 5000, 2);
    insert(&pair.primary, 1, 10, 3);
    let batch = pair.ship().expect("two transactions in one batch");
    assert_eq!(batch.header(), None);

    checkpoint(&pair.primary, Checkpoint::Truncate);
    pair.primary
        .execute("DELETE FROM t WHERE id % 2 = 0", [])
        .expect("delete");
    let restarted = pair.ship().expect("new generation");
    assert!(
        restarted.header().is_some(),
        "a truncating checkpoint restarts the WAL"
    );

    checkpoint(&pair.primary, Checkpoint::Passive);
    insert(&pair.primary, 2, 3000, 4);
    let restarted = pair.ship().expect("new generation");
    assert!(
        restarted.header().is_some(),
        "a passive checkpoint restarts the WAL on the next write"
    );
    pair.primary
        .execute("UPDATE t SET v = x'00ff' WHERE id = 1", [])
        .expect("update");
    pair.ship().expect("update");

    let reopened = Mirror::open(pair.mirror.path()).expect("reopen mirror");
    assert_eq!(reopened.position(), pair.mirror.position());

    let follower = pair.open_follower();
    assert_eq!(integrity(&follower), "ok");
    assert_eq!(rows(&follower), rows(&pair.primary));
}

#[test]
fn a_mirror_rejects_batches_that_do_not_continue_it() {
    let mut pair = Pair::new(4096);
    insert(&pair.primary, 1, 10, 1);
    let first = pair.ship().expect("first batch");
    insert(&pair.primary, 1, 10, 2);
    let second = read_committed_file(pair.primary_wal(), Some(first.end()))
        .expect("read")
        .expect("frames");

    assert!(matches!(
        pair.mirror.append(&first),
        Err(MirrorError::Append(AppendError::Discontinuous { .. }))
    ));
    let header = *first.header().expect("starts a generation");
    assert!(matches!(
        pair.mirror.start_generation(&header),
        Err(MirrorError::StaleGeneration { .. })
    ));
    assert_eq!(
        pair.mirror.position(),
        Some(first.end()),
        "rejections leave the mirror as it was"
    );
    assert_eq!(
        pair.mirror.append(&second).expect("continues"),
        second.end()
    );

    let mut empty = Mirror::open(pair.follower_db.with_extension("other-wal")).expect("open");
    assert!(matches!(
        empty.append(&second),
        Err(MirrorError::NoGeneration)
    ));
    assert!(matches!(
        empty.truncate(second.end()),
        Err(MirrorError::NoGeneration)
    ));
}

#[test]
fn truncating_a_mirror_discards_the_divergent_tail() {
    let mut pair = Pair::new(4096);
    insert(&pair.primary, 4, 200, 1);
    let first = pair.ship().expect("first batch");
    let after_first = rows(&pair.primary);
    insert(&pair.primary, 4, 2000, 2);
    let second = pair.ship().expect("second batch");

    let inside = Position::new(second.generation(), second.start().offset() + 24 + 4096);
    assert!(matches!(
        pair.mirror.truncate(inside),
        Err(MirrorError::Truncate(ReadError::NotCommitBoundary { .. }))
    ));
    pair.mirror
        .truncate(first.end())
        .expect("truncate to a commit boundary");
    assert_eq!(pair.mirror.position(), Some(first.end()));
    assert_eq!(
        fs::metadata(pair.mirror.path()).expect("stat").len(),
        first.end().offset()
    );

    // The cut frames can be appended again, and cut again.
    let again = pair.ship().expect("the cut transaction ships again");
    assert_eq!(again, second);
    pair.mirror.truncate(first.end()).expect("truncate again");

    let follower = pair.open_follower();
    assert_eq!(integrity(&follower), "ok");
    assert_eq!(rows(&follower), after_first);
}

/// One step of a primary's workload.
#[derive(Debug, Clone)]
enum Step {
    /// One transaction, then a shipment if `ship` is set; otherwise its frames wait for the next.
    Write { stmts: Vec<Stmt>, ship: bool },
    /// Ship everything, then checkpoint the primary.
    Checkpoint(Checkpoint),
}

#[derive(Debug, Clone)]
enum Stmt {
    Insert { len: usize, fill: u8 },
    Update { bucket: i64, len: usize, fill: u8 },
    Delete { bucket: i64 },
}

fn stmt() -> impl Strategy<Value = Stmt> {
    let len = prop_oneof![0usize..64, 64usize..2048, 4000usize..9000];
    prop_oneof![
        3 => (len.clone(), any::<u8>()).prop_map(|(len, fill)| Stmt::Insert { len, fill }),
        1 => (0i64..4, len, any::<u8>()).prop_map(|(bucket, len, fill)| Stmt::Update { bucket, len, fill }),
        1 => (0i64..4).prop_map(|bucket| Stmt::Delete { bucket }),
    ]
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (proptest::collection::vec(stmt(), 1..6), any::<bool>())
            .prop_map(|(stmts, ship)| Step::Write { stmts, ship }),
        1 => prop_oneof![Just(Checkpoint::Passive), Just(Checkpoint::Truncate)].prop_map(Step::Checkpoint),
    ]
}

fn run(conn: &Connection, stmts: &[Stmt]) {
    let tx = conn.unchecked_transaction().expect("begin");
    for stmt in stmts {
        match *stmt {
            Stmt::Insert { len, fill } => {
                tx.execute("INSERT INTO t (v) VALUES (?1)", params![vec![fill; len]])
            }
            Stmt::Update { bucket, len, fill } => tx.execute(
                "UPDATE t SET v = ?1 WHERE id % 4 = ?2",
                params![vec![fill; len], bucket],
            ),
            Stmt::Delete { bucket } => {
                tx.execute("DELETE FROM t WHERE id % 4 = ?1", params![bucket])
            }
        }
        .expect("statement");
    }
    tx.commit().expect("commit");
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    #[test]
    fn a_mirrored_follower_matches_the_primary_after_both_checkpoint(
        steps in proptest::collection::vec(step(), 1..10),
    ) {
        let mut pair = Pair::new(1024);
        for step in &steps {
            match step {
                Step::Write { stmts, ship } => {
                    run(&pair.primary, stmts);
                    if *ship {
                        pair.ship();
                    }
                }
                Step::Checkpoint(mode) => {
                    pair.ship();
                    checkpoint(&pair.primary, *mode);
                }
            }
        }
        pair.ship();
        {
            let follower = pair.open_follower();
            prop_assert_eq!(integrity(&follower), "ok");
            prop_assert_eq!(rows(&follower), rows(&pair.primary));
        }

        checkpoint(&pair.primary, Checkpoint::Truncate);
        pair.checkpoint_follower();
        let primary = fs::read(&pair.primary_db).expect("read primary");
        let follower = fs::read(&pair.follower_db).expect("read follower");
        prop_assert!(primary == follower, "database files differ after both checkpoint");
    }
}
