use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use fleetring_control::{Action, Command, Feature, Release, Timestamp};
use openraft::raft::{AppendEntriesRequest, VoteRequest, VoteResponse};

use super::*;
use crate::control::raft::raft_id;

/// The leader of most vectors: a Raft ID above 2^63, so every vector shows it survives as text.
const LEADER: RaftId = 0xd733_a26d_d4b4_9b94;
const FOLLOWER: RaftId = 0x0000_0000_0000_0002;
const LEARNER: RaftId = 0x7fff_ffff_ffff_fffe;

fn directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/raft-wire-v1")
}

fn log_id(term: u64, node: RaftId, index: u64) -> LogId {
    LogId::new(leader(term, node), index)
}

fn committed_vote(term: u64, node: RaftId) -> Vote {
    let mut vote = Vote::new(term, node);
    vote.committed = true;
    vote
}

fn command() -> Command {
    Command::stamped(
        Timestamp::from_millis(1_764_000_000_000),
        "c1".parse().expect("a node id"),
        Action::Leave {
            id: "n3".parse().expect("a node id"),
        },
    )
}

fn request() -> ControlRequest {
    ControlRequest::new(
        LeaderRef {
            term: 3,
            node: LEADER,
        },
        command(),
    )
    .expect("the command encodes")
}

fn peers() -> BTreeMap<RaftId, ControlPeer> {
    BTreeMap::from([
        (LEADER, ControlPeer::new("c1", "c1.fleet.internal:7400")),
        (FOLLOWER, ControlPeer::new("c2", "10.0.0.2:7400")),
        (LEARNER, ControlPeer::new("c3", "[fd00::3]:7400")),
    ])
}

fn uniform() -> Membership {
    Membership::new(vec![BTreeSet::from([LEADER, FOLLOWER])], peers()).expect("voters have nodes")
}

fn joint() -> Membership {
    Membership::new(
        vec![
            BTreeSet::from([LEADER, FOLLOWER]),
            BTreeSet::from([LEADER, FOLLOWER, LEARNER]),
        ],
        peers(),
    )
    .expect("voters have nodes")
}

fn entry(index: u64, payload: EntryPayload<ControlRequest, Membership>) -> Entry {
    Entry {
        log_id: log_id(3, LEADER, index),
        payload,
    }
}

fn meta() -> SnapshotMeta {
    SnapshotMeta {
        last_log_id: Some(log_id(3, LEADER, 1_000)),
        last_membership: StoredMembership::new(Some(log_id(2, FOLLOWER, 17)), uniform()),
    }
}

/// Decodes a vector's file into openraft's type, re-encodes it, and says whether the decoded
/// value is the one the vector was made from.
type RoundTrip = Box<dyn Fn(&str) -> (String, bool)>;

/// One checked-in vector: its file name, and how its value encodes to and decodes from JSON.
struct Vector {
    name: &'static str,
    encode: Box<dyn Fn() -> String>,
    round_trip: RoundTrip,
}

fn pretty<T: Serialize>(value: &T) -> String {
    let mut text = serde_json::to_string_pretty(value).expect("the vector encodes");
    text.push('\n');
    text
}

/// A vector whose wire type converts to `O` with `From` or `TryFrom`, and back with `From<&O>`.
fn vector<W, O>(name: &'static str, value: impl Fn() -> O + Clone + 'static) -> Vector
where
    W: Serialize + for<'de> Deserialize<'de> + for<'o> From<&'o O> + 'static,
    O: TryFrom<W> + PartialEq + 'static,
    <O as TryFrom<W>>::Error: fmt::Debug,
{
    let made = value.clone();
    Vector {
        name,
        encode: Box::new(move || pretty(&W::from(&made()))),
        round_trip: Box::new(move |text| {
            let wire: W = serde_json::from_str(text).expect("the vector decodes");
            let decoded = O::try_from(wire).expect("the vector converts");
            (pretty(&W::from(&decoded)), decoded == value())
        }),
    }
}

fn vectors() -> Vec<Vector> {
    let append = || AppendEntriesRequest::<ControlTypes> {
        vote: committed_vote(3, LEADER),
        prev_log_id: Some(log_id(3, LEADER, 40)),
        entries: vec![
            entry(41, EntryPayload::Blank),
            entry(42, EntryPayload::Normal(request())),
            entry(43, EntryPayload::Membership(joint())),
        ],
        leader_commit: Some(log_id(3, LEADER, 40)),
    };
    let chunk = || {
        let mut chunks = snapshot_chunks(
            &committed_vote(3, LEADER),
            &meta(),
            br#"{"config":{},"now":5}"#,
            SNAPSHOT_CHUNK_BYTES,
        );
        chunks.remove(0)
    };
    vec![
        vector::<LogIdWire, LogId>("log-id.json", || log_id(3, LEADER, 42)),
        vector::<VoteWire, Vote>("vote.json", || Vote::new(4, FOLLOWER)),
        vector::<VoteWire, Vote>("vote-committed.json", || committed_vote(3, LEADER)),
        vector::<MembershipWire, Membership>("membership.json", uniform),
        vector::<MembershipWire, Membership>("membership-joint.json", joint),
        vector::<SnapshotMetaWire, SnapshotMeta>("snapshot-meta.json", meta),
        vector::<SnapshotMetaWire, SnapshotMeta>("snapshot-meta-empty.json", || SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
        }),
        entry_vector("entry-command.json", || {
            entry(42, EntryPayload::Normal(request()))
        }),
        entry_vector("entry-membership.json", || {
            entry(43, EntryPayload::Membership(joint()))
        }),
        entry_vector("entry-blank.json", || entry(41, EntryPayload::Blank)),
        Vector {
            name: "append-entries.json",
            encode: Box::new(move || pretty(&AppendEntries::from(&append()))),
            round_trip: Box::new(move |text| {
                let wire: AppendEntries = serde_json::from_str(text).expect("the vector decodes");
                let decoded =
                    AppendEntriesRequest::<ControlTypes>::try_from(wire).expect("it converts");
                let again = pretty(&AppendEntries::from(&decoded));
                let made = append();
                let same = decoded.vote == made.vote
                    && decoded.prev_log_id == made.prev_log_id
                    && decoded.leader_commit == made.leader_commit
                    && decoded.entries.len() == made.entries.len();
                (again, same)
            }),
        },
        reply_vector("append-entries-reply-success.json", || {
            Ok(Some(log_id(3, LEADER, 43)))
        }),
        reply_vector("append-entries-reply-partial.json", || {
            Ok(Some(log_id(3, LEADER, 41)))
        }),
        reply_vector("append-entries-reply-empty.json", || Ok(None)),
        reply_vector("append-entries-reply-conflict.json", || {
            Err(AppendError::Conflict {
                log_id: log_id(3, LEADER, 40),
                hint: Some(ConflictHint::new(vec![
                    log_id(1, FOLLOWER, 12),
                    log_id(2, FOLLOWER, 30),
                ])),
            })
        }),
        reply_vector("append-entries-reply-conflict-no-hint.json", || {
            Err(AppendError::Conflict {
                log_id: log_id(3, LEADER, 40),
                hint: None,
            })
        }),
        reply_vector("append-entries-reply-higher-vote.json", || {
            Err(AppendError::HigherVote(Vote::new(5, FOLLOWER)))
        }),
        vector::<VoteRequestWire, VoteRequest<ControlTypes>>("vote-request.json", || {
            VoteRequest::new(Vote::new(4, FOLLOWER), Some(log_id(3, LEADER, 43)))
        }),
        vector::<VoteRequestWire, VoteRequest<ControlTypes>>("vote-request-transfer.json", || {
            VoteRequest {
                vote: Vote::new(4, FOLLOWER),
                last_log_id: Some(log_id(3, LEADER, 43)),
                leadership_transfer_from: Some(committed_vote(3, LEADER)),
            }
        }),
        vector::<VoteReply, VoteResponse<ControlTypes>>("vote-reply.json", || {
            VoteResponse::new(Vote::new(4, FOLLOWER), Some(log_id(3, LEADER, 43)), true)
        }),
        vector::<InstallSnapshotReply, SnapshotResponse<ControlTypes>>(
            "install-snapshot-reply.json",
            || SnapshotResponse::new(committed_vote(3, LEADER)),
        ),
        Vector {
            name: "install-snapshot-chunk.json",
            encode: Box::new(move || pretty(&chunk())),
            round_trip: Box::new(move |text| {
                let wire: InstallSnapshotChunk =
                    serde_json::from_str(text).expect("the vector decodes");
                let mut assembly = SnapshotAssembly::new();
                let whole = assembly
                    .push(wire.clone())
                    .expect("the chunk assembles")
                    .expect("it is the last chunk");
                let same = whole.meta == meta()
                    && whole.vote == committed_vote(3, LEADER)
                    && whole.data == br#"{"config":{},"now":5}"#;
                (pretty(&wire), same && wire == chunk())
            }),
        },
        Vector {
            name: "probe-reply.json",
            encode: Box::new(|| pretty(&probe())),
            round_trip: Box::new(|text| {
                let wire: ProbeReply = serde_json::from_str(text).expect("the vector decodes");
                (pretty(&wire), wire == probe())
            }),
        },
        Vector {
            name: "probe-reply-genesis.json",
            encode: Box::new(|| pretty(&probe_with_genesis())),
            round_trip: Box::new(|text| {
                let wire: ProbeReply = serde_json::from_str(text).expect("the vector decodes");
                (pretty(&wire), wire == probe_with_genesis())
            }),
        },
    ]
}

fn probe() -> ProbeReply {
    ProbeReply {
        node_id: "c3".into(),
        software: Software::new(Release::new(0, 10, 0), 2)
            .with_feature(Feature::Handover)
            .with_feature(Feature::Delegation),
        has_raft_state: false,
        genesis: None,
    }
}

fn probe_with_genesis() -> ProbeReply {
    ProbeReply {
        genesis: Some(
            fleetring_control::ControlConfig::new()
                .with_lease_duration(fleetring_control::Millis::new(4_000)),
        ),
        ..probe()
    }
}

fn entry_vector(name: &'static str, value: impl Fn() -> Entry + 'static) -> Vector {
    let value = std::rc::Rc::new(value);
    let made = std::rc::Rc::clone(&value);
    Vector {
        name,
        encode: Box::new(move || pretty(&EntryWire::from(&made()))),
        round_trip: Box::new(move |text| {
            let wire: EntryWire = serde_json::from_str(text).expect("the vector decodes");
            let decoded = wire.into_entry().expect("the vector converts");
            let made = value();
            let same = decoded.log_id == made.log_id
                && match (&decoded.payload, &made.payload) {
                    (EntryPayload::Blank, EntryPayload::Blank) => true,
                    (EntryPayload::Normal(a), EntryPayload::Normal(b)) => a == b,
                    (EntryPayload::Membership(a), EntryPayload::Membership(b)) => a == b,
                    _ => false,
                };
            (pretty(&EntryWire::from(&decoded)), same)
        }),
    }
}

fn reply_vector(
    name: &'static str,
    value: impl Fn() -> AppendResult<ControlTypes> + 'static,
) -> Vector {
    let value = std::rc::Rc::new(value);
    let made = std::rc::Rc::clone(&value);
    Vector {
        name,
        encode: Box::new(move || pretty(&AppendEntriesReply::from(&made()))),
        round_trip: Box::new(move |text| {
            let wire: AppendEntriesReply = serde_json::from_str(text).expect("the vector decodes");
            let decoded = AppendResult::<ControlTypes>::from(wire);
            (
                pretty(&AppendEntriesReply::from(&decoded)),
                decoded == value(),
            )
        }),
    }
}

/// Writes the vectors that have no file yet. Run it once after adding a vector, and commit the
/// new file; it never rewrites an existing one, because the vectors are append-only.
#[test]
#[ignore = "writes golden vectors; run by hand after adding one"]
fn write_new_golden_vectors() {
    let directory = directory();
    fs::create_dir_all(&directory).expect("the vector directory can be created");
    for vector in vectors() {
        let path = directory.join(vector.name);
        if !path.exists() {
            fs::write(&path, (vector.encode)()).expect("the vector can be written");
        }
    }
}

#[test]
fn every_golden_vector_round_trips() {
    let directory = directory();
    let mut checked = BTreeSet::new();
    for vector in vectors() {
        let path = directory.join(vector.name);
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("can't read {}: {error}", path.display()));
        assert_eq!(
            (vector.encode)(),
            text,
            "{} encodes to its file",
            vector.name
        );
        let (again, same) = (vector.round_trip)(&text);
        assert_eq!(again, text, "{} re-encodes byte for byte", vector.name);
        assert!(
            same,
            "{} decodes to the value it was made from",
            vector.name
        );
        checked.insert(vector.name.to_owned());
    }
    // Every file in the directory is a vector this test checks.
    for file in fs::read_dir(&directory).expect("the vector directory exists") {
        let name = file
            .expect("a directory entry")
            .file_name()
            .into_string()
            .expect("a UTF-8 file name");
        if name.ends_with(".json") {
            assert!(checked.contains(&name), "{name} has no vector");
        }
    }
}

#[test]
fn unknown_fields_are_ignored() {
    let entry = r#"{"index": 7, "term": 2, "leader_node": "0000000000000002", "kind": "command",
        "payload": {"screened": {"term": 2, "node": "0000000000000002", "added": 1},
                    "command": {"at": 5, "action": "tick"}, "added": [1, 2]},
        "added": {"nested": true}}"#;
    let wire: EntryWire = serde_json::from_str(entry).expect("unknown fields are ignored");
    let decoded = wire.into_entry().expect("it converts");
    assert_eq!(decoded.log_id, log_id(2, FOLLOWER, 7));
    let EntryPayload::Normal(request) = decoded.payload else {
        panic!("a command entry decodes to a normal payload");
    };
    assert_eq!(
        request.command_json().get(),
        r#"{"at": 5, "action": "tick"}"#
    );
    let reply = r#"{"result": "higher_vote", "vote": {"term": 5, "node": "0000000000000002",
        "committed": false, "added": "x"}, "added": 1}"#;
    let reply: AppendEntriesReply =
        serde_json::from_str(reply).expect("unknown fields are ignored");
    assert_eq!(
        AppendResult::<ControlTypes>::from(reply),
        Err(AppendError::HigherVote(Vote::new(5, FOLLOWER)))
    );
}

#[test]
fn a_command_payload_is_format_ones_command_json() {
    let command = command();
    assert!(command.recorded_by.is_some());
    let format_one = serde_json::to_string(&command).expect("the command encodes");
    let request = request();
    assert_eq!(request.command_json().get(), format_one);
    let payload = PayloadWire::from(&EntryPayload::Normal(request))
        .to_json()
        .expect("the payload encodes");
    let stored: CommandPayload = serde_json::from_str(&payload).expect("the payload decodes");
    assert_eq!(stored.command.get(), format_one);
    let screened = format!(r#"{{"term":3,"node":"{}"}}"#, raft_id_text(LEADER));
    assert_eq!(
        payload,
        format!(r#"{{"screened":{screened},"command":{format_one}}}"#)
    );
}

#[test]
fn a_raft_id_above_two_to_the_63_survives_sqlite_and_json() {
    let id = u64::MAX - 1;
    assert!(id > 1 << 63);
    let json = serde_json::to_string(&VoteWire::from(&Vote::new(1, id))).expect("it encodes");
    assert!(json.contains("\"fffffffffffffffe\""), "{json}");
    let vote: VoteWire = serde_json::from_str(&json).expect("it decodes");
    assert_eq!(Vote::from(vote), Vote::new(1, id));

    let connection = rusqlite::Connection::open_in_memory().expect("an in-memory database");
    connection
        .execute_batch("CREATE TABLE t (leader_node TEXT NOT NULL, payload TEXT NOT NULL) STRICT")
        .expect("the table is created");
    connection
        .execute(
            "INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![raft_id_text(id), json],
        )
        .expect("the row is inserted");
    let (text, payload): (String, String) = connection
        .query_row("SELECT leader_node, payload FROM t", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .expect("the row reads back");
    assert_eq!(decode_raft_id(&text).expect("it parses"), id);
    let vote: VoteWire = serde_json::from_str(&payload).expect("it decodes");
    assert_eq!(vote.node, HexId(id));
    assert_eq!(raft_id("n1"), LEADER);
}

#[test]
fn a_whole_snapshot_splits_into_chunks_and_reassembles() {
    let data: Vec<u8> = (0..(2 * SNAPSHOT_CHUNK_BYTES + 17))
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect();
    let vote = committed_vote(3, LEADER);
    let chunks = snapshot_chunks(&vote, &meta(), &data, SNAPSHOT_CHUNK_BYTES);
    assert_eq!(chunks.len(), 3);
    assert!(chunks.iter().all(|chunk| {
        BASE64.decode(&chunk.data).expect("base64").len() <= SNAPSHOT_CHUNK_BYTES
    }));
    assert_eq!(
        chunks.iter().map(|chunk| chunk.done).collect::<Vec<_>>(),
        [false, false, true]
    );

    let mut assembly = SnapshotAssembly::new();
    let mut whole = None;
    for chunk in chunks.clone() {
        whole = assembly.push(chunk).expect("chunks arrive in order");
    }
    let whole = whole.expect("the last chunk completes the snapshot");
    assert_eq!(whole.data, data);
    assert_eq!(whole.meta, meta());
    assert_eq!(whole.vote, vote);

    // A chunk out of order is refused.
    let mut assembly = SnapshotAssembly::new();
    assert!(matches!(
        assembly.push(chunks[1].clone()),
        Err(CodecError::Chunk { received: 0, .. })
    ));

    // A chunk of another vote or snapshot doesn't continue one in progress, and a later chunk at
    // offset 0 starts the snapshot over.
    let mut assembly = SnapshotAssembly::new();
    assert!(
        assembly
            .push(chunks[0].clone())
            .expect("the first")
            .is_none()
    );
    let mut other = chunks[1].clone();
    other.vote = VoteWire::from(&committed_vote(4, LEADER));
    assert!(matches!(
        assembly.push(other),
        Err(CodecError::ChunkMismatch { .. })
    ));
    let mut assembly = SnapshotAssembly::new();
    let mut whole = None;
    for chunk in [chunks[0].clone(), chunks[0].clone()]
        .into_iter()
        .chain(chunks[1..].iter().cloned())
    {
        whole = assembly
            .push(chunk)
            .expect("a restart at offset 0 is accepted");
    }
    assert_eq!(whole.expect("complete").data, data);

    // A chunk larger than a chunk may be is refused.
    let mut oversized = chunks[0].clone();
    oversized.data = BASE64.encode(vec![0_u8; SNAPSHOT_CHUNK_BYTES + 3]);
    assert!(matches!(
        SnapshotAssembly::new().push(oversized),
        Err(CodecError::ChunkTooLarge { .. })
    ));

    // An empty snapshot is one empty chunk.
    let empty = snapshot_chunks(&vote, &meta(), &[], SNAPSHOT_CHUNK_BYTES);
    assert_eq!(empty.len(), 1);
    let whole = SnapshotAssembly::new()
        .push(empty[0].clone())
        .expect("it assembles")
        .expect("it is the last chunk");
    assert!(whole.data.is_empty());
}

#[test]
fn a_membership_naming_a_voter_without_a_node_is_refused() {
    let wire = MembershipWire {
        configs: vec![vec![HexId(LEADER)]],
        nodes: BTreeMap::new(),
    };
    assert!(matches!(
        Membership::try_from(wire),
        Err(CodecError::Membership { voter: LEADER })
    ));
}
