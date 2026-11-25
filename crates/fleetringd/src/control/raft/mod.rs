//! The control group's Raft: openraft's types for the control log (ADR-0035, ADR-0040).
//!
//! - **openraft replicates the control log** (ADR-0035 point 1). Its log storage and state
//!   machine sit over `control.db` format 2, and `fleetring-control` doesn't change: `apply`,
//!   `State` and `Command` keep their serde forms.
//! - **A control node's Raft ID** is the XXH3-64 hash of its fleet node ID (ADR-0035 point 2,
//!   [`raft_id`]), written as 16 lowercase hex digits wherever it is stored or sent (ADR-0040
//!   point 3, [`raft_id_text`]).
//! - **One code path** (ADR-0035 point 3): a single control node is a group of one voter, and the
//!   group's membership is Raft configuration, not control state.
//! - **`control.db` format 2** (ADR-0035 point 4) holds the Raft log, its metadata, and control
//!   snapshots, in FleetRing's own encodings (ADR-0040 point 3, `codec`): openraft's types reach
//!   neither the disk nor the wire, so a later openraft release changes code, not formats.
//! - **openraft is pinned to `=0.10.1`** without default features and without `serde` (ADR-0040
//!   point 1). [`ControlRuntime`] is its runtime in every build: Tokio's timers and channels, and
//!   randomness from a generator the environment seeds.
//! - **The leader screens each command** against the state its own log will reach, and every
//!   proposal carries the leader it was screened under ([`ControlRequest::screened`], ADR-0040
//!   point 2).
//!
//! # openraft 0.10.1's interfaces
//!
//! As checked against the crate's source; later pull requests implement them.
//!
//! - `RaftLogReader<C>`: `try_get_log_entries(&mut self, range: impl RangeBounds<u64>) ->
//!   Result<Vec<C::Entry>, io::Error>` and `read_vote(&mut self) -> Result<Option<Vote>,
//!   io::Error>`; `limited_get_log_entries`, `get_key_log_ids`, `entries_stream` and
//!   `leader_bounded_stream` have defaults.
//! - `RaftLogStorage<C>`: `type LogReader`; `get_log_state`, `get_log_reader`,
//!   `save_vote(&Vote)`, `save_committed(Option<LogId>)` and `read_committed` (both defaulted),
//!   `append(entries: impl IntoIterator<Item = C::Entry>, callback: IOFlushed<C>)`, where the
//!   callback reports the entries durable, `truncate_after(Option<LogId>)` (exclusive), and
//!   `purge(LogId)` (inclusive). Every error is `io::Error`.
//! - `RaftStateMachine<C>`: `type SnapshotData` (moved here and onto the network in 0.10, out of
//!   `RaftTypeConfig`) and `type SnapshotBuilder`; `applied_state() -> (Option<LogId>,
//!   StoredMembership)`; `apply(entries)`, where `entries` is a `Stream` of
//!   `Result<EntryResponder<C>, io::Error>`, an entry paired with its optional responder;
//!   `get_snapshot_builder`, `try_create_snapshot_builder(force)` (defaulted),
//!   `install_snapshot(&SnapshotMeta, SnapshotData)`, and `get_current_snapshot`.
//! - `RaftSnapshotBuilder<C>`: `type SnapshotData`; `build_snapshot() -> Snapshot`.
//! - `RaftNetworkFactory<C>`: `type Network`; `new_client(target, &node)`, with
//!   `new_heartbeat_client` and `new_snapshot_client` defaulting to it.
//! - `RaftNetworkV2<C>`: `type SnapshotData`; `append_entries(AppendEntriesRequest, RPCOption) ->
//!   AppendResult`, where `AppendResult` is `Result<Option<LogId>, AppendError>` and
//!   `AppendError` is `Conflict { log_id, hint }` or `HigherVote(vote)`; `vote` and `pre_vote`
//!   (`VoteRequest` to `VoteResponse`); `full_snapshot(vote, Snapshot, cancel, RPCOption) ->
//!   SnapshotResponse`, which hands the network the whole snapshot to send as it chooses; and
//!   `transfer_leader`, `stream_append` and `backoff`, which have defaults.
//! - `AsyncRuntime` (`openraft::rt`, from `openraft-rt`): associated types `JoinError`,
//!   `JoinHandle<T>`, `Sleep`, `Instant`, `TimeoutError`, `Timeout<R, F>`, `ThreadLocalRng:
//!   rand::Rng`, `Mpsc`, `Watch`, `Oneshot`, and `Mutex<T>`; functions `spawn`, `sleep`,
//!   `sleep_until`, `timeout`, `timeout_at`, `is_panic`, `thread_rng` (the RNG hook, which
//!   openraft calls for every election-timeout draw), `new` and `block_on` (unused by openraft),
//!   and `spawn_blocking`, which by default spawns an OS thread of its own per call.
//! - Writes: `Raft::write(data)` returns a builder taking `.with_leader(leader_id)` (the same as
//!   `WriteOption::expect_leader`) and `.responder(responder)`, and sends without waiting.
//!   `Raft::ensure_linearizable(ReadPolicy::ReadIndex)` reads, and
//!   `Raft::trigger().transfer_leader(to)` hands leadership over. `Raft` takes the state machine
//!   as a second type parameter: `Raft<C, SM>`.

pub mod codec;
mod id;
mod runtime;
mod types;

pub use id::{ParseRaftIdError, RaftIdError, check_raft_id, parse_raft_id, raft_id, raft_id_text};
pub use runtime::{ControlInstant, ControlRng, ControlRuntime, RaftSeed};
pub use types::{ControlPeer, ControlRequest, ControlResponse, LeaderRef};

/// A control node's Raft ID: the XXH3-64 hash of its fleet node ID (ADR-0035 point 2).
pub type RaftId = u64;

/// A log ID of the control group: the leader's term and Raft ID, and the entry's index.
pub type LogId = openraft::type_config::alias::LogIdOf<ControlTypes>;

/// A vote of the control group.
pub type Vote = openraft::type_config::alias::VoteOf<ControlTypes>;

/// An entry of the control group's log.
pub type Entry = openraft::type_config::alias::EntryOf<ControlTypes>;

/// The control group's membership: its voter configs and every member's peer.
pub type Membership = openraft::type_config::alias::MembershipOf<ControlTypes>;

/// A membership and the log ID of the entry that holds it.
pub type StoredMembership = openraft::type_config::alias::StoredMembershipOf<ControlTypes>;

/// A control snapshot's metadata: its last log ID and its membership.
pub type SnapshotMeta = openraft::type_config::alias::SnapshotMetaOf<ControlTypes>;

openraft::declare_raft_types!(
    /// openraft's type configuration for the control group.
    pub ControlTypes:
        D = ControlRequest,
        R = ControlResponse,
        NodeId = RaftId,
        Node = ControlPeer,
        AsyncRuntime = ControlRuntime,
);
