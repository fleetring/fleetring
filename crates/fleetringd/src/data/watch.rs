//! The `WatchChanges` stream: a database's recorded row changes, from its primary (ADR-0045).
//!
//! [`open`] routes the request to the primary, checks that the database records its changes and
//! that the primary holds every change after the position asked for, admits a [`Watcher`], and
//! starts a task that feeds the stream. The task sends `ChangesOpened` first, then reads the
//! recorded transactions between its position and the write quorum's, a bounded read at a time,
//! and hands each to the stream through a bounded buffer. It ends the stream, naming the position
//! to resume after:
//!
//! - with `UNAVAILABLE` once the node stops serving the database at the epoch it opened under;
//! - with `RESOURCE_EXHAUSTED` once the subscriber lets the buffer fill;
//! - with `OUT_OF_RANGE` if the changes it is about to read were pruned.
//!
//! Before it waits to deliver a final error it drops its [`Watcher`], so a subscriber that has
//! stopped reading holds neither a watcher's place on the node nor back the release of row
//! changes.

use std::num::NonZeroUsize;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::DatabaseName;
use std::sync::Arc;

use fleetring_proto::v1 as proto;
use fleetring_proto::v1::change_update::Update;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::DatabaseError;
use crate::Registry;
use crate::auth::BoxStream;
use crate::changes::Watcher;
use crate::data::convert::{changed_transaction_to_proto, changes_opened_to_proto};
use crate::data::node::DataNode;
use crate::data::service::database_status;
use crate::engine::{CaptureMode, ChangesAfter, EngineError, Position, ReadLimit};
use crate::replication::PrimaryFeed;

/// How many transactions wait for a subscriber that hasn't read them yet (ADR-0045, point 8). A
/// subscriber further behind is cut off with `RESOURCE_EXHAUSTED`.
pub(crate) const WATCH_BUFFER: usize = 64;

/// How much one read takes: 256 transactions, or fewer once 10,000 row changes are reached,
/// always one whole transaction (ADR-0045, point 3).
pub(crate) const READ_LIMIT: ReadLimit = ReadLimit::new(
    NonZeroUsize::new(256).expect("nonzero"),
    NonZeroUsize::new(10_000).expect("nonzero"),
);

/// How often a feed with nothing to read looks whether its node still serves the database.
const CURRENCY_CHECK: Duration = Duration::from_secs(1);

/// The stream `WatchChanges` returns.
pub(crate) type ChangeStream = BoxStream<proto::ChangeUpdate>;

/// Opens a watch of `name` on `node`, after `after` or, without it, at the log's end.
///
/// # Errors
///
/// A redirect if another node is the primary, and the statuses of [`DataNode::route`];
/// `FAILED_PRECONDITION` if the database records no row changes here; `UNAVAILABLE` if the node
/// stopped serving it meanwhile or serves as many watchers as it may; `OUT_OF_RANGE` if `after`
/// is past the log's end or the primary no longer holds every change after it;
/// `INVALID_ARGUMENT` for a position that isn't one of the engine's; and `UNIMPLEMENTED` if the
/// engine can't record row changes.
pub(crate) async fn open(
    node: &DataNode,
    name: &DatabaseName,
    after: Option<Position>,
) -> Result<ChangeStream, Status> {
    // Routed as every data call is, and let go at once: a watch never holds up a drain.
    drop(node.route(name).await?);
    let id = node.id().clone();
    let Some(capture) = node.registry().capture_mode(name) else {
        return Err(Status::failed_precondition(format!(
            "{name} records no row changes on node {id}: name it in a logical change sink \
             ([[changes.sink]] with format = \"logical\") on every node to watch it"
        )));
    };
    let feed = node.replication().primary_feed(name).ok_or_else(|| {
        Status::unavailable(format!("database {name} is moving to a new primary; retry"))
    })?;
    let handle = node
        .registry()
        .open(name)
        .await
        .map_err(|error| database_status(&error))?;
    let end = handle
        .position()
        .await
        .map_err(|error| database_status(&error))?;
    let start = match after {
        None => live_start(feed.acknowledged(), end.clone()),
        Some(after) => {
            // A position that isn't the engine's is refused as such, before it is compared.
            handle
                .recorded_transactions(Some(after.clone()), after.clone(), ReadLimit::one())
                .await
                .map_err(|error| read_failed(name, &error))?;
            if Some(&after) > end.as_ref() {
                return Err(Status::out_of_range(format!(
                    "position {after} is past the end of {name}'s log ({}); a failover may have \
                     lost writes acknowledged with acks = local",
                    shown(end.as_ref())
                )));
            }
            Some(after)
        }
    };
    let watcher = node
        .change_streams()
        .watch(name, start.clone())
        .map_err(|full| Status::unavailable(format!("node {id} {full}; retry later")))?;
    // Every change after the start must be there now; each later read checks again.
    if let Some(end) = &end {
        handle
            .recorded_transactions(start.clone(), end.clone(), ReadLimit::one())
            .await
            .map_err(|error| read_failed(name, &error))?;
    }
    Ok(change_stream(Feed {
        database: name.clone(),
        node: id,
        registry: node.registry().clone(),
        feed,
        watcher: Arc::new(watcher),
        start,
        capture,
        buffer: WATCH_BUFFER,
        limit: READ_LIMIT,
    }))
}

/// Where a live tail starts: the write quorum's position, so that every position the stream
/// names, its first included, survives a failover (ADR-0018). The log's end stands in only
/// while the primary knows no quorum position yet, as just after it starts serving.
fn live_start(acknowledged: Option<Position>, end: Option<Position>) -> Option<Position> {
    acknowledged.or(end)
}

/// What a feed task needs.
pub(crate) struct Feed {
    pub(crate) database: DatabaseName,
    pub(crate) node: NodeId,
    pub(crate) registry: Registry,
    pub(crate) feed: PrimaryFeed,
    /// The watcher; only the task keeps it, so it goes when the stream ends.
    pub(crate) watcher: Arc<Watcher>,
    /// The position the stream starts after.
    pub(crate) start: Option<Position>,
    pub(crate) capture: CaptureMode,
    /// How many transactions may wait for the subscriber.
    pub(crate) buffer: usize,
    pub(crate) limit: ReadLimit,
}

/// Starts the task that feeds a stream from `feed`, and returns the stream.
///
/// The watcher's hold on the release of row changes advances only as a transaction leaves the
/// buffer for the connection, so it stays at what the subscriber has been handed, never at what
/// is merely buffered. The task owns the watcher, and drops it when the stream ends.
pub(crate) fn change_stream(feed: Feed) -> ChangeStream {
    let (updates, stream) = mpsc::channel(feed.buffer.max(1));
    let held = Arc::downgrade(&feed.watcher);
    tokio::spawn(run(feed, updates));
    Box::pin(ReceiverStream::new(stream).map(move |item| {
        if let Ok(proto::ChangeUpdate {
            update: Some(Update::Transaction(transaction)),
        }) = &item
            && let Some(watcher) = held.upgrade()
        {
            watcher.advance(Some(Position::from(transaction.position.clone())));
        }
        item
    }))
}

/// Why a feed ended, with the status the subscriber gets, if any.
enum Ended {
    /// The subscriber went away.
    Gone,
    /// The stream ends with this status.
    Failed(Status),
}

async fn run(feed: Feed, updates: mpsc::Sender<Result<proto::ChangeUpdate, Status>>) {
    let Feed {
        database,
        node,
        registry,
        mut feed,
        watcher,
        start,
        capture,
        buffer,
        limit,
    } = feed;
    let epoch = feed.epoch();
    let opened = changes_opened_to_proto(start.clone(), &node, epoch, capture);
    if updates.send(Ok(opened)).await.is_err() {
        return;
    }
    let mut position = start;
    let ended = 'feed: loop {
        // Everything the write quorum holds past the position, a bounded read at a time.
        while let Some(until) = feed
            .acknowledged()
            .filter(|until| Some(until) > position.as_ref())
        {
            let read = match registry.open(&database).await {
                Ok(handle) => {
                    handle
                        .recorded_transactions(position.clone(), until.clone(), limit)
                        .await
                }
                Err(error) => Err(error),
            };
            let transactions = match read {
                Ok(transactions) => transactions,
                Err(error) => break 'feed Ended::Failed(read_failed(&database, &error)),
            };
            // Nothing recorded up to `until`: the next read starts there. The hold stays at what
            // the subscriber was handed, so a resume after it is never refused for want of it.
            if transactions.is_empty() {
                position = Some(until);
            }
            for transaction in transactions {
                let next = transaction.position.clone();
                match updates.try_send(Ok(changed_transaction_to_proto(transaction, epoch))) {
                    Ok(()) => position = Some(next),
                    Err(TrySendError::Closed(_)) => break 'feed Ended::Gone,
                    Err(TrySendError::Full(_)) => {
                        break 'feed Ended::Failed(Status::resource_exhausted(format!(
                            "the watcher fell more than {buffer} transactions behind; {}",
                            resume(position.as_ref())
                        )));
                    }
                }
            }
        }
        let moved = tokio::select! {
            () = updates.closed() => break 'feed Ended::Gone,
            changed = feed.changed() => changed.is_err(),
            () = tokio::time::sleep(CURRENCY_CHECK) => false,
        };
        if moved || !feed.is_current() {
            break 'feed Ended::Failed(Status::unavailable(format!(
                "{database} is no longer served by node {node} at epoch {}; {}",
                epoch.get(),
                resume(position.as_ref())
            )));
        }
    };
    // Neither a watcher's place nor its hold on the release outlasts the stream's end.
    drop(watcher);
    if let Ended::Failed(status) = ended {
        // Waits for the subscriber to read what is buffered, or to go away.
        let _ = updates.send(Err(status)).await;
    }
}

/// What a watcher does next after its stream ended at `position`.
fn resume(position: Option<&Position>) -> String {
    match position {
        Some(position) => format!("resume after {position}"),
        None => "resume from the log's beginning".to_owned(),
    }
}

/// `position` as a message shows it.
fn shown(position: Option<&Position>) -> String {
    position.map_or_else(|| "its beginning".to_owned(), ToString::to_string)
}

/// The status a failed read of `database`'s recorded transactions answers: `OUT_OF_RANGE`
/// naming the oldest position held when the changes are no longer held, `INVALID_ARGUMENT` for
/// a position that isn't the engine's, and the usual database status otherwise.
pub(crate) fn read_failed(database: &DatabaseName, error: &DatabaseError) -> Status {
    match error.engine_error() {
        Some(EngineError::ChangesReleased { position, oldest }) => {
            let held = match oldest {
                Some(oldest) => {
                    format!("the oldest position its primary holds changes from is {oldest}")
                }
                None => "its primary holds none from before its log's current end".to_owned(),
            };
            Status::out_of_range(format!(
                "the changes of {database} {} are no longer held: {held}",
                ChangesAfter(position)
            ))
        }
        Some(EngineError::InvalidPosition { position }) => Status::invalid_argument(format!(
            "position {position} isn't a position of {database}'s log"
        )),
        _ => database_status(error),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use fleetring::ReplicationFactor;
    use fleetring_control::LeaseEpoch;
    use tonic::Code;

    use super::*;
    use crate::Membership;
    use crate::changes::{
        CaptureRules, ChangeParts, ChangeSettings, ChangeStreams, DatabaseMatchers,
    };
    use crate::engine::{EngineConfig, Execution, SqliteEngine, Statement, Transaction};
    use crate::replication::{
        EpochHolders, PrimaryRole, Replication, ReplicationParts, ReplicationSettings,
    };
    use crate::transport::Peers;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn name(text: &str) -> DatabaseName {
        text.parse().expect("name")
    }

    /// One node's replication, recording every database's changes, with `ledger` created and
    /// served as primary with one replica, so a write is acknowledged as soon as it commits.
    async fn primary(dir: &std::path::Path) -> Result<Replication, Box<dyn std::error::Error>> {
        let engine = SqliteEngine::new(512).expect("a valid page size");
        let parts = ReplicationParts {
            node: "a".parse()?,
            engine: Arc::new(engine.clone()),
            membership: Membership::detached(),
            peers: Arc::new(Peers::with_network(crate::Network::default())),
            addresses: Arc::new(BTreeMap::new()),
            authority: Arc::new(BTreeMap::<DatabaseName, EpochHolders>::new()),
            clock: Arc::new(crate::clock::SystemClock),
            execution: Execution::Inline,
            state_dir: dir.join("state"),
            settings: ReplicationSettings::new(),
            metrics: crate::observe::ReplicationMetrics::default(),
        };
        let every = DatabaseMatchers::new(["*".parse().expect("a star")]);
        let replication = Replication::start(parts, |sink| {
            Registry::new(engine, sink, dir.join("databases"), EngineConfig::new())
                .with_recorded_changes(CaptureRules::every(every, CaptureMode::After))
        });
        replication.registry().create(&name("ledger")).await?;
        replication.serve_primary(
            name("ledger"),
            PrimaryRole::new(ReplicationFactor::ONE, LeaseEpoch::new(1)),
        )?;
        write(&replication, "CREATE TABLE t (v INTEGER)").await?;
        Ok(replication)
    }

    async fn write(replication: &Replication, sql: &str) -> TestResult {
        replication
            .registry()
            .execute(&name("ledger"), Transaction::from(Statement::new(sql)))
            .await?;
        Ok(())
    }

    fn streams(replication: &Replication, dir: &std::path::Path) -> ChangeStreams {
        ChangeStreams::start(ChangeParts {
            sinks: Vec::new(),
            replication: replication.clone(),
            state_dir: dir.join("changes"),
            settings: ChangeSettings::new(),
            metrics: crate::observe::ChangeMetrics::default(),
        })
    }

    async fn feed_from(
        replication: &Replication,
        streams: &ChangeStreams,
        buffer: usize,
    ) -> Result<ChangeStream, Box<dyn std::error::Error>> {
        let ledger = name("ledger");
        let start = replication
            .registry()
            .open(&ledger)
            .await?
            .position()
            .await?;
        let feed = replication.primary_feed(&ledger).ok_or("a primary")?;
        assert_eq!(feed.epoch(), LeaseEpoch::new(1));
        Ok(change_stream(Feed {
            database: ledger.clone(),
            node: "a".parse()?,
            registry: replication.registry().clone(),
            feed,
            watcher: Arc::new(streams.watch(&ledger, start.clone())?),
            start,
            capture: CaptureMode::After,
            buffer,
            limit: ReadLimit::one(),
        }))
    }

    async fn next(stream: &mut ChangeStream) -> Result<Update, Status> {
        let update = tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .map_err(|_| Status::deadline_exceeded("no update"))?
            .ok_or_else(|| Status::cancelled("the stream ended"))??;
        update
            .update
            .ok_or_else(|| Status::internal("an update without a kind"))
    }

    #[tokio::test]
    async fn the_stream_opens_then_delivers_each_transaction_in_order() -> TestResult {
        let dir = tempfile::tempdir()?;
        let replication = primary(dir.path()).await?;
        let streams = streams(&replication, dir.path());
        let mut stream = feed_from(&replication, &streams, 8).await?;
        let Update::Opened(opened) = next(&mut stream).await? else {
            panic!("the first update says what the stream opened on");
        };
        assert_eq!(opened.epoch, 1);
        for value in 1..=3 {
            write(&replication, &format!("INSERT INTO t VALUES ({value})")).await?;
        }
        let mut delivered = Vec::new();
        for _ in 1..=3 {
            let Update::Transaction(transaction) = next(&mut stream).await? else {
                panic!("then transactions");
            };
            assert_eq!(transaction.changes.len(), 1);
            delivered.push(transaction);
        }
        in_order_once(&opened.position, &delivered);
        assert_eq!(streams.watchers(), 1);
        drop(stream);
        replication.stop();
        Ok(())
    }

    #[tokio::test]
    async fn a_slow_watcher_is_cut_off_with_a_position_to_resume_after_and_frees_its_place()
    -> TestResult {
        let dir = tempfile::tempdir()?;
        let replication = primary(dir.path()).await?;
        let streams = streams(&replication, dir.path());
        let mut stream = feed_from(&replication, &streams, 2).await?;
        let Update::Opened(opened) = next(&mut stream).await? else {
            panic!("opened first");
        };
        // Nothing reads the stream while ten transactions commit: the buffer of two fills.
        for value in 1..=10 {
            write(&replication, &format!("INSERT INTO t VALUES ({value})")).await?;
        }
        let mut delivered = Vec::new();
        let ended = loop {
            match next(&mut stream).await {
                Ok(Update::Transaction(transaction)) => delivered.push(transaction),
                Ok(Update::Opened(_)) => panic!("one opened message per stream"),
                Err(status) => break status,
            }
        };
        assert_eq!(ended.code(), Code::ResourceExhausted, "{ended}");
        assert!(
            delivered.len() < 10,
            "the watcher didn't get everything: {}",
            delivered.len()
        );
        in_order_once(&opened.position, &delivered);
        let last = delivered.last().ok_or("something was buffered")?;
        let resumable = Position::from(last.position.clone());
        assert!(
            ended
                .message()
                .ends_with(&format!("resume after {resumable}")),
            "the refusal names the last position delivered: {ended}"
        );
        assert!(stream.next().await.is_none(), "the stream ends");
        // The task let go of its place before it waited to deliver the error.
        assert_eq!(streams.watchers(), 0);
        replication.stop();
        Ok(())
    }

    #[tokio::test]
    async fn the_stream_ends_resumably_once_the_node_stops_serving_the_database() -> TestResult {
        let dir = tempfile::tempdir()?;
        let replication = primary(dir.path()).await?;
        let streams = streams(&replication, dir.path());
        let mut stream = feed_from(&replication, &streams, 8).await?;
        let Update::Opened(opened) = next(&mut stream).await? else {
            panic!("opened first");
        };
        replication.stop_primary(&name("ledger"));
        let ended = next(&mut stream).await.expect_err("the stream ends");
        assert_eq!(ended.code(), Code::Unavailable, "{ended}");
        let start = Position::from(opened.position);
        assert!(
            ended.message().ends_with(&format!("resume after {start}")),
            "{ended}"
        );
        replication.stop();
        Ok(())
    }

    /// Asserts that `delivered`, after a stream opened at `opened`, holds each transaction once,
    /// in order: distinct, increasing starts, each at or past the position before it.
    fn in_order_once(opened: &[u8], delivered: &[proto::ChangedTransaction]) {
        let mut after = opened.to_vec();
        let mut starts: Vec<&[u8]> = Vec::new();
        for transaction in delivered {
            assert!(
                transaction.start >= after,
                "a transaction starts at or past the position before it"
            );
            assert!(
                transaction.position > transaction.start,
                "a transaction's position is past its start"
            );
            if let Some(previous) = starts.last() {
                assert!(
                    transaction.start.as_slice() > *previous,
                    "starts are distinct and increasing"
                );
            }
            starts.push(&transaction.start);
            after.clone_from(&transaction.position);
        }
    }

    #[tokio::test]
    async fn the_hold_stays_at_what_the_watcher_was_handed_not_what_is_buffered() -> TestResult {
        let dir = tempfile::tempdir()?;
        let replication = primary(dir.path()).await?;
        let streams = streams(&replication, dir.path());
        let ledger = name("ledger");
        let mut stream = feed_from(&replication, &streams, 8).await?;
        let Update::Opened(opened) = next(&mut stream).await? else {
            panic!("opened first");
        };
        let opened = Some(Position::from(opened.position));
        for value in 1..=3 {
            write(&replication, &format!("INSERT INTO t VALUES ({value})")).await?;
        }
        // The feed buffers the three; none has been handed to the connection.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(streams.watcher_positions(&ledger), [opened]);
        let Update::Transaction(first) = next(&mut stream).await? else {
            panic!("a transaction");
        };
        assert_eq!(
            streams.watcher_positions(&ledger),
            [Some(Position::from(first.position))],
            "the hold moves as a transaction leaves the buffer"
        );
        drop(stream);
        replication.stop();
        Ok(())
    }

    #[test]
    fn a_live_tail_starts_at_the_write_quorums_position() {
        let acknowledged = Some(Position::new([1]));
        let end = Some(Position::new([2]));
        assert_eq!(
            live_start(acknowledged.clone(), end.clone()),
            acknowledged,
            "never past what a failover keeps"
        );
        assert_eq!(live_start(None, end.clone()), end);
    }

    #[test]
    fn a_range_from_the_logs_beginning_reads_as_such() {
        let error = DatabaseError::Engine(EngineError::ChangesReleased {
            position: Position::default(),
            oldest: None,
        });
        assert_eq!(
            read_failed(&name("notes"), &error).message(),
            "the changes of notes from the log's beginning are no longer held: its primary \
             holds none from before its log's current end"
        );
    }

    #[test]
    fn a_released_range_reads_as_out_of_range_naming_the_oldest_position() {
        let error = DatabaseError::Engine(EngineError::ChangesReleased {
            position: Position::new([1]),
            oldest: Some(Position::new([2])),
        });
        let status = read_failed(&name("notes"), &error);
        assert_eq!(status.code(), Code::OutOfRange);
        assert_eq!(
            status.message(),
            "the changes of notes after 01 are no longer held: the oldest position its primary \
             holds changes from is 02"
        );
    }
}
