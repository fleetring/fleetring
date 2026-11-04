//! FR-050: replication calls bound to placement (ADR-0031).
//!
//! Four nodes, `a`, `b`, `c`, and `d`, each serving its `Replication` service over TLS with its
//! own node certificate, so that every call names its sender. The database `orders` lives on
//! `a`, `b`, and `c` at R = 3, led by `a`; `d` is outside its replica set. Each node's
//! membership view is published by hand, and an [`Authority`] the test controls stands in for
//! control state.

use std::collections::BTreeMap;
use std::error::Error;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, LeaseEpoch};
use fleetring_proto::v1::replicate_message::Message;
use fleetring_proto::v1::{
    FenceRequest, FetchRequest, Member as ProtoMember, MembershipUpdate, Node as ProtoNode,
    PromoteRequest, ReplicateMessage, ReplicateOpen, SnapshotRequest,
};
use fleetringd::auth::refused_binding;
use fleetringd::clock::SystemClock;
use fleetringd::engine::{EngineConfig, Execution, Statement, Transaction};
use fleetringd::replication::{
    EpochAuthority, EpochHolders, HoldersFuture, PeerAddresses, PrimaryRole, Replication,
    ReplicationParts,
};
use fleetringd::tls::Security;
use fleetringd::transport::{Listener, Peers, incoming, replication_client};
use fleetringd::{ManualMembership, Membership, Registry};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Code, Status};

use crate::common::{
    PAGE_SIZE, TestResult, eventually, guarded, id, name, put, routes, settings, sqlite,
};
use crate::support::Tls;

const DATABASE: &str = "orders";

/// Control state as the test sets it: who leads each database, and how often nodes asked.
#[derive(Debug, Default)]
struct Authority {
    holders: Mutex<BTreeMap<DatabaseName, EpochHolders>>,
    calls: AtomicU64,
    down: AtomicBool,
}

impl Authority {
    fn set(&self, epoch: u64, holder: Option<&str>, candidate: Option<&str>, replicas: &[&str]) {
        let holders = EpochHolders {
            epoch: LeaseEpoch::new(epoch),
            holder: holder.map(id),
            candidate: candidate.map(id),
            replicas: replicas.iter().map(|replica| id(replica)).collect(),
            replication_factor: ReplicationFactor::new(3).expect("nonzero"),
        };
        self.holders
            .lock()
            .expect("authority lock")
            .insert(name(DATABASE), holders);
    }

    fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }
}

impl EpochAuthority for Authority {
    fn holders(&self, database: &DatabaseName) -> HoldersFuture<'_> {
        let answer = if self.down.load(Ordering::SeqCst) {
            Err(Status::unavailable("the control plane is down"))
        } else {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.holders
                .lock()
                .expect("authority lock")
                .get(database)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("database '{database}' does not exist")))
        };
        Box::pin(std::future::ready(answer))
    }
}

/// Where each node listens; a restarted node moves.
#[derive(Debug, Default)]
struct Addresses(Mutex<BTreeMap<NodeId, String>>);

impl PeerAddresses for Addresses {
    fn address(&self, node: &NodeId) -> Option<String> {
        self.0.lock().expect("addresses lock").get(node).cloned()
    }
}

/// A membership update listing `members`, all live.
fn members(epoch: u64, ids: &[&str]) -> MembershipUpdate {
    MembershipUpdate {
        membership_epoch: epoch,
        members: ids
            .iter()
            .map(|id| ProtoMember {
                node: Some(ProtoNode {
                    id: (*id).to_owned(),
                    zone: None,
                    host: None,
                    weight: 1,
                    address: None,
                }),
                live: true,
                draining: false,
            })
            .collect(),
    }
}

/// The membership in which `orders` lives on `a`, `b`, and `c`.
fn before() -> MembershipUpdate {
    members(1, &["a", "b", "c"])
}

/// The membership after `d` replaced `c`.
fn after() -> MembershipUpdate {
    members(2, &["a", "b", "d"])
}

struct Node {
    replication: Replication,
    membership: ManualMembership,
    root: PathBuf,
    _server: (oneshot::Sender<()>, JoinHandle<()>),
}

struct Bound {
    tls: Tls,
    authority: Arc<Authority>,
    addresses: Arc<Addresses>,
    nodes: BTreeMap<String, Node>,
    db: DatabaseName,
    _dir: TempDir,
}

impl Bound {
    /// Starts the four nodes, each seeing the membership before `d` replaced `c`, and creates
    /// `orders` on `a` at epoch 1.
    async fn start() -> Result<Self, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let tls = Tls::create(dir.path())?;
        let authority = Arc::new(Authority::default());
        authority.set(1, Some("a"), None, &["a", "b", "c"]);
        let mut bound = Self {
            tls,
            authority,
            addresses: Arc::new(Addresses::default()),
            nodes: BTreeMap::new(),
            db: name(DATABASE),
            _dir: dir,
        };
        for node in ["a", "b", "c", "d"] {
            let root = bound._dir.path().join(node);
            bound.start_node(node, root).await?;
            bound.node(node).membership.publish(before());
        }
        bound.create().await?;
        Ok(bound)
    }

    async fn start_node(&mut self, node: &str, root: PathBuf) -> TestResult {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        self.addresses
            .0
            .lock()
            .expect("addresses lock")
            .insert(id(node), listener.local_addr()?.to_string());
        let (membership, manual) = Membership::manual();
        let config = EngineConfig {
            execution: Execution::Threads,
            max_open: NonZeroUsize::new(16).ok_or("zero")?,
            page_size: PAGE_SIZE,
            ..EngineConfig::default()
        };
        let engine = sqlite();
        let databases = root.join("databases");
        let registry_engine = engine.clone();
        let replication = Replication::start(
            ReplicationParts {
                node: id(node),
                engine: Arc::new(engine),
                membership,
                peers: Arc::new(Peers::with_network(self.tls.node_network(node)?)),
                addresses: Arc::clone(&self.addresses) as Arc<dyn PeerAddresses>,
                authority: Arc::clone(&self.authority) as Arc<dyn EpochAuthority>,
                clock: Arc::new(SystemClock),
                execution: Execution::Threads,
                state_dir: root.join("replication"),
                settings: settings(),
                metrics: fleetringd::observe::ReplicationMetrics::default(),
            },
            move |sink| Registry::new(registry_engine, sink, databases, config),
        );
        let security = Security::Tls(self.tls.node_identity(node)?);
        let router = routes(replication.service());
        let (stop, stopped) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let shutdown = async {
                let _stopped = stopped.await;
            };
            let served = Server::builder()
                .add_routes(router)
                .serve_with_incoming_shutdown(incoming(Listener::Tcp(listener), security), shutdown)
                .await;
            if let Err(error) = served {
                tracing::error!(%error, "test server failed");
            }
        });
        self.nodes.insert(
            node.to_owned(),
            Node {
                replication,
                membership: manual,
                root,
                _server: (stop, server),
            },
        );
        Ok(())
    }

    /// Stops `node` and starts it again on the same directories, at a new address, with the
    /// membership before `d` replaced `c`.
    async fn restart(&mut self, node: &str) -> TestResult {
        let Node {
            replication,
            root,
            _server: (stop, server),
            ..
        } = self.nodes.remove(node).ok_or("no such node")?;
        replication.stop();
        drop(replication);
        let _stopped = stop.send(());
        server.abort();
        let _aborted = server.await;
        self.start_node(node, root).await?;
        self.node(node).membership.publish(before());
        // The primary's streams to the old instance end, and it dials the new address.
        self.replication("a")
            .serve_primary(self.db.clone(), Self::role(1))?;
        Ok(())
    }

    fn node(&self, node: &str) -> &Node {
        &self.nodes[node]
    }

    fn replication(&self, node: &str) -> &Replication {
        &self.node(node).replication
    }

    fn address(&self, node: &str) -> String {
        self.addresses
            .address(&id(node))
            .expect("a node with that name")
    }

    fn role(epoch: u64) -> PrimaryRole {
        PrimaryRole::new(
            ReplicationFactor::new(3).expect("nonzero"),
            LeaseEpoch::new(epoch),
        )
    }

    /// Creates `orders` on `a` with a table, serves it there at epoch 1, and waits until `b`
    /// and `c` hold the table.
    async fn create(&self) -> TestResult {
        let primary = self.replication("a");
        guarded("creating the database", primary.registry().create(&self.db)).await??;
        primary.serve_primary(self.db.clone(), Self::role(1))?;
        let created = Transaction::from(Statement::new(
            "CREATE TABLE kv (k INTEGER PRIMARY KEY, v BLOB)",
        ));
        guarded("the first write", async {
            while primary
                .registry()
                .execute(&self.db, created.clone())
                .await
                .is_err()
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        self.await_caught_up(&["b", "c"]).await
    }

    /// Writes row `k` through `a`, waiting for its quorum, and again while `a`'s streams are
    /// still opening and refuse it.
    async fn write(&self, k: i64) -> TestResult {
        let registry = self.replication("a").registry();
        guarded("a write", async {
            while registry.execute(&self.db, put(k, 16)).await.is_err() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
    }

    /// Waits until each of `followers` has acknowledged everything `a` committed.
    async fn await_caught_up(&self, followers: &[&str]) -> TestResult {
        let primary = self.replication("a");
        let db = &self.db;
        eventually("followers catching up", || {
            primary.primary_status(db).is_some_and(|status| {
                status.committed.is_some()
                    && followers.iter().all(|follower| {
                        status.followers.iter().any(|progress| {
                            progress.node.as_str() == *follower
                                && progress.reported
                                && progress.acknowledged == status.committed
                        })
                    })
            })
        })
        .await
    }

    /// A `Replication` client to `to`, acting as the node `as_node` with its certificate.
    fn client(
        &self,
        as_node: &str,
        to: &str,
    ) -> Result<
        fleetring_proto::v1::replication_client::ReplicationClient<tonic::transport::Channel>,
        Box<dyn Error>,
    > {
        let network = self.tls.node_network(as_node)?;
        Ok(replication_client(network.channel(&self.address(to))?))
    }

    /// Opens a `Replicate` stream from `from` to `to` for `orders` at `epoch`; `Ok` if `to`
    /// accepted it.
    async fn replicate(&self, from: &str, to: &str, epoch: u64) -> Result<(), Status> {
        let mut client = self
            .client(from, to)
            .map_err(|error| Status::internal(error.to_string()))?;
        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(ReplicateMessage {
                message: Some(Message::Open(ReplicateOpen {
                    database: DATABASE.to_owned(),
                    epoch,
                    epoch_start: None,
                })),
            })
            .await
            .map_err(|_| Status::internal("can't send the open message"))?;
        let mut request = tonic::Request::new(ReceiverStream::new(receiver));
        request.metadata_mut().insert(
            fleetringd::replication::PRIMARY_METADATA_KEY,
            MetadataValue::try_from(from).map_err(|_| Status::internal("bad metadata"))?,
        );
        let response = guarded("the stream opening", client.replicate(request))
            .await
            .map_err(|error| Status::deadline_exceeded(error.to_string()))?;
        // Keep the sender alive until the answer is in.
        drop(sender);
        response.map(|_| ())
    }

    async fn fence(&self, from: &str, to: &str, epoch: u64, candidate: &str) -> Result<(), Status> {
        let mut client = self
            .client(from, to)
            .map_err(|error| Status::internal(error.to_string()))?;
        client
            .fence(FenceRequest {
                database: DATABASE.to_owned(),
                epoch,
                candidate: candidate.to_owned(),
            })
            .await
            .map(|_| ())
    }

    async fn snapshot(&self, from: &str, to: &str, epoch: u64) -> Result<(), Status> {
        let mut client = self
            .client(from, to)
            .map_err(|error| Status::internal(error.to_string()))?;
        client
            .snapshot(SnapshotRequest {
                database: DATABASE.to_owned(),
                epoch,
            })
            .await
            .map(|_| ())
    }

    fn highest_epoch(&self, node: &str) -> Option<u64> {
        self.replication(node)
            .highest_epoch(&self.db)
            .map(LeaseEpoch::get)
    }

    fn refusals(&self, node: &str) -> u64 {
        self.replication(node).binding_refusals()
    }
}

/// Asserts that `result` is a binding refusal naming `binding`.
fn assert_refused(result: Result<(), Status>, binding: &str) -> Result<Status, Box<dyn Error>> {
    let status = result.err().ok_or("the call was accepted")?;
    assert_eq!(status.code(), Code::PermissionDenied, "{status}");
    assert_eq!(refused_binding(&status), Some(binding), "{status}");
    Ok(status)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_from_a_node_that_does_not_hold_the_epoch_is_refused() -> TestResult {
    let bound = Bound::start().await?;
    // `d` claims the next epoch on `b`, as a stolen key would to fence the real primary out.
    let refused = assert_refused(bound.replicate("d", "b", 2).await, "holder or candidate")?;
    assert_eq!(
        refused.message(),
        "Replicate refused for node 'd': not the holder of database 'orders' at epoch 2"
    );
    assert_refused(bound.replicate("d", "b", 1).await, "holder or candidate")?;
    assert_eq!(bound.highest_epoch("b"), Some(1), "b recorded nothing");
    assert_eq!(bound.refusals("b"), 2);
    let status = bound
        .replication("b")
        .follower_status(&bound.db)
        .ok_or("b follows orders")?;
    assert_eq!(status.binding_refusals, 2);
    // `a`'s stream to `b` goes on.
    bound.write(1).await?;
    bound.await_caught_up(&["b", "c"]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_to_a_node_outside_the_replica_set_is_refused() -> TestResult {
    let bound = Bound::start().await?;
    let refused = assert_refused(bound.replicate("a", "d", 1).await, "replica")?;
    assert_eq!(
        refused.message(),
        "Replicate refused for node 'a': not a replica of database 'orders'"
    );
    assert_eq!(bound.highest_epoch("d"), None, "d recorded nothing");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_holder_is_verified_once_and_then_trusted_from_the_record() -> TestResult {
    let bound = Bound::start().await?;
    // `b` and `c` each asked once, when `a`'s first stream reached them.
    assert_eq!(bound.authority.calls(), 2);
    // Serving again reopens every stream at the same epoch.
    bound
        .replication("a")
        .serve_primary(bound.db.clone(), Bound::role(1))?;
    bound.write(1).await?;
    bound.await_caught_up(&["b", "c"]).await?;
    assert_eq!(bound.authority.calls(), 2, "the record answered");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fence_naming_another_candidate_than_the_caller_is_refused() -> TestResult {
    let bound = Bound::start().await?;
    bound.authority.set(2, None, Some("b"), &["a", "b", "c"]);
    let refused = assert_refused(bound.fence("d", "c", 2, "b").await, "self")?;
    assert_eq!(
        refused.message(),
        "Fence refused for node 'd': the request names node 'b'"
    );
    assert_eq!(bound.highest_epoch("c"), Some(1), "c recorded nothing");
    Ok(())
}

/// The fence a stolen key would send to take a database's primary down: a non-replica fences
/// the primary at a higher epoch, for a promotion control state never started.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fence_for_a_promotion_control_does_not_know_is_refused() -> TestResult {
    let bound = Bound::start().await?;
    let refused = assert_refused(bound.fence("d", "a", 2, "d").await, "candidate")?;
    assert_eq!(
        refused.message(),
        "Fence refused for node 'd': not the candidate of database 'orders' at epoch 2"
    );
    // So does a replica, for a promotion control state doesn't name it the candidate of.
    assert_refused(bound.fence("b", "a", 2, "b").await, "candidate")?;
    let serving = bound
        .replication("a")
        .serving(&bound.db)
        .ok_or("a stepped down")?;
    assert_eq!(serving.epoch.get(), 1);
    assert!(bound.highest_epoch("a") < Some(2), "a recorded nothing");
    bound.write(1).await?;
    bound.await_caught_up(&["b", "c"]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetch_from_a_node_other_than_the_fenced_candidate_is_refused() -> TestResult {
    let bound = Bound::start().await?;
    bound.write(1).await?;
    // A promotion to epoch 2 with `b` as its candidate; `b` fences `c`.
    bound.authority.set(2, None, Some("b"), &["a", "b", "c"]);
    bound.fence("b", "c", 2, "b").await?;
    let fetch = |from: &str| {
        let client = bound.client(from, "c");
        async move {
            client
                .map_err(|error| Status::internal(error.to_string()))?
                .fetch(FetchRequest {
                    database: DATABASE.to_owned(),
                    epoch: 2,
                    after: Vec::new(),
                })
                .await
                .map(|_| ())
        }
    };
    for other in ["a", "d"] {
        let refused = assert_refused(fetch(other).await, "candidate")?;
        assert_eq!(
            refused.message(),
            format!(
                "Fetch refused for node '{other}': not the candidate of database 'orders' at \
                 epoch 2"
            )
        );
    }
    fetch("b").await?;
    Ok(())
}

/// The read a stolen key would make of a database it doesn't host: a non-replica pulls a
/// snapshot, from the primary or from a follower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_for_a_node_that_is_not_a_follower_is_refused() -> TestResult {
    let bound = Bound::start().await?;
    let refused = assert_refused(bound.snapshot("d", "a", 1).await, "follower")?;
    assert_eq!(
        refused.message(),
        "Snapshot refused for node 'd': not a follower of database 'orders' on this node"
    );
    assert_refused(bound.snapshot("d", "a", 2).await, "follower")?;
    assert_refused(bound.snapshot("d", "b", 1).await, "follower")?;
    // A follower gets one from its primary, and a stale answer from a node that isn't it.
    bound.snapshot("b", "a", 1).await?;
    let stale = bound
        .snapshot("c", "b", 1)
        .await
        .err()
        .ok_or("b serves no snapshot")?;
    assert_eq!(stale.code(), Code::FailedPrecondition, "{stale}");
    assert_eq!(refused_binding(&stale), None);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_for_a_promotion_naming_another_candidate_is_refused() -> TestResult {
    let bound = Bound::start().await?;
    bound.authority.set(2, None, Some("b"), &["a", "b", "c"]);
    let promote = |to: &str| {
        let client = bound.client("d", to);
        async move {
            client
                .map_err(|error| Status::internal(error.to_string()))?
                .promote(PromoteRequest {
                    database: DATABASE.to_owned(),
                    epoch: 2,
                    replication_factor: 3,
                })
                .await
                .map(|_| ())
        }
    };
    let refused = assert_refused(promote("c").await, "candidate")?;
    assert_eq!(
        refused.message(),
        "Promote refused for node 'c': not the candidate of database 'orders' at epoch 2"
    );
    assert!(
        !bound
            .replication("c")
            .promotions()
            .borrow()
            .contains_key(&bound.db)
    );
    promote("b").await?;
    assert!(
        bound
            .replication("b")
            .promotions()
            .borrow()
            .contains_key(&bound.db)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_moved_by_a_membership_change_is_accepted_during_the_overlap() -> TestResult {
    let bound = Bound::start().await?;
    bound.write(1).await?;
    // Control state moves `orders` from `c` to `d`; `a` publishes the new ring before `d` does,
    // so `a` streams to `d`, which doesn't yet see itself as a replica. `d` asks `a` for its
    // first copy, and `a`, already on the new ring, counts it a follower.
    bound.authority.set(1, Some("a"), None, &["a", "b", "d"]);
    bound.node("a").membership.publish(after());
    bound.write(2).await?;
    bound.await_caught_up(&["b", "d"]).await?;

    // `c` publishes the new ring before `a` leads at the next epoch on the old one: `c`, out
    // of the replica set on its current ring, is in it on the ring before.
    bound.node("a").membership.publish(before());
    bound.node("c").membership.publish(after());
    bound.authority.set(2, Some("a"), None, &["a", "b", "d"]);
    bound
        .replication("a")
        .serve_primary(bound.db.clone(), Bound::role(2))?;
    bound.write(3).await?;
    bound.await_caught_up(&["b", "c"]).await?;
    assert_eq!(bound.highest_epoch("c"), Some(2));
    for node in ["a", "b", "c", "d"] {
        assert_eq!(bound.refusals(node), 0, "{node} refused a call");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_known_holder_is_accepted_without_the_control_plane_and_an_unknown_one_waits()
-> TestResult {
    let mut bound = Bound::start().await?;
    bound.write(1).await?;
    bound.authority.down.store(true, Ordering::SeqCst);
    // `b` restarts while the control plane is down, and takes `a`'s stream back from its
    // record.
    bound.restart("b").await?;
    bound.write(2).await?;
    bound.await_caught_up(&["b", "c"]).await?;
    assert_eq!(bound.refusals("b"), 0);

    // A holder of a new epoch that `b` hasn't recorded waits for the control plane.
    let waiting = bound
        .replicate("a", "b", 2)
        .await
        .err()
        .ok_or("accepted without control state")?;
    assert_eq!(waiting.code(), Code::Unavailable, "{waiting}");
    assert_eq!(bound.highest_epoch("b"), Some(1));
    bound.authority.set(2, Some("a"), None, &["a", "b", "c"]);
    bound.authority.down.store(false, Ordering::SeqCst);
    bound.replicate("a", "b", 2).await?;
    assert_eq!(bound.highest_epoch("b"), Some(2));
    Ok(())
}
