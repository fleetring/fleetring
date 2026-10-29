//! A data node's membership: it joins, sends heartbeats, and publishes the ring (ADR-0013,
//! ADR-0017).
//!
//! [`Membership::start`] runs two tasks:
//!
//! - **Heartbeats.** The node joins with its placement attributes, then sends a heartbeat every
//!   interval. A failed join or heartbeat is logged and tried again at the next interval, so a
//!   control node outage, or a laptop that slept, needs no manual step: the node is marked down
//!   while its heartbeats stop, and up again with the first one that arrives. If the control
//!   plane no longer knows the node, because it was removed, the node joins again. Each heartbeat
//!   reports the databases a [`DatabaseStatuses`] source lists; one the control plane doesn't
//!   know is left out and the heartbeat resent, so it never reads as an unknown node.
//! - **Ring publication.** The node follows `WatchMembership`. Each update carries the members
//!   and their liveness; when the membership epoch changes, the task rebuilds the ring with
//!   [`membership_ring`], the function the control plane builds its own ring with, so every node
//!   computes the same fingerprint. It then publishes a new [`MembershipView`] through an
//!   [`ArcSwap`]. A liveness-only update reuses the ring it already has.
//!
//! Readers never wait for an update: [`Membership::ring`] and [`Membership::view`] are
//! synchronous, take no lock, and only load the `ArcSwap`. The ring is built before it is stored,
//! so a reader sees either the previous view or the new one, whole.
//!
//! A connection can stall without failing, for example behind a network partition or across a
//! machine's sleep. A heartbeat that times out makes the client dial a new connection, and the
//! heartbeat task has the membership stream reopened when heartbeats succeed again after failing,
//! or when two heartbeat replies in a row report a later epoch than the published one.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use fleetring::{Diagnostic, Epoch, Fingerprint, NodeId, Ring, Severity};
use fleetring_control::{Liveness, Member, membership_ring};
use fleetring_proto::PROTOCOL_VERSION;
use fleetring_proto::v1::{
    DatabaseStatus, HeartbeatRequest, HeartbeatResponse, JoinRequest, LeaveRequest,
    MembershipUpdate, WatchMembershipRequest,
};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use tokio::time::{MissedTickBehavior, timeout};
use tonic::{Code, Status};

use crate::control::{ControlClient, member_from, missing_database, node_to_proto};
use crate::{MembershipSettings, OnShutdown};

/// How many heartbeat replies in a row may report a later membership epoch than the published one
/// before the membership stream counts as stalled and is reopened.
const STALE_REPLIES: u32 = 2;

/// How many joins or heartbeats in a row may time out before the connection counts as stalled and
/// the next call dials a new connection. One slow reply, on a loaded machine, doesn't make it stalled:
/// dialing again on every timeout would open a connection per heartbeat for as long as the load
/// lasts.
const STALLED_HEARTBEATS: u32 = 3;

/// The membership one node knows: the epoch, the members with their liveness, and the ring.
///
/// Views are immutable; a change publishes a new one.
#[derive(Debug, Clone, Default)]
pub struct MembershipView {
    epoch: Epoch,
    ring: Option<Arc<Ring>>,
    fingerprint: Option<Fingerprint>,
    members: Vec<(Member, Liveness)>,
}

impl MembershipView {
    /// Returns the membership epoch, or 0 before the node has heard from the control plane.
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Returns the ring over every member, live or down, or `None` while there are no members.
    #[must_use]
    pub fn ring(&self) -> Option<&Arc<Ring>> {
        self.ring.as_ref()
    }

    /// Returns the ring's fingerprint, or `None` while there is no ring.
    #[must_use]
    pub fn fingerprint(&self) -> Option<Fingerprint> {
        self.fingerprint
    }

    /// Returns every member with its liveness, in ID order.
    pub fn members(&self) -> impl ExactSizeIterator<Item = (&Member, Liveness)> {
        self.members
            .iter()
            .map(|(member, liveness)| (member, *liveness))
    }

    /// Returns whether the member with `id` is live, or `None` if it isn't a member.
    #[must_use]
    pub fn liveness(&self, id: &NodeId) -> Option<Liveness> {
        self.members
            .iter()
            .find(|(member, _)| member.id() == id)
            .map(|(_, liveness)| *liveness)
    }

    fn live_count(&self) -> usize {
        self.members
            .iter()
            .filter(|(_, liveness)| *liveness == Liveness::Live)
            .count()
    }
}

/// A data node's view of the cluster, kept current by the tasks [`Membership::start`] runs.
///
/// Cloning is cheap; clones share the published view. Each clone tracks separately which views
/// [`Membership::changed`] has returned.
#[derive(Debug, Clone)]
pub struct Membership {
    current: Arc<ArcSwap<MembershipView>>,
    updates: watch::Receiver<Arc<MembershipView>>,
}

impl Membership {
    /// Joins the cluster as `member` through `client`, and starts the heartbeat and ring
    /// publication tasks on the current Tokio runtime. Heartbeats report no databases; see
    /// [`Membership::start_reporting`].
    ///
    /// The tasks run until the returned [`MembershipTasks`] is stopped or dropped.
    ///
    /// # Panics
    ///
    /// If called outside a Tokio runtime.
    #[must_use]
    pub fn start(
        member: Member,
        client: ControlClient,
        settings: &MembershipSettings,
    ) -> (Self, MembershipTasks) {
        Self::start_reporting(member, client, settings, NoDatabases)
    }

    /// Starts membership as [`Membership::start`] does, with every heartbeat reporting the
    /// databases `databases` lists at that moment: each one's role, epoch, and position.
    ///
    /// The control plane refuses a heartbeat that names a database it doesn't know. The
    /// heartbeat is then sent again at once without that database, so the node stays live and
    /// doesn't rejoin; only an unknown node makes it rejoin.
    ///
    /// # Panics
    ///
    /// If called outside a Tokio runtime.
    #[must_use]
    pub fn start_reporting(
        member: Member,
        client: ControlClient,
        settings: &MembershipSettings,
        databases: impl DatabaseStatuses,
    ) -> (Self, MembershipTasks) {
        let (membership, publisher) = Self::unstarted();
        let interval = settings.heartbeat_interval();
        let id = member.id().to_string();
        let resubscribe = Arc::new(Notify::new());
        let heartbeat = tokio::spawn(heartbeat_loop(
            Heartbeats {
                member,
                client: client.clone(),
                interval,
                databases: Box::new(databases),
            },
            Arc::clone(&resubscribe),
            membership.updates.clone(),
        ));
        let watch = tokio::spawn(watch_loop(client.clone(), interval, resubscribe, publisher));
        let tasks = MembershipTasks {
            heartbeat,
            watch,
            leave: Leave {
                client,
                id,
                limit: interval,
                on_shutdown: settings.on_shutdown,
            },
        };
        (membership, tasks)
    }

    /// A membership with an empty view, and the publisher that updates it.
    fn unstarted() -> (Self, Publisher) {
        let empty = Arc::new(MembershipView::default());
        let current = Arc::new(ArcSwap::new(Arc::clone(&empty)));
        let (sender, updates) = watch::channel(empty);
        let publisher = Publisher {
            current: Arc::clone(&current),
            updates: sender,
        };
        (Self { current, updates }, publisher)
    }

    /// Returns the current ring, or `None` before the node has heard of any member.
    ///
    /// It never waits: it loads the published view and clones its `Arc`.
    #[must_use]
    pub fn ring(&self) -> Option<Arc<Ring>> {
        self.current.load().ring.clone()
    }

    /// Returns the current membership epoch.
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.current.load().epoch
    }

    /// Returns the current view. It never waits.
    #[must_use]
    pub fn view(&self) -> Arc<MembershipView> {
        self.current.load_full()
    }

    /// Waits for a view this handle hasn't returned yet, and returns it.
    ///
    /// Returns `None` once the publication task has stopped.
    pub async fn changed(&mut self) -> Option<Arc<MembershipView>> {
        self.updates.changed().await.ok()?;
        Some(Arc::clone(&self.updates.borrow_and_update()))
    }

    /// Waits until the current view satisfies `condition`, and returns that view.
    ///
    /// Returns `None` if the publication task stops first.
    pub async fn wait_for(
        &self,
        mut condition: impl FnMut(&MembershipView) -> bool,
    ) -> Option<Arc<MembershipView>> {
        let mut updates = self.updates.clone();
        let view = updates.wait_for(|view| condition(view)).await.ok()?;
        Some(Arc::clone(&view))
    }
}

/// Where a data node's heartbeats learn which databases it hosts (FR-029, FR-030).
///
/// The heartbeat task calls [`statuses`](Self::statuses) once per heartbeat, so it must answer
/// quickly and without waiting.
pub trait DatabaseStatuses: fmt::Debug + Send + Sync + 'static {
    /// The role, epoch, and position of every database this node hosts, one entry each.
    fn statuses(&self) -> Vec<DatabaseStatus>;
}

impl<T: DatabaseStatuses + ?Sized> DatabaseStatuses for Arc<T> {
    fn statuses(&self) -> Vec<DatabaseStatus> {
        (**self).statuses()
    }
}

/// Reports no databases.
#[derive(Debug, Clone, Copy, Default)]
struct NoDatabases;

impl DatabaseStatuses for NoDatabases {
    fn statuses(&self) -> Vec<DatabaseStatus> {
        Vec::new()
    }
}

/// The tasks behind a [`Membership`]. Dropping it stops them.
#[derive(Debug)]
pub struct MembershipTasks {
    heartbeat: JoinHandle<()>,
    watch: JoinHandle<()>,
    leave: Leave,
}

/// How a node leaves the cluster when it shuts down gracefully.
#[derive(Debug)]
struct Leave {
    client: ControlClient,
    id: String,
    /// How long the control plane gets to answer.
    limit: Duration,
    on_shutdown: OnShutdown,
}

impl MembershipTasks {
    /// Stops sending heartbeats, leaves the cluster unless the settings say to stay, stops
    /// following the membership, and waits for both tasks to end.
    ///
    /// Leaving tells the control plane that this is a planned departure rather than a failure:
    /// the member is removed at once, and the ring is rebuilt without it. If the control plane
    /// doesn't answer within one heartbeat interval, the node stops anyway and is marked down
    /// once its heartbeats are missed, as a node that stays is. Dropping the tasks instead stops
    /// them without leaving.
    pub async fn stop(mut self) {
        self.heartbeat.abort();
        let Leave {
            client,
            id,
            limit,
            on_shutdown,
        } = &self.leave;
        if *on_shutdown == OnShutdown::Leave {
            let request = LeaveRequest {
                node_id: id.clone(),
            };
            match timeout(*limit, client.leave(request)).await {
                Ok(Ok(response)) => {
                    tracing::info!(node = %id, epoch = response.membership_epoch, "left the cluster");
                }
                Ok(Err(status)) => tracing::warn!(node = %id, %status, "can't leave the cluster"),
                Err(_) => tracing::warn!(node = %id, "leaving the cluster timed out"),
            }
        }
        self.watch.abort();
        for task in [&mut self.heartbeat, &mut self.watch] {
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                tracing::error!(%error, "membership task failed");
            }
        }
    }
}

impl Drop for MembershipTasks {
    fn drop(&mut self) {
        self.heartbeat.abort();
        self.watch.abort();
    }
}

/// Builds and publishes views. Only the ring publication task holds it.
struct Publisher {
    current: Arc<ArcSwap<MembershipView>>,
    updates: watch::Sender<Arc<MembershipView>>,
}

impl fmt::Debug for Publisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Publisher")
            .field("epoch", &self.current.load().epoch)
            .finish_non_exhaustive()
    }
}

impl Publisher {
    /// Publishes the membership in `update`, rebuilding the ring only if the epoch changed.
    fn apply(&self, update: MembershipUpdate) {
        let epoch = Epoch::new(update.membership_epoch);
        let members = match members_from(update) {
            Ok(members) => members,
            Err(status) => {
                tracing::error!(%status, epoch = epoch.get(), "ignoring an invalid membership update");
                return;
            }
        };

        let previous = self.current.load_full();
        let same_members = previous.epoch == epoch
            && previous.members.len() == members.len()
            && previous
                .members
                .iter()
                .zip(&members)
                .all(|((a, _), (b, _))| a == b);
        let view = if same_members {
            MembershipView {
                members,
                ..MembershipView::clone(&previous)
            }
        } else {
            match rebuild(epoch, members) {
                Some(view) => view,
                None => return,
            }
        };
        log_change(&previous, &view, same_members);
        self.publish(view);
    }

    /// Stores `view` for readers, then wakes waiters, so a woken waiter reads it too.
    fn publish(&self, view: MembershipView) {
        let view = Arc::new(view);
        self.current.store(Arc::clone(&view));
        self.updates.send_replace(view);
    }
}

impl MembershipView {
    /// The view one membership update describes, with the ring rebuilt from it.
    ///
    /// # Errors
    ///
    /// `INTERNAL` if the update names an invalid member or its members can't form a ring.
    pub(crate) fn from_update(update: MembershipUpdate) -> Result<Self, tonic::Status> {
        let epoch = Epoch::new(update.membership_epoch);
        let members = members_from(update)
            .map_err(|status| tonic::Status::internal(format!("invalid membership: {status}")))?;
        rebuild(epoch, members)
            .ok_or_else(|| tonic::Status::internal("the membership can't form a ring"))
    }
}

/// The members an update carries, with their liveness, in ID order.
fn members_from(update: MembershipUpdate) -> Result<Vec<(Member, Liveness)>, tonic::Status> {
    let mut members = update
        .members
        .into_iter()
        .map(|member| {
            let liveness = if member.live {
                Liveness::Live
            } else {
                Liveness::Down
            };
            member_from(member.node).map(|member| (member, liveness))
        })
        .collect::<Result<Vec<_>, _>>()?;
    members.sort_by(|(a, _), (b, _)| a.id().cmp(b.id()));
    Ok(members)
}

/// Builds the view for a new membership epoch, or `None` if its members can't form a ring.
fn rebuild(epoch: Epoch, members: Vec<(Member, Liveness)>) -> Option<MembershipView> {
    if members.is_empty() {
        return Some(MembershipView {
            epoch,
            ..MembershipView::default()
        });
    }
    match membership_ring(members.iter().map(|(member, _)| member), epoch) {
        Ok(ring) => {
            for diagnostic in ring.diagnostics() {
                log_diagnostic(epoch, &diagnostic);
            }
            Some(MembershipView {
                epoch,
                fingerprint: Some(ring.fingerprint()),
                ring: Some(Arc::new(ring)),
                members,
            })
        }
        Err(error) => {
            tracing::error!(%error, epoch = epoch.get(), "can't build the ring for a membership update");
            None
        }
    }
}

fn log_diagnostic(epoch: Epoch, diagnostic: &Diagnostic) {
    match diagnostic.severity() {
        Severity::Warning => {
            tracing::warn!(epoch = epoch.get(), %diagnostic, "ring diagnostic");
        }
        _ => tracing::info!(epoch = epoch.get(), %diagnostic, "ring diagnostic"),
    }
}

fn log_change(previous: &MembershipView, view: &MembershipView, same_members: bool) {
    let down: Vec<&str> = view
        .members
        .iter()
        .filter(|(_, liveness)| *liveness == Liveness::Down)
        .map(|(member, _)| member.id().as_str())
        .collect();
    if same_members {
        tracing::info!(
            epoch = view.epoch.get(),
            live = view.live_count(),
            ?down,
            "member liveness changed",
        );
    } else {
        tracing::info!(
            epoch = view.epoch.get(),
            previous_epoch = previous.epoch.get(),
            members = view.members.len(),
            live = view.live_count(),
            ?down,
            fingerprint = ?view.fingerprint.map(Fingerprint::get),
            "ring published",
        );
    }
}

/// What the heartbeat task sends, and where.
#[derive(Debug)]
struct Heartbeats {
    member: Member,
    client: ControlClient,
    interval: Duration,
    databases: Box<dyn DatabaseStatuses>,
}

impl Heartbeats {
    /// Sends one heartbeat. A database the control plane doesn't know is dropped from the
    /// report and the heartbeat sent again, so that it never reads as an unknown node.
    async fn send(&self, id: &str) -> Result<Result<HeartbeatResponse, Status>, Elapsed> {
        let mut databases = self.databases.statuses();
        loop {
            let request = HeartbeatRequest {
                node_id: id.to_owned(),
                databases: databases.clone(),
            };
            let result = timeout(self.interval, self.client.heartbeat(request)).await;
            if let Ok(Err(status)) = &result
                && let Some(missing) = missing_database(status)
                && databases.iter().any(|status| status.database == missing)
            {
                tracing::debug!(node = %id, database = missing, "the control plane doesn't know this database; leaving it out");
                databases.retain(|status| status.database != missing);
                continue;
            }
            return result;
        }
    }
}

/// Joins, then sends a heartbeat every `interval`, forever. Never fails: every error is logged and
/// the step is tried again at the next interval.
async fn heartbeat_loop(
    heartbeats: Heartbeats,
    resubscribe: Arc<Notify>,
    published: watch::Receiver<Arc<MembershipView>>,
) {
    let Heartbeats {
        member,
        client,
        interval,
        ..
    } = &heartbeats;
    let interval = *interval;
    let id = member.id().to_string();
    let join = JoinRequest {
        protocol: Some(PROTOCOL_VERSION),
        node: Some(node_to_proto(member)),
    };
    let mut joined = false;
    let mut failing = false;
    let mut stale_replies = 0;
    let mut timeouts = 0;
    let mut ticks = tokio::time::interval(interval);
    // A node that slept, or fell behind, sends one heartbeat on waking rather than a burst.
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        if !joined {
            match timeout(interval, client.join(join.clone())).await {
                Ok(Ok(response)) => {
                    tracing::info!(node = %id, epoch = response.membership_epoch, "joined the cluster");
                    joined = true;
                    timeouts = 0;
                }
                Ok(Err(status)) => {
                    tracing::warn!(node = %id, %status, "can't join the cluster; will retry");
                    timeouts = 0;
                    failing = true;
                    continue;
                }
                Err(_) => {
                    timeouts += 1;
                    if timeouts >= STALLED_HEARTBEATS {
                        tracing::warn!(node = %id, "joining the cluster keeps timing out; will retry on a new connection");
                        client.reconnect();
                        timeouts = 0;
                    } else {
                        tracing::debug!(node = %id, "joining the cluster timed out; will retry");
                    }
                    failing = true;
                    continue;
                }
            }
        }
        let sent = heartbeats.send(&id).await;
        if sent.is_ok() {
            timeouts = 0;
        }
        match sent {
            Ok(Ok(response)) => {
                if failing {
                    tracing::info!(node = %id, "heartbeats are reaching the control plane again");
                    failing = false;
                    stale_replies = 0;
                    resubscribe.notify_one();
                    continue;
                }
                if response.membership_epoch > published.borrow().epoch.get() {
                    stale_replies += 1;
                    if stale_replies >= STALE_REPLIES {
                        tracing::info!(node = %id, "membership stream is behind; reopening it");
                        stale_replies = 0;
                        resubscribe.notify_one();
                    }
                } else {
                    stale_replies = 0;
                }
            }
            Ok(Err(status)) if status.code() == Code::NotFound => {
                tracing::warn!(node = %id, %status, "the control plane doesn't know this node; rejoining");
                joined = false;
                failing = true;
            }
            Ok(Err(status)) => {
                tracing::warn!(node = %id, %status, "heartbeat failed; will retry");
                failing = true;
            }
            Err(_) => {
                timeouts += 1;
                if timeouts >= STALLED_HEARTBEATS {
                    tracing::warn!(node = %id, "heartbeats keep timing out; will retry on a new connection");
                    client.reconnect();
                    timeouts = 0;
                } else {
                    tracing::debug!(node = %id, "heartbeat timed out; will retry");
                }
                failing = true;
            }
        }
    }
}

/// Follows `WatchMembership` and publishes every update, forever. A stream that fails or ends is
/// reopened after `interval`; one the heartbeat task reports as stalled is reopened at once.
async fn watch_loop(
    client: ControlClient,
    interval: Duration,
    resubscribe: Arc<Notify>,
    publisher: Publisher,
) {
    loop {
        // Ask for the whole membership every time, so a reopened stream starts with the current
        // liveness too.
        let request = WatchMembershipRequest { after_epoch: 0 };
        let reopen_now = match timeout(interval, client.watch_membership(request)).await {
            Ok(Ok(mut stream)) => loop {
                tokio::select! {
                    biased;
                    () = resubscribe.notified() => break true,
                    message = stream.message() => match message {
                        Ok(Some(update)) => publisher.apply(update),
                        Ok(None) => {
                            tracing::warn!("membership stream ended; reopening");
                            break false;
                        }
                        Err(status) => {
                            tracing::warn!(%status, "membership stream failed; reopening");
                            break false;
                        }
                    },
                }
            },
            Ok(Err(status)) => {
                tracing::warn!(%status, "can't open the membership stream; will retry");
                false
            }
            Err(_) => {
                tracing::warn!("opening the membership stream timed out; will retry");
                false
            }
        };
        if !reopen_now {
            tokio::time::sleep(interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use fleetring_proto::v1::{self as proto};

    use super::*;

    fn update(epoch: u64, members: &[(&str, bool)]) -> MembershipUpdate {
        MembershipUpdate {
            membership_epoch: epoch,
            members: members
                .iter()
                .map(|&(id, live)| proto::Member {
                    node: Some(proto::Node {
                        id: id.into(),
                        zone: None,
                        host: None,
                        weight: 1,
                        address: None,
                    }),
                    live,
                })
                .collect(),
        }
    }

    #[test]
    fn a_liveness_change_keeps_the_ring_and_an_epoch_change_rebuilds_it() {
        let (membership, publisher) = Membership::unstarted();
        assert!(membership.ring().is_none());

        publisher.apply(update(2, &[("b", true), ("a", true)]));
        let first = membership.ring().expect("ring after the first update");
        assert_eq!(membership.epoch().get(), 2);
        assert_eq!(first.node_count(), 2);

        publisher.apply(update(2, &[("a", true), ("b", false)]));
        let view = membership.view();
        assert!(Arc::ptr_eq(&first, view.ring().expect("ring")));
        assert_eq!(
            view.liveness(&"b".parse().expect("id")),
            Some(Liveness::Down)
        );

        publisher.apply(update(3, &[("a", true)]));
        let second = membership.ring().expect("ring after a leave");
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(second.node_count(), 1);
        assert_eq!(second.epoch().get(), 3);
    }

    #[test]
    fn readers_see_whole_views_while_updates_publish() {
        const LAST: u64 = 40;
        let (membership, publisher) = Membership::unstarted();
        // Readers and the publisher start together, and each reader reads until it sees the last
        // view, so every reader overlaps the publications however the threads are scheduled.
        let start = Arc::new(Barrier::new(3));
        let readers: Vec<_> = (0..2)
            .map(|_| {
                let membership = membership.clone();
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    loop {
                        let view = membership.view();
                        // A view's ring always belongs to the view's epoch and members.
                        if let Some(ring) = view.ring() {
                            assert_eq!(ring.epoch(), view.epoch());
                            assert_eq!(ring.node_count(), view.members().len());
                            assert_eq!(Some(ring.fingerprint()), view.fingerprint());
                        }
                        if view.epoch().get() == LAST {
                            return;
                        }
                    }
                })
            })
            .collect();
        let ids = ["a", "b", "c", "d"];
        start.wait();
        for epoch in 1..=LAST {
            let count = usize::try_from(epoch % 4).expect("small") + 1;
            let members: Vec<(&str, bool)> = ids[..count].iter().map(|id| (*id, true)).collect();
            publisher.apply(update(epoch, &members));
        }
        for reader in readers {
            reader.join().expect("reader panicked");
        }
        assert_eq!(membership.epoch().get(), LAST);
    }

    #[tokio::test]
    async fn changed_and_wait_for_follow_publications() {
        let (mut membership, publisher) = Membership::unstarted();
        let waiter = membership.clone();
        let waiting = tokio::spawn(async move {
            waiter
                .wait_for(|view| view.epoch().get() >= 2)
                .await
                .map(|view| view.epoch().get())
        });
        publisher.apply(update(1, &[("a", true)]));
        let view = membership.changed().await.expect("publisher alive");
        assert_eq!(view.epoch().get(), 1);
        publisher.apply(update(2, &[("a", true), ("b", true)]));
        assert_eq!(waiting.await.expect("waiter"), Some(2));
        drop(publisher);
        assert_eq!(membership.changed().await.map(|_| ()), Some(()));
        assert!(membership.changed().await.is_none());
    }
}
