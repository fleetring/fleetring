//! One daemon's metrics: the registry, and typed handles to every family (ADR-0034 point 5).

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_proto::PROTOCOL_VERSION;
use prometheus_client::encoding::{EncodeLabelSet, EncodeMetric, MetricEncoder};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

use crate::NodeRole;
use crate::observe::names::{
    self, FAMILIES, OUTCOMES_HEARTBEAT, OUTCOMES_PROMOTION, OUTCOMES_QUERY, OUTCOMES_RENEWAL,
    OUTCOMES_TRANSFER, OUTCOMES_WRITE, REFUSAL_REASONS,
};
use crate::tls::FleetName;

/// The first histogram bucket's upper bound, in seconds: half a millisecond.
const FIRST_BUCKET_SECONDS: f64 = 0.0005;

/// How many histogram buckets there are, each twice the last: up to about 16 s.
const BUCKETS: u16 = 16;

/// A histogram over durations, with the buckets every `_seconds` family uses.
fn duration_histogram() -> Histogram {
    Histogram::new(exponential_buckets(FIRST_BUCKET_SECONDS, 2.0, BUCKETS))
}

/// A family of duration histograms.
type Histograms<L> = Family<L, Histogram, fn() -> Histogram>;

fn histograms<L: Clone + std::hash::Hash + Eq>() -> Histograms<L> {
    Family::new_with_constructor(duration_histogram)
}

/// A count as a gauge value, saturating.
pub(crate) fn gauge_value(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

/// A count as a gauge value, saturating.
pub(crate) fn gauge_value_u64(count: u64) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

/// The `node_role` label of `role`, one of [`NODE_ROLES`](names::NODE_ROLES).
fn node_role(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Data => names::NODE_ROLES[0],
        NodeRole::Control => names::NODE_ROLES[1],
        NodeRole::Both => names::NODE_ROLES[2],
    }
}

/// What the two info families say about this node.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct NodeInfo {
    /// The node's ID.
    pub node_id: NodeId,
    /// Which roles the daemon runs.
    pub node_role: NodeRole,
    /// The fleet name from the node's certificate; `None` in plaintext.
    pub fleet: Option<FleetName>,
}

impl NodeInfo {
    /// The info of node `node_id` running `node_role` in `fleet`.
    #[must_use]
    pub fn new(node_id: NodeId, node_role: NodeRole, fleet: Option<FleetName>) -> Self {
        Self {
            node_id,
            node_role,
            fleet,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct BuildLabels {
    version: &'static str,
    protocol: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct NodeLabels {
    node_id: String,
    node_role: &'static str,
    fleet: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub(crate) struct RpcLabels {
    pub(crate) service: &'static str,
    pub(crate) method: &'static str,
    pub(crate) code: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub(crate) struct MethodLabels {
    pub(crate) service: &'static str,
    pub(crate) method: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct ReasonLabels {
    reason: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct OutcomeLabels {
    outcome: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct RoleLabels {
    replica_role: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct LivenessLabels {
    liveness: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct StateLabels {
    state: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct CommandLabels {
    action: &'static str,
    outcome: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct SinkOutcomeLabels {
    sink: String,
    outcome: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct SinkLabels {
    sink: String,
}

/// Counters by a closed set of outcomes, each created at zero so a scrape shows it.
#[derive(Debug, Clone, Default)]
struct Outcomes(Family<OutcomeLabels, Counter>);

impl Outcomes {
    fn with(values: &[&'static str]) -> Self {
        let outcomes = Self::default();
        for outcome in values {
            let _created = outcomes.0.get_or_create(&OutcomeLabels { outcome });
        }
        outcomes
    }

    fn inc(&self, outcome: &'static str) {
        self.0.get_or_create(&OutcomeLabels { outcome }).inc();
    }
}

/// The RPC families, which the trace layer updates.
#[derive(Debug, Clone)]
pub(crate) struct RpcMetrics {
    requests: Family<RpcLabels, Counter>,
    duration: Histograms<MethodLabels>,
    in_flight: Family<MethodLabels, Gauge>,
}

impl Default for RpcMetrics {
    fn default() -> Self {
        Self {
            requests: Family::default(),
            duration: histograms(),
            in_flight: Family::default(),
        }
    }
}

impl RpcMetrics {
    pub(crate) fn started(&self, labels: &MethodLabels) {
        self.in_flight.get_or_create(labels).inc();
    }

    /// Records the end of an RPC: its code, and its duration if `duration` is `Some`.
    pub(crate) fn finished(
        &self,
        labels: &MethodLabels,
        code: &'static str,
        duration: Option<Duration>,
    ) {
        self.in_flight.get_or_create(labels).dec();
        self.requests
            .get_or_create(&RpcLabels {
                service: labels.service,
                method: labels.method,
                code,
            })
            .inc();
        if let Some(duration) = duration {
            self.duration
                .get_or_create(labels)
                .observe(duration.as_secs_f64());
        }
    }
}

/// `fleetring_auth_refusals_total`.
#[derive(Debug, Clone, Default)]
pub(crate) struct AuthMetrics {
    refusals: Family<ReasonLabels, Counter>,
}

impl AuthMetrics {
    fn created() -> Self {
        let metrics = Self::default();
        for reason in REFUSAL_REASONS {
            let _created = metrics.refusals.get_or_create(&ReasonLabels { reason });
        }
        metrics
    }

    pub(crate) fn refused(&self, reason: &'static str) {
        self.refusals.get_or_create(&ReasonLabels { reason }).inc();
    }
}

/// The heartbeat families, which a data node's heartbeat task updates.
///
/// A `Default` handle counts without being registered anywhere; the daemon passes the one its
/// [`Metrics`] registered.
#[derive(Debug, Clone)]
pub struct MembershipMetrics {
    heartbeats: Outcomes,
    heartbeat_duration: Histogram,
}

impl Default for MembershipMetrics {
    fn default() -> Self {
        Self {
            heartbeats: Outcomes::with(OUTCOMES_HEARTBEAT),
            heartbeat_duration: duration_histogram(),
        }
    }
}

impl MembershipMetrics {
    /// A heartbeat the control plane recorded, `duration` after it was sent.
    pub(crate) fn recorded(&self, duration: Duration) {
        self.heartbeats.inc("recorded");
        self.heartbeat_duration.observe(duration.as_secs_f64());
    }

    /// A heartbeat that failed or timed out.
    pub(crate) fn failed(&self) {
        self.heartbeats.inc("failed");
    }
}

/// The data plane's families: writes, queries, renewals, transfers, and promotions.
#[derive(Debug, Clone)]
pub(crate) struct DataMetrics {
    writes: Outcomes,
    write_duration: Histogram,
    queries: Outcomes,
    query_duration: Histogram,
    renewals: Outcomes,
    transfers: Outcomes,
    promotions: Outcomes,
    promotion_duration: Histogram,
}

impl Default for DataMetrics {
    fn default() -> Self {
        Self {
            writes: Outcomes::with(OUTCOMES_WRITE),
            write_duration: duration_histogram(),
            queries: Outcomes::with(OUTCOMES_QUERY),
            query_duration: duration_histogram(),
            renewals: Outcomes::with(OUTCOMES_RENEWAL),
            transfers: Outcomes::with(OUTCOMES_TRANSFER),
            promotions: Outcomes::with(OUTCOMES_PROMOTION),
            promotion_duration: duration_histogram(),
        }
    }
}

impl DataMetrics {
    /// An `Execute` that ended with `outcome`; an acknowledged one took `duration`.
    pub(crate) fn write(&self, outcome: &'static str, duration: Option<Duration>) {
        self.writes.inc(outcome);
        if let Some(duration) = duration {
            self.write_duration.observe(duration.as_secs_f64());
        }
    }

    /// A `Query` that ended with `outcome`; a successful one took `duration`.
    pub(crate) fn query(&self, outcome: &'static str, duration: Option<Duration>) {
        self.queries.inc(outcome);
        if let Some(duration) = duration {
            self.query_duration.observe(duration.as_secs_f64());
        }
    }

    pub(crate) fn renewal(&self, outcome: &'static str) {
        self.renewals.inc(outcome);
    }

    pub(crate) fn transfer(&self, outcome: &'static str) {
        self.transfers.inc(outcome);
    }

    /// A promotion that ended with `outcome`; a completed one took `duration`.
    pub(crate) fn promotion(&self, outcome: &'static str, duration: Option<Duration>) {
        self.promotions.inc(outcome);
        if let Some(duration) = duration {
            self.promotion_duration.observe(duration.as_secs_f64());
        }
    }

    /// How many `Execute` calls ended with `outcome`.
    #[cfg(test)]
    pub(crate) fn writes(&self, outcome: &'static str) -> u64 {
        self.writes
            .0
            .get_or_create(&OutcomeLabels { outcome })
            .get()
    }
}

/// The replication families, which a node's streams, followers, and snapshots update.
///
/// A `Default` handle counts without being registered anywhere; the daemon passes the one its
/// [`Metrics`] registered.
#[derive(Debug, Clone, Default)]
pub struct ReplicationMetrics {
    batches_sent: Counter,
    sent_bytes: Counter,
    batches_applied: Counter,
    snapshots_sent: Counter,
    reseeds: Counter,
}

impl ReplicationMetrics {
    /// A batch of `bytes` payload bytes sent to a follower.
    pub(crate) fn batch_sent(&self, bytes: usize) {
        self.batches_sent.inc();
        self.sent_bytes
            .inc_by(u64::try_from(bytes).unwrap_or(u64::MAX));
    }

    pub(crate) fn batch_applied(&self) {
        self.batches_applied.inc();
    }

    pub(crate) fn snapshot_sent(&self) {
        self.snapshots_sent.inc();
    }

    pub(crate) fn reseed(&self) {
        self.reseeds.inc();
    }
}

/// The control plane's families, which the control writer updates.
///
/// A `Default` handle counts without being registered anywhere.
#[derive(Debug, Clone)]
pub struct ControlMetrics {
    commands: Family<CommandLabels, Counter>,
    log_index: Gauge,
    log_write_duration: Histogram,
    snapshot_index: Gauge,
}

impl Default for ControlMetrics {
    fn default() -> Self {
        Self {
            commands: Family::default(),
            log_index: Gauge::default(),
            log_write_duration: duration_histogram(),
            snapshot_index: Gauge::default(),
        }
    }
}

impl ControlMetrics {
    fn created() -> Self {
        let metrics = Self::default();
        for action in crate::observe::codes::ACTIONS {
            for outcome in names::OUTCOMES_CONTROL {
                let _created = metrics
                    .commands
                    .get_or_create(&CommandLabels { action, outcome });
            }
        }
        metrics
    }

    /// A command logged at `index`, with its action and outcome.
    pub(crate) fn command(&self, index: u64, action: &'static str, outcome: &'static str) {
        self.commands
            .get_or_create(&CommandLabels { action, outcome })
            .inc();
        self.log_index.set(gauge_value_u64(index));
    }

    pub(crate) fn log_written(&self, duration: Duration) {
        self.log_write_duration.observe(duration.as_secs_f64());
    }

    /// The log index and snapshot index recovery found.
    pub(crate) fn recovered(&self, index: u64, snapshot: u64) {
        self.log_index.set(gauge_value_u64(index));
        self.snapshot_index.set(gauge_value_u64(snapshot));
    }

    pub(crate) fn snapshot_saved(&self, index: u64) {
        self.snapshot_index.set(gauge_value_u64(index));
    }
}

/// The change stream families, which the pumps update.
///
/// A `Default` handle counts without being registered anywhere; the daemon passes the one its
/// [`Metrics`] registered.
#[derive(Debug, Clone, Default)]
pub struct ChangeMetrics {
    deliveries: Family<SinkOutcomeLabels, Counter>,
    resets: Family<SinkLabels, Counter>,
}

impl ChangeMetrics {
    /// Creates every series of `sink` at zero.
    pub(crate) fn add_sink(&self, sink: &str) {
        for outcome in names::OUTCOMES_DELIVERY {
            let _created = self.deliveries.get_or_create(&SinkOutcomeLabels {
                sink: sink.to_owned(),
                outcome,
            });
        }
        let _created = self.resets.get_or_create(&SinkLabels {
            sink: sink.to_owned(),
        });
    }

    pub(crate) fn delivered(&self, sink: &str) {
        self.delivery(sink, "delivered");
    }

    pub(crate) fn delivery_failed(&self, sink: &str) {
        self.delivery(sink, "failed");
    }

    fn delivery(&self, sink: &str, outcome: &'static str) {
        self.deliveries
            .get_or_create(&SinkOutcomeLabels {
                sink: sink.to_owned(),
                outcome,
            })
            .inc();
    }

    pub(crate) fn reset(&self, sink: &str) {
        self.resets
            .get_or_create(&SinkLabels {
                sink: sink.to_owned(),
            })
            .inc();
    }
}

/// Every gauge the sampler sets (ADR-0034 point 7): counts, maxima, and sums over the node's
/// databases and the fleet's members.
#[derive(Debug, Clone, Default)]
pub(crate) struct SampledMetrics {
    denylist_age: Gauge<f64, AtomicU64>,
    denylist_entries: Gauge,
    membership_epoch: Gauge,
    members: Family<LivenessLabels, Gauge>,
    members_draining: Gauge,
    databases: Family<RoleLabels, Gauge>,
    databases_open: Gauge,
    under_replicated: Gauge,
    leases_held: Gauge,
    followers: Family<StateLabels, Gauge>,
    mailbox_depth: Gauge,
    promotions_in_progress: Gauge,
    control_databases: Gauge,
    control_leader: Gauge,
    change_backlog: Family<SinkLabels, Gauge>,
    change_backlog_max: Family<SinkLabels, Gauge>,
}

/// One data role sample.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct DataSample {
    /// Seconds since the denylist arrived; `None` before it did.
    pub(crate) denylist_age: Option<f64>,
    pub(crate) denylist_entries: usize,
    pub(crate) membership_epoch: u64,
    pub(crate) primaries: usize,
    pub(crate) followers: usize,
    pub(crate) open: usize,
    pub(crate) under_replicated: usize,
    pub(crate) leases_held: usize,
    /// Followers by state, in [`FOLLOWER_STATES`](names::FOLLOWER_STATES) order.
    pub(crate) follower_states: [usize; 4],
    /// Each sink's name, total backlog, and largest backlog.
    pub(crate) backlogs: Vec<(String, u64, u64)>,
}

/// One control role sample.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ControlSample {
    pub(crate) live: usize,
    pub(crate) down: usize,
    pub(crate) draining: usize,
    pub(crate) databases: usize,
    pub(crate) promotions_in_progress: usize,
    pub(crate) mailbox_depth: usize,
    pub(crate) leader: bool,
}

impl SampledMetrics {
    pub(crate) fn set_data(&self, sample: &DataSample) {
        self.denylist_age
            .set(sample.denylist_age.unwrap_or(f64::INFINITY));
        self.denylist_entries
            .set(gauge_value(sample.denylist_entries));
        self.membership_epoch
            .set(gauge_value_u64(sample.membership_epoch));
        for (replica_role, count) in names::REPLICA_ROLES
            .iter()
            .zip([sample.primaries, sample.followers])
        {
            self.databases
                .get_or_create(&RoleLabels { replica_role })
                .set(gauge_value(count));
        }
        self.databases_open.set(gauge_value(sample.open));
        self.under_replicated
            .set(gauge_value(sample.under_replicated));
        self.leases_held.set(gauge_value(sample.leases_held));
        for (state, count) in names::FOLLOWER_STATES.iter().zip(sample.follower_states) {
            self.followers
                .get_or_create(&StateLabels { state })
                .set(gauge_value(count));
        }
        for (sink, total, max) in &sample.backlogs {
            let labels = SinkLabels { sink: sink.clone() };
            self.change_backlog
                .get_or_create(&labels)
                .set(gauge_value_u64(*total));
            self.change_backlog_max
                .get_or_create(&labels)
                .set(gauge_value_u64(*max));
        }
    }

    pub(crate) fn set_control(&self, sample: &ControlSample) {
        for (liveness, count) in names::LIVENESS.iter().zip([sample.live, sample.down]) {
            self.members
                .get_or_create(&LivenessLabels { liveness })
                .set(gauge_value(count));
        }
        self.members_draining.set(gauge_value(sample.draining));
        self.control_databases.set(gauge_value(sample.databases));
        self.promotions_in_progress
            .set(gauge_value(sample.promotions_in_progress));
        self.mailbox_depth.set(gauge_value(sample.mailbox_depth));
        self.control_leader.set(i64::from(sample.leader));
    }
}

/// One daemon's metrics: the registry and a handle to every family (ADR-0034).
///
/// Cloning is cheap and shares the counts. Nothing is global: two daemons in one process have
/// two `Metrics`.
#[derive(Debug, Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    rpc: RpcMetrics,
    auth: AuthMetrics,
    membership: MembershipMetrics,
    data: DataMetrics,
    replication: ReplicationMetrics,
    control: ControlMetrics,
    changes: ChangeMetrics,
    sampled: SampledMetrics,
}

/// A family the exposition always lists, with its help and type, even before it has a series:
/// a scraper sees every family a node can report from its first scrape on.
#[derive(Debug)]
struct Listed<M>(M);

impl<M: EncodeMetric> EncodeMetric for Listed<M> {
    fn encode(&self, encoder: MetricEncoder<'_>) -> Result<(), std::fmt::Error> {
        self.0.encode(encoder)
    }

    fn metric_type(&self) -> MetricType {
        self.0.metric_type()
    }
}

/// Registers families in [`FAMILIES`] order, with the table's help text.
struct Registrar {
    registry: Registry,
    next: usize,
}

impl Registrar {
    fn add<M: EncodeMetric + Send + Sync + std::fmt::Debug + 'static>(
        &mut self,
        name: &'static str,
        metric: M,
    ) {
        debug_assert_eq!(
            FAMILIES.get(self.next).map(|family| family.name),
            Some(name),
            "families are registered in the table's order"
        );
        self.next += 1;
        let help = names::spec(name).map_or("", |family| family.help);
        self.registry.register(name, help, Listed(metric));
    }
}

impl Metrics {
    /// Registers every family in [`FAMILIES`] and sets the info families.
    #[must_use]
    pub fn new(info: &NodeInfo) -> Self {
        let build = Family::<BuildLabels, Gauge>::default();
        build
            .get_or_create(&BuildLabels {
                version: env!("CARGO_PKG_VERSION"),
                protocol: format!("{}.{}", PROTOCOL_VERSION.major, PROTOCOL_VERSION.minor),
            })
            .set(1);
        let node = Family::<NodeLabels, Gauge>::default();
        node.get_or_create(&NodeLabels {
            node_id: info.node_id.to_string(),
            node_role: node_role(info.node_role),
            fleet: info
                .fleet
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
        })
        .set(1);
        let rpc = RpcMetrics::default();
        let auth = AuthMetrics::created();
        let membership = MembershipMetrics::default();
        let data = DataMetrics::default();
        let replication = ReplicationMetrics::default();
        let control = ControlMetrics::created();
        let changes = ChangeMetrics::default();
        let sampled = SampledMetrics::default();
        let mut registrar = Registrar {
            registry: Registry::default(),
            next: 0,
        };
        let r = &mut registrar;
        r.add("fleetring_build_info", build);
        r.add("fleetring_node_info", node);
        r.add("fleetring_rpc_requests", rpc.requests.clone());
        r.add("fleetring_rpc_duration_seconds", rpc.duration.clone());
        r.add("fleetring_rpc_in_flight", rpc.in_flight.clone());
        r.add("fleetring_auth_refusals", auth.refusals.clone());
        r.add(
            "fleetring_denylist_age_seconds",
            sampled.denylist_age.clone(),
        );
        r.add(
            "fleetring_denylist_entries",
            sampled.denylist_entries.clone(),
        );
        r.add(
            "fleetring_membership_epoch",
            sampled.membership_epoch.clone(),
        );
        r.add("fleetring_heartbeats", membership.heartbeats.0.clone());
        r.add(
            "fleetring_heartbeat_duration_seconds",
            membership.heartbeat_duration.clone(),
        );
        r.add("fleetring_members", sampled.members.clone());
        r.add(
            "fleetring_members_draining",
            sampled.members_draining.clone(),
        );
        r.add("fleetring_databases", sampled.databases.clone());
        r.add("fleetring_databases_open", sampled.databases_open.clone());
        r.add(
            "fleetring_databases_under_replicated",
            sampled.under_replicated.clone(),
        );
        r.add("fleetring_leases_held", sampled.leases_held.clone());
        r.add("fleetring_writes", data.writes.0.clone());
        r.add(
            "fleetring_write_duration_seconds",
            data.write_duration.clone(),
        );
        r.add("fleetring_queries", data.queries.0.clone());
        r.add(
            "fleetring_query_duration_seconds",
            data.query_duration.clone(),
        );
        r.add(
            "fleetring_replication_batches_sent",
            replication.batches_sent.clone(),
        );
        r.add(
            "fleetring_replication_sent_bytes",
            replication.sent_bytes.clone(),
        );
        r.add(
            "fleetring_replication_batches_applied",
            replication.batches_applied.clone(),
        );
        r.add(
            "fleetring_replication_snapshots_sent",
            replication.snapshots_sent.clone(),
        );
        r.add("fleetring_replication_reseeds", replication.reseeds.clone());
        r.add("fleetring_replication_followers", sampled.followers.clone());
        r.add("fleetring_lease_renewals", data.renewals.0.clone());
        r.add("fleetring_lease_transfers", data.transfers.0.clone());
        r.add("fleetring_promotions", data.promotions.0.clone());
        r.add(
            "fleetring_promotion_duration_seconds",
            data.promotion_duration.clone(),
        );
        r.add("fleetring_control_commands", control.commands.clone());
        r.add("fleetring_control_log_index", control.log_index.clone());
        r.add(
            "fleetring_control_log_write_duration_seconds",
            control.log_write_duration.clone(),
        );
        r.add(
            "fleetring_control_mailbox_depth",
            sampled.mailbox_depth.clone(),
        );
        r.add(
            "fleetring_control_snapshot_index",
            control.snapshot_index.clone(),
        );
        r.add(
            "fleetring_control_promotions_in_progress",
            sampled.promotions_in_progress.clone(),
        );
        r.add(
            "fleetring_control_databases",
            sampled.control_databases.clone(),
        );
        r.add("fleetring_control_leader", sampled.control_leader.clone());
        r.add("fleetring_change_deliveries", changes.deliveries.clone());
        r.add("fleetring_change_resets", changes.resets.clone());
        r.add(
            "fleetring_change_backlog_batches",
            sampled.change_backlog.clone(),
        );
        r.add(
            "fleetring_change_backlog_batches_max",
            sampled.change_backlog_max.clone(),
        );
        debug_assert_eq!(registrar.next, FAMILIES.len(), "every family is registered");
        Self {
            registry: Arc::new(registrar.registry),
            rpc,
            auth,
            membership,
            data,
            replication,
            control,
            changes,
            sampled,
        }
    }

    /// The exposition, in OpenMetrics text, ending with `# EOF`.
    #[must_use]
    pub fn encode(&self) -> String {
        let mut text = String::new();
        // Writing to a `String` can't fail; an error would leave what was encoded so far.
        let _encoded = prometheus_client::encoding::text::encode(&mut text, &self.registry);
        text
    }

    pub(crate) fn rpc(&self) -> &RpcMetrics {
        &self.rpc
    }

    pub(crate) fn auth(&self) -> &AuthMetrics {
        &self.auth
    }

    /// The handle a data node's heartbeat task counts with.
    #[must_use]
    pub fn membership(&self) -> &MembershipMetrics {
        &self.membership
    }

    pub(crate) fn data(&self) -> &DataMetrics {
        &self.data
    }

    /// The handle a node's replication counts with; see
    /// [`ReplicationParts`](crate::replication::ReplicationParts).
    #[must_use]
    pub fn replication(&self) -> &ReplicationMetrics {
        &self.replication
    }

    /// The handle the control writer counts with.
    #[must_use]
    pub fn control(&self) -> &ControlMetrics {
        &self.control
    }

    /// The handle a node's change streams count with; see
    /// [`ChangeParts`](crate::changes::ChangeParts).
    #[must_use]
    pub fn changes(&self) -> &ChangeMetrics {
        &self.changes
    }

    pub(crate) fn sampled(&self) -> &SampledMetrics {
        &self.sampled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info() -> NodeInfo {
        NodeInfo::new(
            "node-1".parse().expect("a node id"),
            NodeRole::Both,
            Some("test".parse().expect("a fleet name")),
        )
    }

    #[test]
    fn the_exposition_lists_every_family() {
        let text = Metrics::new(&info()).encode();
        let mut types = text
            .lines()
            .filter_map(|line| line.strip_prefix("# TYPE "))
            .filter_map(|rest| rest.split(' ').next());
        for family in FAMILIES {
            assert_eq!(
                types.next(),
                Some(family.name),
                "families appear in table order:\n{text}"
            );
        }
        assert!(
            text.contains(&format!(
                "fleetring_build_info{{version=\"{}\",protocol=\"{}.{}\"}} 1\n",
                env!("CARGO_PKG_VERSION"),
                PROTOCOL_VERSION.major,
                PROTOCOL_VERSION.minor
            )),
            "{text}"
        );
        assert!(
            text.contains(
                "fleetring_node_info{node_id=\"node-1\",node_role=\"both\",fleet=\"test\"} 1\n"
            ),
            "{text}"
        );
        assert!(
            text.contains("fleetring_writes_total{outcome=\"acknowledged\"} 0\n"),
            "{text}"
        );
        assert!(text.ends_with("# EOF\n"), "{text}");
    }

    #[test]
    fn two_registries_keep_separate_counts() {
        let first = Metrics::new(&info());
        let second = Metrics::new(&info());
        first
            .data()
            .write("acknowledged", Some(Duration::from_millis(3)));
        assert_eq!(first.data().writes("acknowledged"), 1);
        assert_eq!(second.data().writes("acknowledged"), 0);
        assert!(
            first
                .encode()
                .contains("fleetring_writes_total{outcome=\"acknowledged\"} 1\n")
        );
        assert!(
            second
                .encode()
                .contains("fleetring_writes_total{outcome=\"acknowledged\"} 0\n")
        );
    }

    #[test]
    fn the_denylist_age_is_infinite_before_the_first() {
        let metrics = Metrics::new(&info());
        metrics.sampled().set_data(&DataSample::default());
        let text = metrics.encode();
        let line = text
            .lines()
            .find(|line| line.starts_with("fleetring_denylist_age_seconds "))
            .expect("the age is exposed");
        let value = line.rsplit(' ').next().expect("a value");
        assert!(
            value.eq_ignore_ascii_case("inf") || value.eq_ignore_ascii_case("+inf"),
            "{line}"
        );
    }
}
