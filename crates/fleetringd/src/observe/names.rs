//! The metric family table: every family's name, kind, labels, and help text (ADR-0034).
//!
//! [`Metrics::new`](crate::observe::Metrics::new) registers exactly these families, in this
//! order, and the tests hold every name and label to the naming scheme and the label allow-list.

/// What kind of metric a family is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Kind {
    /// A count that only goes up; the exposition adds `_total` to its name.
    Counter,
    /// A value that is set, or goes up and down.
    Gauge,
    /// A distribution of observed values, in buckets.
    Histogram,
}

/// One metric family: its registered name, kind, label names, and help text.
///
/// Counters are registered without `_total`; the OpenMetrics exposition adds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FamilySpec {
    /// The registered name.
    pub name: &'static str,
    /// The kind.
    pub kind: Kind,
    /// The label names, in the order the exposition shows them.
    pub labels: &'static [&'static str],
    /// The help text, without the final period the exposition adds.
    pub help: &'static str,
}

impl FamilySpec {
    /// The name samples carry in the exposition: the registered name, with `_total` for a
    /// counter.
    #[must_use]
    pub fn exposed_name(&self) -> String {
        match self.kind {
            Kind::Counter => format!("{}_total", self.name),
            Kind::Gauge | Kind::Histogram => self.name.to_owned(),
        }
    }
}

/// Every label name a family may use (ADR-0034 point 7).
pub const ALLOWED_LABELS: &[&str] = &[
    "service",
    "method",
    "code",
    "replica_role",
    "liveness",
    "outcome",
    "reason",
    "action",
    "state",
    "sink",
    "version",
    "protocol",
    "node_id",
    "node_role",
    "fleet",
];

/// The labels only the two info families may use.
pub const INFO_LABELS: &[&str] = &["version", "protocol", "node_id", "node_role", "fleet"];

/// The `service` label's values: a service of `auth::RPCS`, or `unknown` for any other path.
pub(crate) const SERVICES: &[&str] = &["control", "replication", "database", "unknown"];

/// The `reason` label's values on `fleetring_auth_refusals_total`.
pub(crate) const REFUSAL_REASONS: &[&str] = &[
    "scope",
    "binding",
    "unauthenticated",
    "denylist_stale",
    "no_requirement",
];

/// The `outcome` label's values on `fleetring_heartbeats_total`.
pub(crate) const OUTCOMES_HEARTBEAT: &[&str] = &["recorded", "failed"];

/// The `outcome` label's values on `fleetring_writes_total`.
pub(crate) const OUTCOMES_WRITE: &[&str] =
    &["acknowledged", "refused", "aborted", "redirected", "failed"];

/// The `outcome` label's values on `fleetring_queries_total`.
pub(crate) const OUTCOMES_QUERY: &[&str] = &["ok", "redirected", "failed"];

/// The `outcome` label's values on `fleetring_lease_renewals_total`.
pub(crate) const OUTCOMES_RENEWAL: &[&str] = &["renewed", "refused", "failed"];

/// The `outcome` label's values on `fleetring_lease_transfers_total`.
pub(crate) const OUTCOMES_TRANSFER: &[&str] = &["completed", "failed"];

/// The `outcome` label's values on `fleetring_promotions_total`.
pub(crate) const OUTCOMES_PROMOTION: &[&str] = &["completed", "superseded", "failed"];

/// The `outcome` label's values on `fleetring_control_commands_total`.
pub(crate) const OUTCOMES_CONTROL: &[&str] = &["applied", "rejected"];

/// The `outcome` label's values on `fleetring_change_deliveries_total`.
pub(crate) const OUTCOMES_DELIVERY: &[&str] = &["delivered", "failed"];

/// The `state` label's values on `fleetring_replication_followers`.
pub(crate) const FOLLOWER_STATES: &[&str] = &["caught_up", "behind", "disconnected", "reseeding"];

/// The `replica_role` label's values.
pub(crate) const REPLICA_ROLES: &[&str] = &["primary", "follower"];

/// The `liveness` label's values.
pub(crate) const LIVENESS: &[&str] = &["live", "down"];

/// The `node_role` label's values.
pub(crate) const NODE_ROLES: &[&str] = &["data", "control", "both"];

const fn family(
    name: &'static str,
    kind: Kind,
    labels: &'static [&'static str],
    help: &'static str,
) -> FamilySpec {
    FamilySpec {
        name,
        kind,
        labels,
        help,
    }
}

use crate::observe::codes::{ACTIONS, CODES};
use Kind::{Counter, Gauge, Histogram};

/// Every family the daemon registers, in exposition order.
pub const FAMILIES: &[FamilySpec] = &[
    family(
        "fleetring_build_info",
        Gauge,
        &["version", "protocol"],
        "The daemon's version and protocol version; always 1",
    ),
    family(
        "fleetring_node_info",
        Gauge,
        &["node_id", "node_role", "fleet"],
        "This node's ID, node role, and fleet name; always 1",
    ),
    family(
        "fleetring_rpc_requests",
        Counter,
        &["service", "method", "code"],
        "RPCs this node served, by final status code",
    ),
    family(
        "fleetring_rpc_duration_seconds",
        Histogram,
        &["service", "method"],
        "Time to serve unary RPCs, from request to final status",
    ),
    family(
        "fleetring_rpc_in_flight",
        Gauge,
        &["service", "method"],
        "RPCs being served now, streams included",
    ),
    family(
        "fleetring_auth_refusals",
        Counter,
        &["reason"],
        "Requests refused at the request boundary or by a handler's scope or binding check",
    ),
    family(
        "fleetring_denylist_age_seconds",
        Gauge,
        &[],
        "Seconds since the token denylist last arrived; +Inf before the first",
    ),
    family(
        "fleetring_denylist_entries",
        Gauge,
        &[],
        "Revoked tokens and signing keys on the denylist",
    ),
    family(
        "fleetring_membership_epoch",
        Gauge,
        &[],
        "The membership epoch this node last published",
    ),
    family(
        "fleetring_heartbeats",
        Counter,
        &["outcome"],
        "Heartbeats sent to the control plane, by outcome",
    ),
    family(
        "fleetring_heartbeat_duration_seconds",
        Histogram,
        &[],
        "Time from sending a heartbeat to its reply",
    ),
    family(
        "fleetring_members",
        Gauge,
        &["liveness"],
        "Members of the fleet, by liveness, as control state has them",
    ),
    family(
        "fleetring_members_draining",
        Gauge,
        &[],
        "Members marked as draining",
    ),
    family(
        "fleetring_databases",
        Gauge,
        &["replica_role"],
        "Databases this node hosts, by replica role",
    ),
    family(
        "fleetring_databases_open",
        Gauge,
        &[],
        "Databases this node has open",
    ),
    family(
        "fleetring_databases_under_replicated",
        Gauge,
        &[],
        "Databases this node is primary for with fewer replicas reachable than the replication \
         factor",
    ),
    family(
        "fleetring_leases_held",
        Gauge,
        &[],
        "Database leases this node holds",
    ),
    family(
        "fleetring_writes",
        Counter,
        &["outcome"],
        "Execute calls, by outcome",
    ),
    family(
        "fleetring_write_duration_seconds",
        Histogram,
        &[],
        "Time from receiving an acknowledged write to replying, write quorum included",
    ),
    family(
        "fleetring_queries",
        Counter,
        &["outcome"],
        "Query calls, by outcome",
    ),
    family(
        "fleetring_query_duration_seconds",
        Histogram,
        &[],
        "Time to answer a query that succeeded",
    ),
    family(
        "fleetring_replication_batches_sent",
        Counter,
        &[],
        "Batches this node sent to followers",
    ),
    family(
        "fleetring_replication_sent_bytes",
        Counter,
        &[],
        "Batch payload bytes this node sent to followers",
    ),
    family(
        "fleetring_replication_batches_applied",
        Counter,
        &[],
        "Batches this node applied as a follower",
    ),
    family(
        "fleetring_replication_snapshots_sent",
        Counter,
        &[],
        "Database snapshots this node served to followers",
    ),
    family(
        "fleetring_replication_reseeds",
        Counter,
        &[],
        "Followers this node told to re-seed from a database snapshot",
    ),
    family(
        "fleetring_replication_followers",
        Gauge,
        &["state"],
        "Followers of the databases this node is primary for, by state",
    ),
    family(
        "fleetring_lease_renewals",
        Counter,
        &["outcome"],
        "Lease renewals this node asked for, by outcome",
    ),
    family(
        "fleetring_lease_transfers",
        Counter,
        &["outcome"],
        "Leases this node handed over while draining, by outcome",
    ),
    family(
        "fleetring_promotions",
        Counter,
        &["outcome"],
        "Promotions this node carried out as candidate, by outcome",
    ),
    family(
        "fleetring_promotion_duration_seconds",
        Histogram,
        &[],
        "Time from a promotion notice to serving as primary",
    ),
    family(
        "fleetring_control_commands",
        Counter,
        &["action", "outcome"],
        "Control commands logged, by action and whether apply accepted them",
    ),
    family(
        "fleetring_control_log_index",
        Gauge,
        &[],
        "The index of the last logged control command",
    ),
    family(
        "fleetring_control_log_write_duration_seconds",
        Histogram,
        &[],
        "Time to log one group of control commands, its sync included",
    ),
    family(
        "fleetring_control_mailbox_depth",
        Gauge,
        &[],
        "Control commands waiting for the writer",
    ),
    family(
        "fleetring_control_snapshot_index",
        Gauge,
        &[],
        "The log index of the last control snapshot saved; 0 before the first",
    ),
    family(
        "fleetring_control_promotions_in_progress",
        Gauge,
        &[],
        "Databases with a promotion in progress",
    ),
    family(
        "fleetring_control_databases",
        Gauge,
        &[],
        "Databases control state records",
    ),
    family(
        "fleetring_control_leader",
        Gauge,
        &[],
        "1 if this node is the control plane's leader",
    ),
    family(
        "fleetring_change_deliveries",
        Counter,
        &["sink", "outcome"],
        "Change stream deliveries, by sink and outcome",
    ),
    family(
        "fleetring_change_resets",
        Counter,
        &["sink"],
        "Times a sink's stream skipped ahead because the log moved past it",
    ),
    family(
        "fleetring_change_backlog_batches",
        Gauge,
        &["sink"],
        "Batches published but not yet delivered, summed over databases",
    ),
    family(
        "fleetring_change_backlog_batches_max",
        Gauge,
        &["sink"],
        "The largest backlog of any one database",
    ),
];

/// The family named `name`.
pub(crate) fn spec(name: &str) -> Option<&'static FamilySpec> {
    FAMILIES.iter().find(|family| family.name == name)
}

/// The closed set of values `label` takes on the family registered as `family`, or `None` for
/// a label whose values come from elsewhere: `method` (the RPCs of `auth::RPCS`), `sink` (the
/// node's configuration), and the info labels but `node_role`.
#[must_use]
pub fn closed_values(family: &str, label: &str) -> Option<&'static [&'static str]> {
    let values = match (family, label) {
        (_, "service") => SERVICES,
        (_, "code") => CODES,
        (_, "reason") => REFUSAL_REASONS,
        (_, "action") => ACTIONS,
        (_, "state") => FOLLOWER_STATES,
        (_, "replica_role") => REPLICA_ROLES,
        (_, "liveness") => LIVENESS,
        (_, "node_role") => NODE_ROLES,
        ("fleetring_heartbeats", "outcome") => OUTCOMES_HEARTBEAT,
        ("fleetring_writes", "outcome") => OUTCOMES_WRITE,
        ("fleetring_queries", "outcome") => OUTCOMES_QUERY,
        ("fleetring_lease_renewals", "outcome") => OUTCOMES_RENEWAL,
        ("fleetring_lease_transfers", "outcome") => OUTCOMES_TRANSFER,
        ("fleetring_promotions", "outcome") => OUTCOMES_PROMOTION,
        ("fleetring_control_commands", "outcome") => OUTCOMES_CONTROL,
        ("fleetring_change_deliveries", "outcome") => OUTCOMES_DELIVERY,
        _ => return None,
    };
    Some(values)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// Words the lexicon's "Words we don't use" table rules out, as name segments.
    const BANNED_WORDS: &[&str] = &["cluster", "master", "slave", "shard", "up", "alive", "dead"];

    #[test]
    fn every_family_uses_only_allowed_labels() {
        for family in FAMILIES {
            for label in family.labels {
                assert!(
                    ALLOWED_LABELS.contains(label),
                    "{} uses label {label}, which isn't allowed",
                    family.name
                );
                let info = matches!(family.name, "fleetring_build_info" | "fleetring_node_info");
                assert_eq!(
                    INFO_LABELS.contains(label),
                    info,
                    "{} uses {label}; info labels are for the info families only",
                    family.name
                );
            }
        }
    }

    #[test]
    fn names_follow_the_scheme() {
        for family in FAMILIES {
            let name = family.name;
            assert!(name.starts_with("fleetring_"), "{name} lacks the prefix");
            assert!(
                name.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "{name} isn't lower snake case"
            );
            for segment in name.split('_') {
                assert!(
                    !BANNED_WORDS.contains(&segment),
                    "{name} uses '{segment}', which the lexicon rules out"
                );
            }
            assert!(
                !name.contains("leader") || name == "fleetring_control_leader",
                "{name} says leader outside the control plane"
            );
            assert!(
                !name.ends_with("_ms") && !name.ends_with("_milliseconds"),
                "{name} isn't in base units"
            );
            match family.kind {
                Kind::Counter => {
                    assert!(!name.ends_with("_total"), "{name}: the encoder adds _total");
                }
                Kind::Histogram => assert!(
                    name.ends_with("_seconds") || name.ends_with("_bytes"),
                    "{name} names no unit"
                ),
                Kind::Gauge => {}
            }
        }
    }

    #[test]
    fn names_are_unique() {
        let names: BTreeSet<&str> = FAMILIES.iter().map(|family| family.name).collect();
        assert_eq!(names.len(), FAMILIES.len());
    }

    #[test]
    fn closed_label_values_are_lower_snake_case() {
        for family in FAMILIES {
            for label in family.labels {
                let open = matches!(*label, "method" | "sink")
                    || (INFO_LABELS.contains(label) && *label != "node_role");
                let values = closed_values(family.name, label);
                assert_eq!(
                    values.is_none(),
                    open,
                    "{} {label}: only method, sink, and the info labels are open",
                    family.name
                );
                for value in values.unwrap_or_default() {
                    assert!(
                        value.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                        "{value} isn't lower snake case"
                    );
                }
            }
        }
    }
}
