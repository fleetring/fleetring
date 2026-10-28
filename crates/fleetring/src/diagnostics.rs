//! Ring health checks (FR-015): topology and balance problems an operator should see before they
//! cost durability or capacity.

use std::fmt;
use std::num::NonZeroU32;

use crate::strategy::StrategyIndex;
use crate::{NodeId, ReplicationFactor, Ring, StableHasher, Strategy, Zone};

/// Basis points in a whole: 10 000 bps is 100%.
const WHOLE_BPS: u128 = 10_000;

/// Below this many tokens a vnode-ring node's balance error exceeds about ±12.5% (1/√64).
const MIN_TOKENS: u64 = 64;

/// Keys [`Ring::diagnostics`] samples to estimate replica shares. Enough for a sampling error
/// near 0.2 percentage points on a zone, well under the skew thresholds.
const DIAGNOSTIC_SAMPLES: NonZeroU32 = NonZeroU32::new(10_000).expect("10 000 is nonzero");

/// How serious a [`Diagnostic`] is. Sorts most serious first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Severity {
    /// Durability or balance suffers. Act on it.
    Warning,
    /// Worth knowing, and often expected in small clusters.
    Info,
}

/// A topology or balance problem found by [`Ring::diagnostics`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Diagnostic {
    /// The ring has fewer nodes than its replication factor, so every key has fewer
    /// replicas than configured.
    UnderReplicated {
        /// Nodes in the ring.
        nodes: usize,
        /// The ring's replication factor.
        replication_factor: ReplicationFactor,
    },
    /// The ring has fewer hosts than its replication factor, so some replicas share a machine.
    FewerHostsThanReplicas {
        /// Distinct hosts, counting each unlabeled node as its own host.
        hosts: usize,
        /// The ring's replication factor.
        replication_factor: ReplicationFactor,
    },
    /// The ring has fewer zones than its replication factor, so some replicas share a zone.
    /// Expected in single-zone home labs.
    FewerZonesThanReplicas {
        /// Distinct zones, counting the default zone.
        zones: usize,
        /// The ring's replication factor.
        replication_factor: ReplicationFactor,
    },
    /// A zone receives a much larger share of replicas than of weight, because spreading
    /// replicas across zones forces them onto it (ADR-0006). Its nodes fill up first.
    ZoneReplicaSkew {
        /// The zone, or `None` for the default zone.
        zone: Option<Zone>,
        /// The zone's share of the ring's weight, in basis points.
        weight_share_bps: u32,
        /// The zone's estimated share of all replicas, in basis points.
        replica_share_bps: u32,
    },
    /// A node in a vnode ring has too few tokens to balance well.
    LowTokenCount {
        /// The node.
        node: NodeId,
        /// The node's tokens: its weight times the ring's tokens per weight.
        tokens: u64,
    },
}

impl Diagnostic {
    /// Returns how serious the problem is.
    #[must_use]
    pub fn severity(&self) -> Severity {
        match self {
            Self::FewerZonesThanReplicas { .. } => Severity::Info,
            Self::UnderReplicated { .. }
            | Self::FewerHostsThanReplicas { .. }
            | Self::ZoneReplicaSkew { .. }
            | Self::LowTokenCount { .. } => Severity::Warning,
        }
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnderReplicated {
                nodes,
                replication_factor,
            } => write!(
                f,
                "ring has {} but replicates {replication_factor} ways, so every key is under-replicated",
                Count(*nodes, "node")
            ),
            Self::FewerHostsThanReplicas {
                hosts,
                replication_factor,
            } => write!(
                f,
                "ring has {} but replicates {replication_factor} ways, so some replicas share a host",
                Count(*hosts, "host")
            ),
            Self::FewerZonesThanReplicas {
                zones,
                replication_factor,
            } => write!(
                f,
                "ring has {} but replicates {replication_factor} ways, so some replicas share a zone",
                Count(*zones, "zone")
            ),
            Self::ZoneReplicaSkew {
                zone,
                weight_share_bps,
                replica_share_bps,
            } => {
                match zone {
                    Some(zone) => write!(f, "zone '{zone}'")?,
                    None => f.write_str("the default zone")?,
                }
                write!(
                    f,
                    " holds {} of the weight but {} of the replicas",
                    Percent(*weight_share_bps),
                    Percent(*replica_share_bps)
                )
            }
            Self::LowTokenCount { node, tokens } => write!(
                f,
                "node '{node}' has {}; vnode balance needs at least {MIN_TOKENS}",
                Count(usize::try_from(*tokens).unwrap_or(usize::MAX), "token")
            ),
        }
    }
}

/// Formats a count with a singular or plural noun.
struct Count(usize, &'static str);

impl fmt::Display for Count {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(count, noun) = *self;
        let plural = if count == 1 { "" } else { "s" };
        write!(f, "{count} {noun}{plural}")
    }
}

/// Formats basis points as a percentage with two decimals.
struct Percent(u32);

impl fmt::Display for Percent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{:02}%", self.0 / 100, self.0 % 100)
    }
}

/// One node's expected share of keys, from [`Ring::expected_shares`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct NodeShare {
    /// The node.
    pub id: NodeId,
    /// The share of keys whose preferred primary is this node, in basis points. Computed
    /// exactly from weights (rendezvous) or token arcs (vnodes), rounded down.
    pub primary_bps: u32,
    /// The node's share of all replicas, primaries and followers, in basis points. Estimated
    /// by sampling keys, rounded down.
    pub replica_bps: u32,
}

impl<H: StableHasher> Ring<H> {
    /// Reports topology and balance problems, most serious first.
    ///
    /// The checks:
    ///
    /// - **Too few nodes, hosts, or zones** for the replication factor. Only the deepest
    ///   shortfall is reported: fewer nodes than replicas implies fewer hosts and zones.
    /// - **Zone skew.** A zone's estimated replica share exceeds its weight share by more than
    ///   half and by at least two percentage points. For example, zones of 10, 10, and 2 nodes
    ///   at R = 3 put a replica of every key in the 2-node zone.
    /// - **Low token counts.** Vnode-ring nodes with fewer than 64 tokens.
    ///
    /// Estimates sample 10 000 synthetic keys, so this costs about 10 000 lookups.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Diagnostic, Node, Ring, Severity};
    ///
    /// let ring = Ring::builder()
    ///     .node(Node::new("laptop-1".parse()?))
    ///     .node(Node::new("laptop-2".parse()?))
    ///     .build()?;
    /// let diagnostics = ring.diagnostics();
    /// assert_eq!(diagnostics.len(), 1);
    /// assert_eq!(diagnostics[0].severity(), Severity::Warning);
    /// assert_eq!(
    ///     diagnostics[0].to_string(),
    ///     "ring has 2 nodes but replicates 3 ways, so every key is under-replicated"
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn diagnostics(&self) -> Vec<Diagnostic> {
        let replication_factor = self.replication_factor();
        let replicas = usize::from(replication_factor.get());
        let members = self.members();
        let mut found = Vec::new();

        if self.node_count() < replicas {
            found.push(Diagnostic::UnderReplicated {
                nodes: self.node_count(),
                replication_factor,
            });
        } else if members.host_count < replicas {
            found.push(Diagnostic::FewerHostsThanReplicas {
                hosts: members.host_count,
                replication_factor,
            });
        } else if self.zone_count() < replicas {
            found.push(Diagnostic::FewerZonesThanReplicas {
                zones: self.zone_count(),
                replication_factor,
            });
        }

        if self.zone_count() > 1 && replicas > 1 {
            found.extend(self.zone_skew());
        }

        if let Strategy::Vnodes(params) = self.strategy() {
            let per_weight = u64::from(params.tokens_per_weight().get());
            found.extend(self.nodes().iter().filter_map(|node| {
                let tokens = u64::from(node.weight().get()) * per_weight;
                (tokens < MIN_TOKENS).then(|| Diagnostic::LowTokenCount {
                    node: node.id().clone(),
                    tokens,
                })
            }));
        }

        // A stable sort keeps each severity's diagnostics in check order, then canonical order.
        found.sort_by_key(Diagnostic::severity);
        found
    }

    /// Returns every node's expected share of primaries and replicas, in canonical order.
    ///
    /// Primary shares are exact: weight ratios under rendezvous, token arcs under vnodes.
    /// Replica shares are estimated by placing `samples` synthetic keys; they depend on
    /// topology, because spreading replicas across zones and hosts overrides weight.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroU32;
    ///
    /// use fleetring::{Node, Ring, Weight};
    ///
    /// let ring = Ring::builder()
    ///     .node(Node::new("small".parse()?))
    ///     .node(Node::new("large".parse()?).with_weight(Weight::new(3).ok_or("zero")?))
    ///     .build()?;
    /// let shares = ring.expected_shares(NonZeroU32::new(1_000).ok_or("zero")?);
    /// let large = shares.iter().find(|share| share.id.as_str() == "large").ok_or("missing")?;
    /// assert_eq!(large.primary_bps, 7_500);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn expected_shares(&self, samples: NonZeroU32) -> Vec<NodeShare> {
        let primary = self.primary_bps();
        let (replica_counts, total) = self.sample_replicas(samples);
        self.nodes()
            .iter()
            .zip(primary)
            .zip(replica_counts)
            .map(|((node, primary_bps), count)| NodeShare {
                id: node.id().clone(),
                primary_bps,
                replica_bps: bps(u128::from(count), u128::from(total)),
            })
            .collect()
    }

    /// Exact primary shares in basis points, in canonical order.
    fn primary_bps(&self) -> Vec<u32> {
        match self.strategy_index() {
            StrategyIndex::Rendezvous => {
                let total = u128::from(
                    self.nodes()
                        .iter()
                        .map(|n| u64::from(n.weight().get()))
                        .sum::<u64>(),
                );
                self.nodes()
                    .iter()
                    .map(|node| bps(u128::from(node.weight().get()), total))
                    .collect()
            }
            StrategyIndex::Vnodes(table) => table
                .ownership(self.node_count())
                .into_iter()
                .map(|arc| bps(arc, 1 << 64))
                .collect(),
        }
    }

    /// Counts how many replicas of `samples` synthetic keys each node holds, plus the total.
    fn sample_replicas(&self, samples: NonZeroU32) -> (Vec<u64>, u64) {
        let nodes = self.nodes();
        let mut counts = vec![0u64; nodes.len()];
        let mut total = 0u64;
        for sample in 0..samples.get() {
            let key = format!("fleetring-share-sample-{sample}");
            for replica in &self.replicas(&key) {
                let index = nodes
                    .binary_search_by(|node| node.id().cmp(replica.id()))
                    .expect("replicas come from the ring");
                counts[index] += 1;
                total += 1;
            }
        }
        (counts, total)
    }

    /// Zones whose replica share far exceeds their weight share.
    fn zone_skew(&self) -> Vec<Diagnostic> {
        let members = self.members();
        let (replica_counts, total_replicas) = self.sample_replicas(DIAGNOSTIC_SAMPLES);
        let mut zones: Vec<(Option<Zone>, u64, u64)> = vec![(None, 0, 0); self.zone_count()];
        for (index, node) in self.nodes().iter().enumerate() {
            let zone =
                &mut zones[usize::try_from(members.entries[index].zone).expect("small index")];
            zone.0 = node.zone().cloned();
            zone.1 += u64::from(node.weight().get());
            zone.2 += replica_counts[index];
        }
        let total_weight: u64 = zones.iter().map(|zone| zone.1).sum();
        zones
            .into_iter()
            .filter_map(|(zone, weight, replicas)| {
                let weight_share_bps = bps(u128::from(weight), u128::from(total_weight));
                let replica_share_bps = bps(u128::from(replicas), u128::from(total_replicas));
                let skewed = u64::from(replica_share_bps) * 2 > u64::from(weight_share_bps) * 3
                    && replica_share_bps >= weight_share_bps + 200;
                skewed.then_some(Diagnostic::ZoneReplicaSkew {
                    zone,
                    weight_share_bps,
                    replica_share_bps,
                })
            })
            .collect()
    }
}

/// `⌊part / whole⌋` in basis points.
fn bps(part: u128, whole: u128) -> u32 {
    u32::try_from(part * WHOLE_BPS / whole.max(1))
        .expect("a share never exceeds 10 000 basis points")
}
