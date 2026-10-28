//! Ring diffs and movement summaries through the public API (FR-012).

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::Debug;
use std::num::NonZeroU32;

use fleetring::{
    BuildRingError, Change, Epoch, MovementSummary, Node, NodeId, ReplicationFactor, Ring,
    RingBuilder, Weight,
};
use proptest::prelude::*;
use proptest::sample::Index;

use crate::common::corpus::{Corpus, LARGE, MEDIUM, SMALL};
use crate::common::fixtures;
use crate::common::stats::{assert_balanced, z_scores};

type TestResult = Result<(), Box<dyn Error>>;

/// Keys per proptest case: enough that a joining node usually takes some of them.
const PROPTEST_KEYS: usize = 128;

/// The sample count for `estimate_movement` accuracy checks. The estimate and an exact diff over
/// 100k keys each have a standard error of at most 0.16 percentage points, so the 1-point
/// tolerance is more than 4σ of their difference.
const ESTIMATE_SAMPLES: u32 = 100_000;

/// The largest gap, in percentage points, allowed between an estimated and an exact share.
const ESTIMATE_TOLERANCE: f64 = 1.0;

fn node(id: &str) -> Node {
    Node::new(id.parse().expect("test ids are valid"))
}

fn weight(value: u32) -> Weight {
    Weight::new(value).expect("test weights are nonzero")
}

fn replication(value: u8) -> ReplicationFactor {
    ReplicationFactor::new(value).expect("test replication factors are nonzero")
}

fn ids<'a>(nodes: impl Iterator<Item = &'a Node>) -> Vec<NodeId> {
    nodes.map(|node| node.id().clone()).collect()
}

/// Converts a key count to `f64` exactly; counts in these tests stay far below 2³².
fn to_f64(count: u64) -> f64 {
    f64::from(u32::try_from(count).expect("key counts in these tests fit in u32"))
}

/// Returns `part` as a percentage of `whole`.
fn percent(part: u64, whole: u64) -> f64 {
    100.0 * to_f64(part) / to_f64(whole)
}

/// Returns whether `sub` appears in `seq` in the same relative order.
fn is_subsequence(sub: &[NodeId], seq: &[NodeId]) -> bool {
    let mut rest = seq.iter();
    sub.iter().all(|id| rest.any(|other| other == id))
}

/// A change as plain data: the key, then both replica sets' IDs in preference order.
type Plain = (Vec<u8>, Vec<NodeId>, Vec<NodeId>);

/// The changes between two rings, found the slow and obvious way: every key whose replica IDs
/// differ between the rings.
fn naive_changes(current: &Ring, target: &Ring, keys: &[Vec<u8>]) -> Vec<Plain> {
    keys.iter()
        .filter_map(|key| {
            let from = current.replicas(key).to_ids();
            let to = target.replicas(key).to_ids();
            (from != to).then(|| (key.clone(), from, to))
        })
        .collect()
}

/// Asserts that a change's accessors agree with set arithmetic on its two replica sets.
fn assert_consistent<K: Debug>(change: &Change<'_, K>) {
    let key = change.key();
    let from = change.from().to_ids();
    let to = change.to().to_ids();
    assert_ne!(from, to, "{key:?} is reported but did not change");
    let from_set: BTreeSet<&NodeId> = from.iter().collect();
    let to_set: BTreeSet<&NodeId> = to.iter().collect();
    let added = ids(change.added());
    let removed = ids(change.removed());
    assert_eq!(
        added.iter().collect::<BTreeSet<_>>(),
        &to_set - &from_set,
        "{key:?}"
    );
    assert_eq!(
        removed.iter().collect::<BTreeSet<_>>(),
        &from_set - &to_set,
        "{key:?}"
    );
    assert!(is_subsequence(&added, &to), "{key:?}: {added:?} in {to:?}");
    assert!(
        is_subsequence(&removed, &from),
        "{key:?}: {removed:?} in {from:?}"
    );
    assert_eq!(change.primary_changed(), from[0] != to[0], "{key:?}");
    assert_eq!(change.is_reorder_only(), from_set == to_set, "{key:?}");
}

/// The kind of changes a scenario must produce, so that the cross-check covers each kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// No key changes.
    Nothing,
    /// Some keys change, and every change only reorders the same nodes.
    Reorders,
    /// Every key changes, only by losing replicas.
    Losses,
    /// Every key changes, only by gaining replicas.
    Gains,
    /// Some keys change, and not only by reordering.
    Moves,
}

/// A ring change to diff: the current ring, the target ring, and the kind of changes between
/// them, named for failure messages.
struct Scenario {
    label: &'static str,
    current: Ring,
    target: Ring,
    expect: Expect,
}

impl Scenario {
    /// Builds the target ring from `target`, a builder derived from `current`.
    fn new(
        label: &'static str,
        current: &Ring,
        target: RingBuilder,
        expect: Expect,
    ) -> Result<Self, BuildRingError> {
        Ok(Self {
            label,
            current: current.clone(),
            target: target.build()?,
            expect,
        })
    }
}

/// Ring changes to membership, weights, labels, the replication factor, and the seed, plus a
/// rebuild that places every key identically.
fn scenarios() -> Result<Vec<Scenario>, Box<dyn Error>> {
    let fleet = Ring::builder().nodes(fixtures::fleet(12, 3)).build()?;
    let original = |id: &str| fleet.node(id).cloned().ok_or("the fleet has the node");
    let full = Ring::builder().nodes(["a", "b", "c"].map(node)).build()?;
    // Shares host z1-h1 with z1-n1 and z1-n2, so host spreading limits where it takes replicas.
    let joining = node("z1-n9")
        .with_zone("z1".parse()?)
        .with_host("z1-h1".parse()?)
        .with_weight(weight(3));
    let moved = node("z1-n1")
        .with_zone("z2".parse()?)
        .with_weight(original("z1-n1")?.weight());
    let reweighted = original("z3-n1")?.with_weight(weight(4));
    let grown = node("b").with_weight(weight(5));
    Ok(vec![
        Scenario::new(
            "adding a node",
            &fleet,
            fleet.to_builder().node(joining),
            Expect::Moves,
        )?,
        Scenario::new(
            "removing a node",
            &fleet,
            fleet.to_builder().remove_node("z2-n2"),
            Expect::Moves,
        )?,
        Scenario::new(
            "reweighting a node",
            &fleet,
            fleet.to_builder().remove_node("z3-n1").node(reweighted),
            Expect::Moves,
        )?,
        Scenario::new(
            "moving a node to another zone",
            &fleet,
            fleet.to_builder().remove_node("z1-n1").node(moved),
            Expect::Moves,
        )?,
        Scenario::new(
            "lowering the replication factor",
            &fleet,
            fleet.to_builder().replication_factor(replication(2)),
            Expect::Losses,
        )?,
        Scenario::new(
            "raising the replication factor",
            &fleet,
            fleet.to_builder().replication_factor(replication(4)),
            Expect::Gains,
        )?,
        Scenario::new(
            "changing the seed",
            &fleet,
            fleet.to_builder().seed(1),
            Expect::Moves,
        )?,
        Scenario::new(
            "reweighting a node of a full ring",
            &full,
            full.to_builder().remove_node("b").node(grown),
            Expect::Reorders,
        )?,
        Scenario::new(
            "draining a node of a full ring",
            &full,
            full.to_builder().remove_node("a"),
            Expect::Losses,
        )?,
        Scenario::new(
            "rebuilding the same ring with a newer epoch",
            &fleet,
            fleet.to_builder().epoch(Epoch::new(1)),
            Expect::Nothing,
        )?,
    ])
}

/// Asserts that a scenario's summary shows the kind of changes it promises.
fn assert_expected_kind(label: &str, expect: Expect, summary: &MovementSummary) {
    let (examined, changed) = (summary.keys_examined(), summary.keys_changed());
    let (added, removed) = (summary.replicas_added(), summary.replicas_removed());
    let reorders = summary.reorder_only_changes();
    let holds = match expect {
        Expect::Nothing => changed == 0,
        Expect::Reorders => {
            changed > 0 && reorders == changed && added.is_empty() && removed.is_empty()
        }
        Expect::Losses => changed == examined && added.is_empty() && !removed.is_empty(),
        Expect::Gains => changed == examined && !added.is_empty() && removed.is_empty(),
        Expect::Moves => changed > reorders,
    };
    assert!(holds, "{label}: expected {expect:?}, got {summary:?}");
}

/// Replicas gained or lost per node, as a summary reports them.
type NodeCounts = BTreeMap<NodeId, u64>;

/// The counts a summary of `changes` must report, tallied from the plain replica IDs:
/// `(changed, primary changes, reorder-only changes, replicas added, replicas removed)`.
fn naive_counts(changes: &[Plain]) -> (usize, usize, usize, NodeCounts, NodeCounts) {
    let mut added = NodeCounts::new();
    let mut removed = NodeCounts::new();
    let (mut primaries, mut reorders) = (0, 0);
    for (_, from, to) in changes {
        primaries += usize::from(from[0] != to[0]);
        let from_set: BTreeSet<&NodeId> = from.iter().collect();
        let to_set: BTreeSet<&NodeId> = to.iter().collect();
        reorders += usize::from(from_set == to_set);
        for id in &to_set - &from_set {
            *added.entry(id.clone()).or_default() += 1;
        }
        for id in &from_set - &to_set {
            *removed.entry(id.clone()).or_default() += 1;
        }
    }
    (changes.len(), primaries, reorders, added, removed)
}

#[test]
fn diffs_yield_exactly_the_keys_whose_replicas_changed() -> TestResult {
    let keys: Vec<Vec<u8>> = Corpus::Agents.keys(SMALL).collect();
    let examined = u64::try_from(keys.len())?;
    for Scenario {
        label,
        current,
        target,
        expect,
    } in scenarios()?
    {
        let changes: Vec<Change<'_, &Vec<u8>>> = current.diff(&target, &keys).collect();
        let got: Vec<Plain> = changes
            .iter()
            .map(|change| {
                let key: &Vec<u8> = change.key();
                (key.clone(), change.from().to_ids(), change.to().to_ids())
            })
            .collect();
        let want = naive_changes(&current, &target, &keys);
        assert!(
            got == want,
            "{label}: the diff disagrees with the naive comparison"
        );
        for change in &changes {
            assert_consistent(change);
        }

        let summary = current.diff(&target, &keys).summarize();
        let (changed, primaries, reorders, added, removed) = naive_counts(&want);
        assert_eq!(summary.keys_examined(), examined, "{label}");
        assert_eq!(summary.keys_changed(), u64::try_from(changed)?, "{label}");
        assert_eq!(
            summary.primary_changes(),
            u64::try_from(primaries)?,
            "{label}"
        );
        assert_eq!(
            summary.reorder_only_changes(),
            u64::try_from(reorders)?,
            "{label}"
        );
        assert_eq!(summary.replicas_added(), &added, "{label}");
        assert_eq!(summary.replicas_removed(), &removed, "{label}");
        assert_eq!(
            MovementSummary::from_changes(&changes, examined),
            summary,
            "{label}"
        );
        assert_expected_kind(label, expect, &summary);
    }
    Ok(())
}

#[test]
fn identical_rings_have_no_changes() -> TestResult {
    let ring = Ring::builder().nodes(fixtures::homelab()).build()?;
    let rebuilt = ring.to_builder().epoch(Epoch::new(9)).build()?;
    for target in [&ring, &rebuilt] {
        assert_eq!(ring.diff(target, Corpus::Uuids.keys(SMALL)).count(), 0);
        let summary = ring.diff(target, Corpus::Uuids.keys(SMALL)).summarize();
        assert_eq!(
            summary,
            MovementSummary::from_changes(Vec::<Change<'_, Vec<u8>>>::new(), u64::try_from(SMALL)?)
        );
        assert_eq!(summary.keys_examined(), u64::try_from(SMALL)?);
    }
    let empty = ring.diff(&rebuilt, Vec::<String>::new()).summarize();
    assert_eq!(empty, MovementSummary::default());
    Ok(())
}

#[test]
fn diffs_pull_keys_only_as_they_are_iterated() -> TestResult {
    // Three replicas on two nodes: every key gains a replica on the third node.
    let current = Ring::builder().nodes(["a", "b"].map(node)).build()?;
    let target = current.to_builder().node(node("c")).build()?;
    let pulled = Cell::new(0);
    let endless = (0u64..).map(|i| {
        pulled.set(pulled.get() + 1);
        format!("agent:{i}")
    });

    let mut diff = current.diff(&target, endless);
    let first = diff.next().ok_or("an endless diff ended")?;
    assert_eq!(pulled.get(), 1);
    assert_eq!(first.key(), "agent:0");
    assert_eq!(first.into_key(), "agent:0");

    let copy = diff.clone();
    let next: Vec<String> = diff.take(3).map(Change::into_key).collect();
    assert_eq!(next, ["agent:1", "agent:2", "agent:3"]);
    assert_eq!(
        copy.take(3).map(Change::into_key).collect::<Vec<_>>(),
        next,
        "a cloned diff resumes where the original was"
    );
    Ok(())
}

#[test]
fn summaries_from_changes_accept_owned_or_borrowed_changes() -> TestResult {
    let current = Ring::builder().nodes(fixtures::homelab()).build()?;
    let target = current.to_builder().remove_node("laptop-2").build()?;
    let keys: Vec<Vec<u8>> = Corpus::Sequential.keys(SMALL).collect();
    let examined = u64::try_from(keys.len())?;
    let changes: Vec<Change<'_, &Vec<u8>>> = current.diff(&target, &keys).collect();

    let summary = current.diff(&target, &keys).summarize();
    assert_eq!(MovementSummary::from_changes(&changes, examined), summary);
    assert_eq!(
        MovementSummary::from_changes(changes.iter(), examined),
        summary
    );
    assert_eq!(
        MovementSummary::from_changes(current.diff(&target, &keys), examined),
        summary
    );
    assert_eq!(
        MovementSummary::from_changes(changes.clone(), examined),
        summary
    );

    // A summary never claims more changed keys than examined ones.
    let changed = u64::try_from(changes.len())?;
    let clamped = MovementSummary::from_changes(&changes, 1);
    assert_eq!(clamped.keys_examined(), changed);
    assert_eq!(clamped.keys_changed(), changed);
    Ok(())
}

/// Asserts that `observed` successes in `trials` lie within 5σ of a binomial with success
/// probability `p`.
#[track_caller]
fn assert_within_five_sigma(observed: u64, trials: u64, p: f64, label: &str) {
    let z = z_scores(&[observed, trials - observed], &[p, 1.0 - p])[0];
    let expected = to_f64(trials) * p;
    assert!(
        z.abs() <= 5.0,
        "{label}: {observed} of {trials} keys, expected {expected:.1}, z = {z:+.2}"
    );
}

#[test]
fn adding_one_node_changes_r_out_of_n_plus_one_keys() -> TestResult {
    const N: u32 = 10;
    const R: u8 = 3;
    let mut builder = Ring::builder().replication_factor(replication(R));
    for i in 0..N {
        builder = builder.node(node(&format!("node-{i}")));
    }
    let current = builder.build()?;
    let target = current.to_builder().node(node("node-new")).build()?;

    let summary = current
        .diff(&target, Corpus::Agents.keys(MEDIUM))
        .summarize();
    let (examined, changed) = (summary.keys_examined(), summary.keys_changed());
    assert_eq!(examined, u64::try_from(MEDIUM)?);
    let n = f64::from(N);
    // The new node joins a key's replica set when it ranks among the top R of N + 1 equal
    // nodes, and becomes primary when it ranks first.
    assert_within_five_sigma(changed, examined, f64::from(R) / (n + 1.0), "keys changed");
    assert_within_five_sigma(
        summary.primary_changes(),
        examined,
        1.0 / (n + 1.0),
        "primary changes",
    );
    assert_eq!(summary.reorder_only_changes(), 0);
    // Every change copies to the new node, and drops the replica that ranked last, which is
    // equally likely to be on any old node.
    let new: NodeId = "node-new".parse()?;
    assert_eq!(
        summary.replicas_added().iter().collect::<Vec<_>>(),
        [(&new, &changed)]
    );
    let removed: Vec<u64> = current
        .nodes()
        .iter()
        .map(|node| {
            let count = summary.replicas_removed().get(node.id());
            count.copied().unwrap_or(0)
        })
        .collect();
    assert_eq!(removed.iter().sum::<u64>(), changed);
    assert_balanced(
        &removed,
        &fixtures::weights(current.nodes()),
        "replicas removed",
    );
    Ok(())
}

/// Ring changes to estimate: a node joining equal peers, and a fleet that gains a zone of two
/// nodes while losing one.
fn estimate_scenarios() -> Result<Vec<Scenario>, Box<dyn Error>> {
    let mut equal = Ring::builder();
    for i in 0..10 {
        equal = equal.node(node(&format!("node-{i}")));
    }
    let equal = equal.build()?;
    let fleet = Ring::builder().nodes(fixtures::fleet(12, 3)).build()?;
    let in_z4 = |id: &str, value| -> Result<Node, Box<dyn Error>> {
        Ok(node(id).with_zone("z4".parse()?).with_weight(weight(value)))
    };
    Ok(vec![
        Scenario::new(
            "an 11th equal node joining",
            &equal,
            equal.to_builder().node(node("node-10")),
            Expect::Moves,
        )?,
        Scenario::new(
            "a fleet gaining a zone and losing a node",
            &fleet,
            fleet
                .to_builder()
                .remove_node("z1-n2")
                .node(in_z4("z4-n1", 1)?)
                .node(in_z4("z4-n2", 2)?),
            Expect::Moves,
        )?,
    ])
}

#[test]
fn estimates_are_deterministic_diffs_of_the_documented_sample_keys() -> TestResult {
    let samples = NonZeroU32::new(1_000).ok_or("zero samples")?;
    for Scenario {
        label,
        current,
        target,
        ..
    } in estimate_scenarios()?
    {
        let estimate = current.estimate_movement(&target, samples);
        assert_eq!(
            current.estimate_movement(&target, samples),
            estimate,
            "{label}: estimates differ between calls"
        );
        let rebuilt = target.to_builder().build()?;
        assert_eq!(
            current.estimate_movement(&rebuilt, samples),
            estimate,
            "{label}: estimates differ between identical rings"
        );
        let keys = (0..samples.get()).map(|i| format!("fleetring-movement-sample-{i}"));
        assert_eq!(
            current.diff(&target, keys).summarize(),
            estimate,
            "{label}: the estimate is not a diff of the documented keys"
        );
    }
    Ok(())
}

/// Asserts that each share `estimate_movement` reports lies within [`ESTIMATE_TOLERANCE`] of the
/// exact diff over the first `keys` keys of the agents corpus.
fn assert_estimates_match_exact_diffs(keys: usize) -> TestResult {
    let samples = NonZeroU32::new(ESTIMATE_SAMPLES).ok_or("zero samples")?;
    for Scenario {
        label,
        current,
        target,
        expect,
    } in estimate_scenarios()?
    {
        let estimate = current.estimate_movement(&target, samples);
        assert_eq!(estimate.keys_examined(), u64::from(ESTIMATE_SAMPLES));
        let exact = current.diff(&target, Corpus::Agents.keys(keys)).summarize();
        assert_expected_kind(label, expect, &exact);
        let check = |what: &str, estimated: u64, actual: u64| {
            let estimated = percent(estimated, estimate.keys_examined());
            let actual = percent(actual, exact.keys_examined());
            assert!(
                (estimated - actual).abs() <= ESTIMATE_TOLERANCE,
                "{label}: {what} estimated at {estimated:.3}% of keys, exactly {actual:.3}%"
            );
        };
        check(
            "keys changed",
            estimate.keys_changed(),
            exact.keys_changed(),
        );
        check(
            "primary changes",
            estimate.primary_changes(),
            exact.primary_changes(),
        );
        check(
            "reorder-only changes",
            estimate.reorder_only_changes(),
            exact.reorder_only_changes(),
        );
        let nodes: BTreeSet<&NodeId> = current
            .nodes()
            .iter()
            .chain(target.nodes())
            .map(Node::id)
            .collect();
        let per_node = [
            (
                "replicas added to",
                estimate.replicas_added(),
                exact.replicas_added(),
            ),
            (
                "replicas removed from",
                estimate.replicas_removed(),
                exact.replicas_removed(),
            ),
        ];
        for id in nodes {
            for (what, estimated, actual) in per_node {
                let count = |counts: &NodeCounts| counts.get(id).copied().unwrap_or(0);
                check(&format!("{what} {id}"), count(estimated), count(actual));
            }
        }
    }
    Ok(())
}

#[test]
fn estimates_are_close_to_exact_diffs() -> TestResult {
    assert_estimates_match_exact_diffs(MEDIUM)
}

#[test]
#[ignore = "1M keys per scenario; runs in the release statistics job"]
fn estimates_are_close_to_exact_diffs_at_one_million_keys() -> TestResult {
    assert_estimates_match_exact_diffs(LARGE)
}

#[test]
fn summaries_display_as_aligned_tables() -> TestResult {
    // A node joins while another grows: some keys move to the new node, and some only reorder.
    let current = Ring::builder()
        .nodes(["laptop-1", "laptop-2", "nas"].map(node))
        .build()?;
    let target = current
        .to_builder()
        .remove_node("nas")
        .node(node("nas").with_weight(weight(4)))
        .node(node("vps"))
        .build()?;
    let databases = (0..24).map(|i| format!("agent:{i}"));
    let summary = current.diff(&target, databases).summarize();
    assert_eq!(
        summary.to_string(),
        "keys examined         24\n\
         keys changed          22  91.67%\n\
         primary changes        5  20.83%\n\
         reorder-only changes   2   8.33%\n\
         \n\
         node      replicas added  replicas removed\n\
         laptop-1               0                 9\n\
         laptop-2               0                11\n\
         vps                   20                 0"
    );
    Ok(())
}

#[test]
fn empty_summaries_display_zero_counts_and_no_node_table() {
    assert_eq!(
        MovementSummary::default().to_string(),
        "keys examined         0\n\
         keys changed          0  0.00%\n\
         primary changes       0  0.00%\n\
         reorder-only changes  0  0.00%"
    );
}

/// A node that can join any membership from [`fixtures::memberships_up_to`]: generated IDs never
/// contain `-`, and its host label names its zone, as the generated labels do.
fn joining_node() -> impl Strategy<Value = Node> {
    let zone = proptest::option::of(1..=7usize);
    let host = proptest::option::of(1..=5usize);
    (1..=1000u32, zone, host).prop_map(|(value, zone, host)| {
        let mut joining = node("new-node").with_weight(weight(value));
        if let Some(zone) = zone {
            joining = joining.with_zone(format!("z{zone}").parse().expect("valid zone"));
        }
        if let Some(host) = host {
            let host = match zone {
                Some(zone) => format!("z{zone}-h{host}"),
                None => format!("h{host}"),
            };
            joining = joining.with_host(host.parse().expect("valid host"));
        }
        joining
    })
}

proptest! {
    #[test]
    fn adding_a_node_copies_only_to_it_and_removing_it_mirrors_that(
        nodes in fixtures::memberships_up_to(16),
        joining in joining_node(),
        r in 1u8..=5,
    ) {
        let before = Ring::builder()
            .replication_factor(replication(r))
            .nodes(nodes)
            .build()
            .expect("valid membership");
        let after = before.to_builder().node(joining.clone()).build().expect("the node can join");
        let keys: Vec<Vec<u8>> = Corpus::Uuids.keys(PROPTEST_KEYS).collect();

        let added: Vec<Change<'_, &Vec<u8>>> = before.diff(&after, &keys).collect();
        for change in &added {
            prop_assert_eq!(ids(change.added()), [joining.id().clone()]);
            // A full replica set swaps one node for the new one; a short one just grows.
            let full = !change.from().is_under_replicated();
            prop_assert_eq!(change.removed().count(), usize::from(full));
        }

        let removed: Vec<Change<'_, &Vec<u8>>> = after.diff(&before, &keys).collect();
        prop_assert_eq!(removed.len(), added.len());
        for (back, forth) in removed.iter().zip(&added) {
            prop_assert_eq!(back.key(), forth.key());
            prop_assert_eq!(back.from(), forth.to());
            prop_assert_eq!(back.to(), forth.from());
            prop_assert_eq!(ids(back.added()), ids(forth.removed()));
            prop_assert_eq!(ids(back.removed()), ids(forth.added()));
            prop_assert_eq!(back.primary_changed(), forth.primary_changed());
            prop_assert!(!back.is_reorder_only() && !forth.is_reorder_only());
        }
        let examined = u64::try_from(keys.len()).expect("few keys");
        let forth = MovementSummary::from_changes(&added, examined);
        let back = MovementSummary::from_changes(&removed, examined);
        prop_assert_eq!(back.replicas_added(), forth.replicas_removed());
        prop_assert_eq!(back.replicas_removed(), forth.replicas_added());
        prop_assert_eq!(back.primary_changes(), forth.primary_changes());
    }

    #[test]
    fn removing_a_node_changes_only_the_keys_it_held(
        nodes in fixtures::memberships_up_to(16),
        joining in joining_node(),
        leaving in any::<Index>(),
        r in 1u8..=5,
    ) {
        let current = Ring::builder()
            .replication_factor(replication(r))
            .nodes(nodes)
            .node(joining)
            .build()
            .expect("the node can join");
        let leaving = current.nodes()[leaving.index(current.node_count())].id().clone();
        let target = current.to_builder().remove_node(&leaving).build().expect("a node remains");
        let keys: Vec<Vec<u8>> = Corpus::Uuids.keys(PROPTEST_KEYS).collect();

        let mut changes = current.diff(&target, &keys).peekable();
        for key in &keys {
            let held = current.replicas(key).contains(&leaving);
            let change = changes.next_if(|change| change.key() == &key);
            prop_assert_eq!(change.is_some(), held, "{:?}", key);
            if let Some(change) = change {
                prop_assert_eq!(ids(change.removed()), [leaving.clone()]);
                prop_assert!(change.added().count() <= 1);
            }
        }
        prop_assert!(changes.next().is_none());
    }
}
