//! FR-038 on the harness: a node hands its primaries over before it leaves (ADR-0022).
//!
//! One control host and three data hosts, and a client that creates several databases with
//! three replicas and writes to them throughout. The test drains the node that holds the most
//! primaries in every way a node can drain: an operator's `drain` while it keeps running, a
//! graceful stop, a stop while it is cut off from a follower, and a stop that turns into a crash
//! halfway through the drain. After each, every database the node led has a new primary at the
//! next epoch, and the history checker holds: no acknowledged write lost, one primary per epoch.

use std::collections::BTreeMap;
use std::error::Error;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{DatabaseName, LeaseEpoch, Liveness};
use fleetringd::sim::{Cluster, CreateAndWrite, Operation, Operator, Timings, check, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const CLIENT: &str = "writer";
const OPERATOR: &str = "operator";

/// How many databases the writer creates, so that every node leads some.
const DATABASES: u32 = 6;

/// How long a hand-over of every lease a node holds may take, in simulated time: well under a
/// lease duration plus the failover margin, which is what a failover would cost.
const HANDOVER: Duration = Duration::from_secs(2);

fn cluster(seed: u64) -> Result<Cluster, Box<dyn Error>> {
    Ok(Cluster::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .data(DATA[1])
        .data(DATA[2])
        .timings(Timings::default())
        .operation_timeout(Duration::from_millis(500))
        .build()?)
}

/// Every database with a primary, by the node that holds its lease, with the lease's epoch.
fn primaries(cluster: &Cluster) -> BTreeMap<String, Vec<(DatabaseName, LeaseEpoch)>> {
    let mut held: BTreeMap<String, Vec<(DatabaseName, LeaseEpoch)>> = BTreeMap::new();
    if let Some(state) = cluster.control_state() {
        for database in state.databases() {
            if let Some(lease) = database.lease()
                && lease.epoch() == database.epoch()
            {
                held.entry(lease.holder().to_string())
                    .or_default()
                    .push((database.name().clone(), lease.epoch()));
            }
        }
    }
    held
}

/// The node that leads the most databases, with them.
fn busiest(cluster: &Cluster) -> Option<(String, Vec<(DatabaseName, LeaseEpoch)>)> {
    primaries(cluster)
        .into_iter()
        .max_by_key(|(node, held)| (held.len(), std::cmp::Reverse(node.clone())))
}

/// Whether every database in `held` has a primary other than `from`, at a later epoch.
fn moved(cluster: &Cluster, from: &str, held: &[(DatabaseName, LeaseEpoch)]) -> bool {
    let Some(state) = cluster.control_state() else {
        return false;
    };
    held.iter().all(|(name, epoch)| {
        state.database(name).is_some_and(|database| {
            database.lease().is_some_and(|lease| {
                lease.epoch() == database.epoch()
                    && lease.epoch() > *epoch
                    && lease.holder().as_str() != from
            })
        })
    })
}

/// Steps until every database in `held` has moved away from `from`, and checks that each moved
/// exactly one epoch on: a hand-over, not a failover after a stall.
fn await_moved(
    cluster: &mut Cluster,
    from: &str,
    held: &[(DatabaseName, LeaseEpoch)],
    limit: Duration,
) -> Result<Duration, Box<dyn Error>> {
    let what = format!("{from}'s {} primaries moving", held.len());
    let took = cluster.run_until(limit, &what, |cluster| moved(cluster, from, held))?;
    cluster.note(format!("{what}: {} ms", took.as_millis()));
    cluster.check()?;
    Ok(took)
}

/// Creates the databases and waits until every one has a primary and writes flow.
fn start_writing(cluster: &mut Cluster, writes: u32) -> TestResult {
    cluster.await_ring(Duration::from_secs(30))?;
    cluster.client(CLIENT, CreateAndWrite::new(DATABASES, 3, writes));
    cluster.run_until(
        Duration::from_secs(60),
        "every database leased",
        |cluster| {
            primaries(cluster).values().map(Vec::len).sum::<usize>()
                == usize::try_from(DATABASES).unwrap_or(usize::MAX)
                && cluster.history().acknowledged_writes().next().is_some()
        },
    )?;
    Ok(())
}

fn drains_under_faults(seed: u64) -> TestResult {
    let mut cluster = cluster(seed)?;
    let operator = Operator::new();
    cluster.client(OPERATOR, operator.clone());
    start_writing(&mut cluster, 400)?;
    let timings = Timings::default();
    let failover = timings.lease_duration + timings.failover_margin + Duration::from_secs(2);

    // 1. An operator drains the busiest node, which keeps running: its primaries move within
    //    the hand-over bound, it gets no new lease, and it stays a live member.
    let (drained, held) = busiest(&cluster).ok_or("no primaries")?;
    assert!(!held.is_empty());
    let drained_id: NodeId = drained.parse()?;
    operator.push(Operation::Drain {
        node: drained_id.clone(),
    });
    let took = await_moved(&mut cluster, &drained, &held, HANDOVER)?;
    assert!(took <= HANDOVER, "drain took {took:?}");
    let state = cluster.control_state().ok_or("control is down")?;
    assert!(state.is_draining(&drained_id));
    assert_eq!(
        state.liveness(&drained_id, state.now()),
        Some(Liveness::Live)
    );
    cluster.run_for(Duration::from_secs(1))?;
    assert!(
        !primaries(&cluster).contains_key(&drained),
        "a draining node got a lease back"
    );
    cluster.check_history()?;

    // 2. A graceful stop of the busiest node: it drains, then leaves. Restarted, it joins again
    //    and is no longer draining.
    let (stopped, held) = busiest(&cluster).ok_or("no primaries")?;
    let before = cluster.elapsed();
    cluster.stop(&stopped)?;
    let took = cluster.elapsed() - before;
    assert!(
        moved(&cluster, &stopped, &held),
        "{stopped} left before its primaries moved"
    );
    cluster.note(format!("{stopped} stopped in {} ms", took.as_millis()));
    let state = cluster.control_state().ok_or("control is down")?;
    assert!(
        state.member(&stopped.parse()?).is_none(),
        "{stopped} didn't leave"
    );
    cluster.check()?;
    cluster.restart(&stopped)?;
    cluster.restart(&drained)?;
    cluster.await_ring(Duration::from_secs(10))?;
    let state = cluster.control_state().ok_or("control is down")?;
    assert!(
        !state.is_draining(&drained_id),
        "a rejoined node is no longer draining"
    );
    cluster.run_for(Duration::from_secs(1))?;

    // 3. A stop while the node is cut off from one follower: the other one gets the leases.
    let (cut, held) = busiest(&cluster).ok_or("no primaries")?;
    let others: Vec<&str> = DATA.into_iter().filter(|node| *node != cut).collect();
    let isolated_from = others[usize::try_from(cluster.rng().below(2))?];
    cluster.partition(&cut, isolated_from)?;
    cluster.run_for(Duration::from_millis(500))?;
    cluster.stop(&cut)?;
    assert!(
        moved(&cluster, &cut, &held),
        "{cut}'s primaries didn't move"
    );
    cluster.repair(&cut, isolated_from)?;
    cluster.restart(&cut)?;
    cluster.await_ring(Duration::from_secs(10))?;
    cluster.run_for(Duration::from_secs(1))?;
    cluster.check_history()?;

    // 4. A crash in the middle of a drain: what was handed over stays handed over, what wasn't
    //    fails over the slow way, and nothing acknowledged is lost either way.
    let (crashed, held) = busiest(&cluster).ok_or("no primaries")?;
    cluster.begin_stop(&crashed)?;
    let into_the_drain = Duration::from_millis(cluster.rng().below(120));
    cluster.run_for(into_the_drain)?;
    cluster.crash(&crashed)?;
    let since = cluster.elapsed();
    await_moved(&mut cluster, &crashed, &held, failover * 2)?;
    cluster.note(format!(
        "{crashed}'s primaries all moved {} ms after the crash",
        (cluster.elapsed() - since).as_millis()
    ));
    cluster.restart(&crashed)?;

    cluster.await_workloads(Duration::from_secs(300))?;
    cluster.await_ring(Duration::from_secs(60))?;
    cluster.await_replicas_converged(Duration::from_secs(60))?;
    assert!(operator.is_idle());
    cluster.check()?;
    let report = cluster.finish()?;
    // The single-writer check ran over the whole history, drains included.
    check::one_primary_per_epoch(report.history())?;
    let epochs: Vec<u64> = report
        .state()
        .databases()
        .map(|database| database.epoch().get())
        .collect();
    assert!(
        epochs.iter().all(|epoch| *epoch >= 2),
        "every database moved at least once: {epochs:?}"
    );
    Ok(())
}

#[test]
fn drains_move_primaries_without_losing_writes_under_faults() {
    seeds::run(BINARY, &seeds::FIXED_HEAVY, drains_under_faults);
}
