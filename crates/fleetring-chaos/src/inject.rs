//! The fault injector: applies a schedule's faults to the running containers at their times, and
//! undoes each at its end (ADR-0036, point 4).

use std::fmt;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, sleep_until};

use crate::config::faketime_offset;
use crate::docker::{
    CLOCK_FILE, DockerError, Runner, Signal, heal_args, partition_args, signal_args, start_args,
};
use crate::node::Node;
use crate::schedule::{Fault, PlannedFault, STROBE_PERIOD, Schedule, database_label};

/// How often a primary isolation asks for the database's primary while it has none.
const LOCATE_RETRY: Duration = Duration::from_millis(200);
/// How long a primary isolation looks for the database's primary before it skips the fault.
const LOCATE_FOR: Duration = Duration::from_secs(2);

/// Finds the node that holds a database's lease now, by the database's index; `None` if it has
/// no primary or can't be asked. In a run it wraps the harness's own `data::Client::locate`,
/// whose calls aren't in the history.
pub type LocatePrimary =
    Arc<dyn Fn(usize) -> Pin<Box<dyn Future<Output = Option<Node>> + Send>> + Send + Sync>;

/// Applies a schedule's faults to the running containers and records what it did.
///
/// Cloning is cheap; clones share the runner and the locate function.
#[derive(Clone)]
pub struct Injector {
    runner: Arc<dyn Runner>,
    run_dir: PathBuf,
    locate: LocatePrimary,
}

impl fmt::Debug for Injector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Injector")
            .field("runner", &self.runner)
            .field("run_dir", &self.run_dir)
            .finish_non_exhaustive()
    }
}

/// A fault the injector ran: when it actually started and ended, and the targets it resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedFault {
    /// When it started, since the workload's start.
    pub started: Duration,
    /// When it ended, since the workload's start.
    pub ended: Duration,
    /// The fault as the schedule planned it.
    pub planned: PlannedFault,
    /// The targets as `schedule.txt` prints them, with a primary isolation's primary resolved.
    pub resolved: String,
}

impl fmt::Display for AppliedFault {
    /// One `schedule.txt` line, with the fault's actual start and end.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let actual = PlannedFault {
            start: self.started,
            end: self.ended,
            fault: self.planned.fault.clone(),
        };
        f.write_str(&actual.line(&self.resolved))
    }
}

impl Injector {
    /// Returns an injector that runs docker commands through `runner`, writes the control
    /// node's clock file under `run_dir`, and finds primaries with `locate`.
    #[must_use]
    pub fn new(runner: Arc<dyn Runner>, run_dir: PathBuf, locate: LocatePrimary) -> Self {
        Self {
            runner,
            run_dir,
            locate,
        }
    }

    /// Runs every fault at its planned time after `t0` until `deadline`, ending the active
    /// fault at the deadline. Returns what it applied, in order.
    ///
    /// # Errors
    ///
    /// The first docker command or clock file write that failed. The fault it was applying may
    /// be left in place: call [`heal_all`](Self::heal_all).
    pub async fn run(
        self,
        schedule: &Schedule,
        t0: Instant,
        deadline: Instant,
    ) -> Result<Vec<AppliedFault>, InjectError> {
        let mut applied = Vec::with_capacity(schedule.faults.len());
        for planned in &schedule.faults {
            let start_at = t0 + planned.start;
            if start_at >= deadline {
                break;
            }
            sleep_until(start_at).await;
            let started = t0.elapsed();
            let end_at = (t0 + planned.end).min(deadline);
            let resolved = self.apply(planned, end_at).await?;
            let ended = t0.elapsed();
            applied.push(AppliedFault {
                started,
                ended,
                planned: planned.clone(),
                resolved,
            });
        }
        Ok(applied)
    }

    /// Applies `planned`, holds it until `end_at`, undoes it, and returns its resolved targets.
    async fn apply(&self, planned: &PlannedFault, end_at: Instant) -> Result<String, InjectError> {
        match &planned.fault {
            Fault::Partition { cut, .. } => {
                self.cut(cut).await?;
                sleep_until(end_at).await;
                self.heal().await?;
                Ok(planned.fault.targets())
            }
            Fault::IsolatePrimary { database } => {
                let label = database_label(*database);
                let Some(primary) = self.primary(*database).await else {
                    sleep_until(end_at).await;
                    return Ok(format!("{label} skipped: no primary"));
                };
                let cut: Vec<(Node, Node)> = Node::ALL
                    .into_iter()
                    .filter(|node| *node != primary)
                    .map(|node| (primary.min(node), primary.max(node)))
                    .collect();
                self.cut(&cut).await?;
                sleep_until(end_at).await;
                self.heal().await?;
                Ok(format!("{label} primary {primary}"))
            }
            Fault::Kill { node } => {
                let container = node.container();
                self.runner
                    .run(signal_args(&container, Signal::Kill))
                    .await?;
                sleep_until(end_at).await;
                self.runner.run(start_args(&container)).await?;
                Ok(planned.fault.targets())
            }
            Fault::Pause { node } => {
                let container = node.container();
                self.runner
                    .run(signal_args(&container, Signal::Stop))
                    .await?;
                sleep_until(end_at).await;
                self.runner
                    .run(signal_args(&container, Signal::Cont))
                    .await?;
                Ok(planned.fault.targets())
            }
            Fault::ClockSkew { offset_s } => {
                self.set_clock(*offset_s)?;
                sleep_until(end_at).await;
                self.set_clock(0)?;
                Ok(planned.fault.targets())
            }
            Fault::ClockStrobe { offset_s } => {
                let mut offset = *offset_s;
                while Instant::now() < end_at {
                    self.set_clock(offset)?;
                    offset = -offset;
                    let next = (Instant::now() + STROBE_PERIOD).min(end_at);
                    sleep_until(next).await;
                }
                self.set_clock(0)?;
                Ok(planned.fault.targets())
            }
        }
    }

    /// Undoes every fault on every node: starts stopped containers, continues paused processes,
    /// flushes iptables, and sets the control node's clock offset back to `+0s`. Docker failures
    /// are ignored, so it is safe to call at any time.
    ///
    /// # Errors
    ///
    /// [`InjectError::Io`] if the clock file can't be written.
    pub async fn heal_all(&self) -> Result<(), InjectError> {
        for node in Node::ALL {
            let container = node.container();
            let _ = self.runner.run(start_args(&container)).await;
            let _ = self.runner.run(signal_args(&container, Signal::Cont)).await;
            for command in heal_args(&container) {
                let _ = self.runner.run(command).await;
            }
        }
        self.set_clock(0)
    }

    /// Finds database `index`'s primary, retrying while it has none, for up to [`LOCATE_FOR`].
    async fn primary(&self, index: usize) -> Option<Node> {
        let give_up = Instant::now() + LOCATE_FOR;
        loop {
            if let Some(primary) = (self.locate)(index).await {
                return Some(primary);
            }
            if Instant::now() + LOCATE_RETRY > give_up {
                return None;
            }
            sleep(LOCATE_RETRY).await;
        }
    }

    /// Drops traffic between every pair in `cut`, both ways, on both sides of each pair.
    async fn cut(&self, cut: &[(Node, Node)]) -> Result<(), InjectError> {
        let commands = cut.iter().flat_map(|&(a, b)| {
            let [a_in, a_out] = partition_args(&a.container(), b.ip());
            let [b_in, b_out] = partition_args(&b.container(), a.ip());
            [a_in, a_out, b_in, b_out]
        });
        self.run_all(commands).await
    }

    /// Flushes every container's rules.
    async fn heal(&self) -> Result<(), InjectError> {
        let commands = Node::ALL
            .into_iter()
            .flat_map(|node| heal_args(&node.container()));
        self.run_all(commands).await
    }

    /// Runs `commands` at once, and returns the first failure once all have ended.
    async fn run_all(
        &self,
        commands: impl IntoIterator<Item = Vec<String>>,
    ) -> Result<(), InjectError> {
        let mut tasks = JoinSet::new();
        for command in commands {
            let runner = Arc::clone(&self.runner);
            tasks.spawn(async move { runner.run(command).await.map(drop) });
        }
        let mut first_error = None;
        while let Some(joined) = tasks.join_next().await {
            let result = joined.map_err(|error| DockerError::Join {
                reason: error.to_string(),
            });
            if let Err(error) = result.and_then(|ran| ran)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }

    /// Writes the control node's clock offset, through a temporary file renamed over the old
    /// one, so libfaketime never reads half a write.
    fn set_clock(&self, offset_s: i64) -> Result<(), InjectError> {
        write_clock(&clock_dir(&self.run_dir, Node::CONTROL), offset_s)
    }
}

/// The directory holding `node`'s clock file under the run directory `run_dir`.
#[must_use]
pub fn clock_dir(run_dir: &Path, node: Node) -> PathBuf {
    run_dir.join("nodes").join(node.name()).join("faketime")
}

/// Writes `offset_s` into the clock file in `dir`, through a temporary file renamed over it.
///
/// # Errors
///
/// [`InjectError::Io`] naming the file that couldn't be written or renamed.
pub fn write_clock(dir: &Path, offset_s: i64) -> Result<(), InjectError> {
    let file = dir.join(CLOCK_FILE);
    let temporary = dir.join(format!("{CLOCK_FILE}.tmp"));
    let failed = |path: &Path| {
        let path = path.to_owned();
        move |source| InjectError::Io { path, source }
    };
    std::fs::write(&temporary, faketime_offset(offset_s)).map_err(failed(&temporary))?;
    std::fs::rename(&temporary, &file).map_err(failed(&file))
}

/// Why the injector failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum InjectError {
    /// A docker command failed.
    #[error("docker failed")]
    Docker(#[from] DockerError),
    /// The clock file couldn't be written.
    #[error("can't write {path}")]
    Io {
        /// The file.
        path: PathBuf,
        /// The error.
        source: io::Error,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::Mode;
    use crate::schedule::{FaultKind, Scenario};

    /// Records each command, and answers every one with empty output.
    #[derive(Debug, Default)]
    struct Recording(Mutex<Vec<Vec<String>>>);

    impl Recording {
        fn commands(&self) -> Vec<Vec<String>> {
            self.0.lock().expect("lock poisoned").clone()
        }
    }

    impl Runner for Recording {
        fn run(
            &self,
            args: Vec<String>,
        ) -> Pin<Box<dyn Future<Output = Result<String, DockerError>> + Send + '_>> {
            self.0.lock().expect("lock poisoned").push(args);
            Box::pin(async { Ok(String::new()) })
        }
    }

    fn locate(answer: Option<Node>) -> LocatePrimary {
        Arc::new(move |_| Box::pin(async move { answer }))
    }

    /// A run directory with the control node's clock file.
    fn run_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("temporary directory");
        let clock = clock_dir(dir.path(), Node::CONTROL);
        std::fs::create_dir_all(&clock).expect("clock directory");
        write_clock(&clock, 0).expect("clock file");
        dir
    }

    fn clock(dir: &Path) -> String {
        std::fs::read_to_string(clock_dir(dir, Node::CONTROL).join(CLOCK_FILE)).expect("clock")
    }

    fn one_fault(fault: Fault, start_s: u64, end_s: u64) -> Schedule {
        Schedule {
            scenario: Scenario::Mixed,
            seed: 0,
            faults: vec![PlannedFault {
                start: Duration::from_secs(start_s),
                end: Duration::from_secs(end_s),
                fault,
            }],
        }
    }

    fn sorted(mut commands: Vec<Vec<String>>) -> Vec<Vec<String>> {
        commands.sort();
        commands
    }

    fn heals() -> Vec<Vec<String>> {
        Node::ALL
            .into_iter()
            .flat_map(|node| heal_args(&node.container()))
            .collect()
    }

    /// Starts `schedule` on `recording` at `t0` (now), with ten minutes until the deadline.
    fn spawn(
        recording: &Arc<Recording>,
        dir: &Path,
        answer: Option<Node>,
        schedule: Schedule,
    ) -> tokio::task::JoinHandle<Result<Vec<AppliedFault>, InjectError>> {
        let runner: Arc<dyn Runner> = recording.clone();
        let injector = Injector::new(runner, dir.to_owned(), locate(answer));
        let t0 = Instant::now();
        let deadline = t0 + Duration::from_secs(600);
        tokio::spawn(async move { injector.run(&schedule, t0, deadline).await })
    }

    #[tokio::test(start_paused = true)]
    async fn a_partition_cuts_then_heals_every_pair() {
        let schedule = Schedule::generate(
            Scenario::Partitions,
            3,
            Duration::from_secs(600),
            Mode::Normal,
        );
        let planned = schedule
            .faults
            .iter()
            .find(|planned| planned.fault.kind() == FaultKind::PartitionHalves)
            .expect("a partition into halves")
            .clone();
        let Fault::Partition { cut, .. } = &planned.fault else {
            panic!("{planned}");
        };
        let expected_cut: Vec<Vec<String>> = cut
            .iter()
            .flat_map(|&(a, b)| {
                let [a_in, a_out] = partition_args(&a.container(), b.ip());
                let [b_in, b_out] = partition_args(&b.container(), a.ip());
                [a_in, a_out, b_in, b_out]
            })
            .collect();
        assert_eq!(expected_cut.len(), 24);
        let dir = run_dir();
        let recording = Arc::new(Recording::default());
        let task = spawn(
            &recording,
            dir.path(),
            None,
            one_fault(planned.fault.clone(), 10, 20),
        );
        sleep(Duration::from_secs(15)).await;
        assert_eq!(sorted(recording.commands()), sorted(expected_cut.clone()));
        let applied = task.await.expect("join").expect("run");
        let commands = recording.commands();
        assert_eq!(commands.len(), 34);
        assert_eq!(sorted(commands[24..].to_vec()), sorted(heals()));
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].started, Duration::from_secs(10));
        assert_eq!(applied[0].ended, Duration::from_secs(20));
    }

    #[tokio::test(start_paused = true)]
    async fn a_kill_restarts_the_container_at_the_end() {
        let dir = run_dir();
        let recording = Arc::new(Recording::default());
        let task = spawn(
            &recording,
            dir.path(),
            None,
            one_fault(Fault::Kill { node: Node::N3 }, 10, 20),
        );
        sleep(Duration::from_secs(15)).await;
        assert_eq!(
            recording.commands(),
            [signal_args("fleetring-chaos-n3", Signal::Kill)]
        );
        let applied = task.await.expect("join").expect("run");
        assert_eq!(
            recording.commands(),
            [
                signal_args("fleetring-chaos-n3", Signal::Kill),
                start_args("fleetring-chaos-n3")
            ]
        );
        assert_eq!(applied[0].resolved, "n3");
    }

    #[tokio::test(start_paused = true)]
    async fn a_pause_continues_the_process_at_the_end() {
        let dir = run_dir();
        let recording = Arc::new(Recording::default());
        let task = spawn(
            &recording,
            dir.path(),
            None,
            one_fault(Fault::Pause { node: Node::N1 }, 10, 20),
        );
        task.await.expect("join").expect("run");
        assert_eq!(
            recording.commands(),
            [
                signal_args("fleetring-chaos-n1", Signal::Stop),
                signal_args("fleetring-chaos-n1", Signal::Cont)
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_skew_writes_the_offset_then_zero() {
        let dir = run_dir();
        let recording = Arc::new(Recording::default());
        let task = spawn(
            &recording,
            dir.path(),
            None,
            one_fault(Fault::ClockSkew { offset_s: -42 }, 10, 20),
        );
        sleep(Duration::from_secs(15)).await;
        assert_eq!(clock(dir.path()), "-42s");
        task.await.expect("join").expect("run");
        assert_eq!(clock(dir.path()), "+0s");
        assert!(recording.commands().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_strobe_flips_every_half_second() {
        let dir = run_dir();
        let recording = Arc::new(Recording::default());
        let task = spawn(
            &recording,
            dir.path(),
            None,
            one_fault(Fault::ClockStrobe { offset_s: 30 }, 10, 40),
        );
        let start = Instant::now() + Duration::from_secs(10);
        let mut seen = Vec::new();
        for at_ms in [100, 600, 1_100] {
            sleep_until(start + Duration::from_millis(at_ms)).await;
            seen.push(clock(dir.path()));
        }
        assert_eq!(seen, ["+30s", "-30s", "+30s"]);
        let applied = task.await.expect("join").expect("run");
        assert_eq!(clock(dir.path()), "+0s");
        assert_eq!(applied[0].resolved, "n1 ±30s");
    }

    #[tokio::test(start_paused = true)]
    async fn an_isolation_without_a_primary_is_skipped() {
        let dir = run_dir();
        let recording = Arc::new(Recording::default());
        let task = spawn(
            &recording,
            dir.path(),
            None,
            one_fault(Fault::IsolatePrimary { database: 3 }, 10, 20),
        );
        let applied = task.await.expect("join").expect("run");
        assert!(recording.commands().is_empty());
        assert_eq!(applied[0].resolved, "chaos/db-3 skipped: no primary");
        assert_eq!(
            applied[0].to_string(),
            "   10000ms    20000ms isolate-primary chaos/db-3 skipped: no primary"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_isolation_cuts_the_primary_from_every_other_node() {
        let dir = run_dir();
        let recording = Arc::new(Recording::default());
        let task = spawn(
            &recording,
            dir.path(),
            Some(Node::N4),
            one_fault(Fault::IsolatePrimary { database: 3 }, 10, 20),
        );
        let applied = task.await.expect("join").expect("run");
        assert_eq!(applied[0].resolved, "chaos/db-3 primary n4");
        let commands = recording.commands();
        assert_eq!(commands.len(), 16 + 10);
        for command in &commands[..16] {
            assert!(command.contains(&"-j".to_owned()), "{command:?}");
            let n4 = Node::N4.container();
            let n4_ip = Node::N4.ip().to_string();
            assert!(
                command.contains(&n4) || command.contains(&n4_ip),
                "{command:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_ends_the_active_fault() {
        let dir = run_dir();
        let recording = Arc::new(Recording::default());
        let runner: Arc<dyn Runner> = recording.clone();
        let injector = Injector::new(runner, dir.path().to_owned(), locate(None));
        let t0 = Instant::now();
        let schedule = one_fault(Fault::Kill { node: Node::N2 }, 10, 20);
        let applied = injector
            .run(&schedule, t0, t0 + Duration::from_secs(15))
            .await
            .expect("run");
        assert_eq!(applied[0].ended, Duration::from_secs(15));
        assert_eq!(recording.commands().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn healing_everything_touches_every_container() {
        let dir = run_dir();
        write_clock(&clock_dir(dir.path(), Node::CONTROL), 17).expect("clock");
        let recording = Arc::new(Recording::default());
        let runner: Arc<dyn Runner> = recording.clone();
        let injector = Injector::new(runner, dir.path().to_owned(), locate(None));
        injector.heal_all().await.expect("heal");
        assert_eq!(recording.commands().len(), 5 * 4);
        assert_eq!(clock(dir.path()), "+0s");
    }
}
