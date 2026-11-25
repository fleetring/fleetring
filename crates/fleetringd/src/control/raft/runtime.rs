//! openraft's runtime for the control group: Tokio's timers, channels and mutexes, with
//! randomness drawn from the generator in scope (ADR-0040 point 1).
//!
//! Every task that openraft spawns through [`ControlRuntime`] inherits its spawner's generator,
//! a [`SplitMix64`] in a `tokio::task_local!`. [`with_generator`] puts one in scope for the
//! control node that creates the `Raft`, so a seed replays the same election timeouts on every
//! run, and two control nodes in one process draw independently.

use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::io;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::rt::{AsyncRuntime, OptionalSend};
use openraft_rt_tokio::{TokioMpsc, TokioMutex, TokioOneshot, TokioWatch};
use rand::TryRng;

use crate::control::raft::RaftId;
use crate::rng::SplitMix64;
use crate::router::Entropy;

tokio::task_local! {
    /// The generator of the control node whose task this is.
    static GENERATOR: Arc<Mutex<SplitMix64>>;
}

/// The Raft ID that seeds the generator a draw outside any scope falls back to.
static FALLBACK_SEED: AtomicU64 = AtomicU64::new(0);

/// The generator a draw outside any scope uses. Seeded on first use from [`FALLBACK_SEED`].
static FALLBACK: Mutex<Option<SplitMix64>> = Mutex::new(None);

/// How many draws were made outside any generator scope. Each is a bug.
static OUT_OF_SCOPE: AtomicU64 = AtomicU64::new(0);

/// Whether a draw outside any scope has been logged; it is logged once per process.
static LOGGED: AtomicBool = AtomicBool::new(false);

/// openraft's runtime: Tokio's timers and channels, with randomness from the generator in
/// scope (ADR-0040 point 1).
///
/// It is the only runtime in every build, production and simulation alike. Time comes only from
/// `tokio::time`: turmoil's clock under simulation, paused time in tests, the real clock in
/// production.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ControlRuntime;

/// A handle that draws from the generator of the task it runs in.
///
/// `next_u64` takes the generator's next word, `next_u32` its high 32 bits, and byte fills its
/// words in little-endian order.
#[derive(Debug, Default, Clone, Copy)]
pub struct ControlRng;

/// openraft's instant: `tokio::time::Instant`, so paused time and turmoil's clock govern it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ControlInstant(tokio::time::Instant);

impl ControlInstant {
    /// Returns the Tokio instant.
    #[must_use]
    pub const fn get(self) -> tokio::time::Instant {
        self.0
    }
}

impl openraft::rt::Instant for ControlInstant {
    fn now() -> Self {
        Self(tokio::time::Instant::now())
    }
}

impl Add<Duration> for ControlInstant {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self {
        Self(self.0 + rhs)
    }
}

impl AddAssign<Duration> for ControlInstant {
    fn add_assign(&mut self, rhs: Duration) {
        self.0 += rhs;
    }
}

impl Sub<Duration> for ControlInstant {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self {
        Self(self.0 - rhs)
    }
}

impl Sub for ControlInstant {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Duration {
        self.0.saturating_duration_since(rhs.0)
    }
}

impl SubAssign<Duration> for ControlInstant {
    fn sub_assign(&mut self, rhs: Duration) {
        self.0 -= rhs;
    }
}

/// The seed of a control node's generator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftSeed(u64);

impl RaftSeed {
    /// Returns the seed `value`.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Eight bytes from the environment's entropy (`Environment::entropy()`), read as a
    /// little-endian integer.
    #[must_use]
    pub fn from_entropy(entropy: &dyn Entropy) -> Self {
        let mut bytes = [0_u8; 8];
        entropy.fill(&mut bytes);
        Self(u64::from_le_bytes(bytes))
    }

    /// Returns the seed as an integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Runs `future` with a generator seeded from `seed` in scope, for it and every task it spawns
/// through [`ControlRuntime`].
///
/// `fallback` seeds the generator that a draw outside any scope uses, which would be a bug: the
/// draw is counted and logged at error level once.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the control node opens its group inside it from PR 3"
    )
)]
pub(crate) async fn with_generator<F: Future>(
    seed: RaftSeed,
    fallback: RaftId,
    future: F,
) -> F::Output {
    FALLBACK_SEED.store(fallback, Ordering::Relaxed);
    let generator = Arc::new(Mutex::new(SplitMix64::new(seed.0)));
    GENERATOR.scope(generator, future).await
}

/// How many draws this process made outside any generator scope.
#[cfg(test)]
pub(crate) fn draws_outside_a_scope() -> u64 {
    OUT_OF_SCOPE.load(Ordering::Relaxed)
}

fn draw() -> u64 {
    if let Ok(word) = GENERATOR.try_with(|generator| {
        generator
            .lock()
            .expect("control generator lock poisoned")
            .next_u64()
    }) {
        return word;
    }
    OUT_OF_SCOPE.fetch_add(1, Ordering::Relaxed);
    if !LOGGED.swap(true, Ordering::Relaxed) {
        tracing::error!(
            "the control group drew a random number outside its generator's scope; this is a bug"
        );
    }
    let mut fallback = FALLBACK.lock().expect("fallback generator lock poisoned");
    fallback
        .get_or_insert_with(|| SplitMix64::new(FALLBACK_SEED.load(Ordering::Relaxed)))
        .next_u64()
}

impl TryRng for ControlRng {
    type Error = Infallible;

    fn try_next_u32(&mut self) -> Result<u32, Infallible> {
        let high = draw() >> 32;
        Ok(u32::try_from(high).unwrap_or(u32::MAX))
    }

    fn try_next_u64(&mut self) -> Result<u64, Infallible> {
        Ok(draw())
    }

    fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Infallible> {
        for chunk in dst.chunks_mut(8) {
            let word = draw().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
        Ok(())
    }
}

impl AsyncRuntime for ControlRuntime {
    type JoinError = tokio::task::JoinError;
    type JoinHandle<T: OptionalSend + 'static> = tokio::task::JoinHandle<T>;
    type Sleep = tokio::time::Sleep;
    type Instant = ControlInstant;
    type TimeoutError = tokio::time::error::Elapsed;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = tokio::time::Timeout<T>;
    type ThreadLocalRng = ControlRng;

    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        match GENERATOR.try_with(Arc::clone) {
            Ok(generator) => tokio::task::spawn(GENERATOR.scope(generator, future)),
            Err(_) => tokio::task::spawn(future),
        }
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration)
    }

    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        tokio::time::sleep_until(deadline.0)
    }

    fn timeout<R, F: Future<Output = R> + OptionalSend>(
        duration: Duration,
        future: F,
    ) -> Self::Timeout<R, F> {
        tokio::time::timeout(duration, future)
    }

    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(
        deadline: Self::Instant,
        future: F,
    ) -> Self::Timeout<R, F> {
        tokio::time::timeout_at(deadline.0, future)
    }

    fn is_panic(join_error: &Self::JoinError) -> bool {
        join_error.is_panic()
    }

    fn thread_rng() -> Self::ThreadLocalRng {
        ControlRng
    }

    type Mpsc = TokioMpsc;
    type Watch = TokioWatch;
    type Oneshot = TokioOneshot;
    type Mutex<T: OptionalSend + 'static> = TokioMutex<T>;

    fn new(_threads: usize) -> Self {
        Self
    }

    /// Runs `future` on a new current-thread Tokio runtime. openraft never calls it; it exists
    /// for openraft's runtime test suite.
    fn block_on<F, T>(&mut self, future: F) -> T
    where
        F: Future<Output = T>,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread Tokio runtime builds")
            .block_on(future)
    }

    /// Runs `f` on Tokio's blocking pool, never on an OS thread of its own (openraft's default),
    /// so paused time doesn't advance past it and the runtime's shutdown waits for it.
    fn spawn_blocking<F, T>(f: F) -> impl Future<Output = Result<T, io::Error>> + Send
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let handle = tokio::task::spawn_blocking(f);
        async move { handle.await.map_err(io::Error::other) }
    }
}

impl fmt::Display for RaftSeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use openraft::Config;

    use super::*;

    fn config() -> Config {
        Config {
            election_timeout_min: 1_000,
            election_timeout_max: 2_000,
            ..Config::default()
        }
    }

    /// Eight election timeouts drawn by openraft itself, in a task spawned through the runtime.
    async fn timeouts() -> Vec<u64> {
        ControlRuntime::spawn(async {
            let config = config();
            (0..8)
                .map(|_| config.new_rand_election_timeout::<ControlRuntime>())
                .collect()
        })
        .await
        .expect("the drawing task finishes")
    }

    #[tokio::test]
    async fn the_same_seed_draws_the_same_election_timeouts() {
        let first = with_generator(RaftSeed::new(7), 1, timeouts()).await;
        let again = with_generator(RaftSeed::new(7), 1, timeouts()).await;
        let other = with_generator(RaftSeed::new(8), 1, timeouts()).await;
        assert_eq!(first, again);
        assert_ne!(first, other);
        assert!(first.iter().all(|timeout| (1_000..2_000).contains(timeout)));
    }

    #[tokio::test]
    async fn a_spawned_task_draws_from_its_spawners_generator() {
        let (spawned, direct) = with_generator(RaftSeed::new(42), 1, async {
            let spawned = ControlRuntime::spawn(async { ControlRng.try_next_u64() })
                .await
                .expect("the drawing task finishes");
            let direct = ControlRng.try_next_u64();
            (spawned, direct)
        })
        .await;
        let mut expected = SplitMix64::new(42);
        assert_eq!(spawned, Ok(expected.next_u64()));
        assert_eq!(direct, Ok(expected.next_u64()));
    }

    #[tokio::test]
    async fn two_generators_draw_independently() {
        let (a_tx, mut a_rx) = tokio::sync::mpsc::channel::<()>(1);
        let (b_tx, mut b_rx) = tokio::sync::mpsc::channel::<()>(1);
        let a = tokio::spawn(with_generator(RaftSeed::new(1), 1, async move {
            let mut draws = Vec::new();
            for _ in 0..4 {
                draws.push(ControlRng.try_next_u64());
                b_tx.send(()).await.expect("b is listening");
                a_rx.recv().await;
            }
            draws
        }));
        let b = tokio::spawn(with_generator(RaftSeed::new(2), 1, async move {
            let mut draws = Vec::new();
            for _ in 0..4 {
                b_rx.recv().await;
                draws.push(ControlRng.try_next_u64());
                a_tx.send(()).await.expect("a is listening");
            }
            draws
        }));
        let a = a.await.expect("a finishes");
        let b = b.await.expect("b finishes");
        let mut one = SplitMix64::new(1);
        let mut two = SplitMix64::new(2);
        assert_eq!(a, (0..4).map(|_| Ok(one.next_u64())).collect::<Vec<_>>());
        assert_eq!(b, (0..4).map(|_| Ok(two.next_u64())).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_draw_outside_a_scope_is_counted() {
        let before = draws_outside_a_scope();
        let mut bytes = [0_u8; 12];
        let _ = ControlRng.try_fill_bytes(&mut bytes);
        // Twelve bytes take two words.
        assert!(draws_outside_a_scope() >= before + 2);
    }

    #[tokio::test(start_paused = true)]
    async fn spawn_blocking_runs_under_paused_time() {
        let started = tokio::time::Instant::now();
        let blocking = ControlRuntime::spawn_blocking(|| {
            std::thread::sleep(Duration::from_millis(20));
            7
        });
        // With paused time, an idle runtime jumps to its next timer. Work on Tokio's blocking
        // pool holds that jump back, so the blocking call finishes before an hour of paused time
        // passes; an OS thread of its own wouldn't.
        tokio::select! {
            biased;
            result = blocking => assert_eq!(result.expect("the blocking call finishes"), 7),
            () = tokio::time::sleep(Duration::from_secs(3_600)) => {
                panic!("paused time advanced past a running blocking call");
            }
        }
        assert!(started.elapsed() < Duration::from_secs(3_600));
    }
}
