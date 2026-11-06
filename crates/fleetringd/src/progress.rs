//! Spinners on stderr for the commands that can take noticeably long (ADR-0043, point 6;
//! `docs/cli.md`, section 5).
//!
//! A spinner shows only where it can't harm a script: both streams are terminals, the output is
//! human, and nothing asked for quiet or plain output. It never writes to stdout, so `$(…)` and
//! the MCP server see the same bytes with or without one, and `fleetring mcp` never starts one.

use std::borrow::Cow;
use std::io::{self, IsTerminal as _, Write as _};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use fleetringd::upgrade::Steps;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use tokio::task::JoinHandle;
use tracing_subscriber::fmt::MakeWriter;

/// How long a command runs before its spinner shows, so that a fast one never flickers.
const SHOW_AFTER: Duration = Duration::from_millis(150);

/// How often the spinner turns once it shows.
const TICK: Duration = Duration::from_millis(80);

/// Whether the command's output is for programs: `-o json`, `jsonl` or `--json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MachineOutput {
    /// The output is for people.
    No,
    /// The output is for programs.
    Yes,
}

/// Whether `-q` was given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Quiet {
    /// Progress is welcome.
    No,
    /// Print the result alone.
    Yes,
}

/// What decides whether a spinner may show, captured once (`docs/cli.md`, section 5).
#[expect(
    clippy::struct_excessive_bools,
    reason = "a snapshot of independent facts about the process, decided by one pure function"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Surroundings {
    /// Whether stdout is a terminal.
    pub(crate) stdout_terminal: bool,
    /// Whether stderr is a terminal.
    pub(crate) stderr_terminal: bool,
    /// Whether the output is for programs.
    pub(crate) machine_output: bool,
    /// Whether `-q` was given.
    pub(crate) quiet: bool,
    /// Whether `NO_COLOR` is set and not empty.
    pub(crate) no_color: bool,
    /// Whether `TERM` is `dumb`.
    pub(crate) term_dumb: bool,
    /// Whether `CI` is set and not empty.
    pub(crate) ci: bool,
}

impl Surroundings {
    /// The process's own: `is_terminal()` on both streams; `NO_COLOR` and `CI` set and not empty;
    /// `TERM` equal to `dumb`.
    pub(crate) fn capture(machine_output: MachineOutput, quiet: Quiet) -> Self {
        let set = |name: &str| std::env::var_os(name).is_some_and(|value| !value.is_empty());
        Self {
            stdout_terminal: io::stdout().is_terminal(),
            stderr_terminal: io::stderr().is_terminal(),
            machine_output: machine_output == MachineOutput::Yes,
            quiet: quiet == Quiet::Yes,
            no_color: set("NO_COLOR"),
            term_dumb: std::env::var_os("TERM").is_some_and(|term| term == "dumb"),
            ci: set("CI"),
        }
    }

    /// Whether a spinner may show: both streams are terminals, the output is for people, and none
    /// of `-q`, `NO_COLOR`, `TERM=dumb` and `CI` applies.
    pub(crate) fn spinners(&self) -> bool {
        self.stdout_terminal
            && self.stderr_terminal
            && !self.machine_output
            && !self.quiet
            && !self.no_color
            && !self.term_dumb
            && !self.ci
    }
}

/// Where a spinner is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Started, and not yet drawn.
    Waiting,
    /// Drawn on stderr.
    Shown,
    /// Finished or dropped.
    Finished,
}

/// A spinner that shows after 150 ms, or nothing.
///
/// Dropping one without finishing it clears it, so an error returned with `?` leaves a clean
/// line.
#[derive(Debug)]
pub(crate) struct Progress {
    live: Option<Live>,
}

/// The parts of a spinner that is not hidden.
#[derive(Debug)]
struct Live {
    bar: ProgressBar,
    phase: Arc<Mutex<Phase>>,
    reveal: JoinHandle<()>,
    id: u64,
}

impl Progress {
    /// Starts a spinner that shows `message` after 150 ms, if `surroundings` allow one and a
    /// runtime is there to time it; otherwise a hidden one.
    pub(crate) fn start(
        message: impl Into<Cow<'static, str>>,
        surroundings: &Surroundings,
    ) -> Self {
        if !surroundings.spinners() {
            return Self::hidden();
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return Self::hidden();
        };
        let bar = ProgressBar::with_draw_target(None, ProgressDrawTarget::hidden());
        bar.set_style(ProgressStyle::default_spinner());
        bar.set_message(message);
        let phase = Arc::new(Mutex::new(Phase::Waiting));
        let reveal = runtime.spawn({
            let bar = bar.clone();
            let phase = Arc::clone(&phase);
            async move {
                tokio::time::sleep(SHOW_AFTER).await;
                let mut phase = locked(&phase);
                if *phase == Phase::Waiting {
                    *phase = Phase::Shown;
                    bar.set_draw_target(ProgressDrawTarget::stderr());
                    bar.enable_steady_tick(TICK);
                }
            }
        });
        let id = register(&bar);
        Self {
            live: Some(Live {
                bar,
                phase,
                reveal,
                id,
            }),
        }
    }

    /// A spinner that never shows and writes nothing.
    pub(crate) fn hidden() -> Self {
        Self { live: None }
    }

    /// Changes the message the spinner shows, as a command moves to its next step.
    pub(crate) fn set_message(&self, message: impl Into<Cow<'static, str>>) {
        if let Some(live) = &self.live {
            live.bar.set_message(message);
        }
    }

    /// Replaces the spinner with `✓ <message>` on stderr if it was shown; otherwise prints
    /// nothing.
    pub(crate) fn finish_ok(self, message: impl Into<Cow<'static, str>>) {
        if let Some(line) = self.done_line(message) {
            // Nothing to do if stderr is gone.
            let _ = writeln!(io::stderr(), "{line}");
        }
    }

    /// Clears the spinner, for a command that failed or whose own output follows.
    pub(crate) fn finish_err(self) {
        drop(self);
    }

    /// Ends the spinner and returns the line that replaces it, if it was shown.
    fn done_line(mut self, message: impl Into<Cow<'static, str>>) -> Option<String> {
        self.end().then(|| format!("\u{2713} {}", message.into()))
    }

    /// Ends the spinner, clearing its line, and returns whether it had been shown.
    fn end(&mut self) -> bool {
        let Some(live) = self.live.take() else {
            return false;
        };
        let was = {
            let mut phase = locked(&live.phase);
            std::mem::replace(&mut *phase, Phase::Finished)
        };
        live.reveal.abort();
        live.bar.finish_and_clear();
        unregister(live.id);
        was == Phase::Shown
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.end();
    }
}

/// A spinner for a command with steps, such as `upgrade`: it starts at the first step the command
/// reports, not before, so that a prompt the command asks first isn't drawn over.
#[derive(Debug)]
pub(crate) struct Stepped {
    surroundings: Surroundings,
    progress: Arc<Mutex<Option<Progress>>>,
}

impl Stepped {
    pub(crate) fn new(surroundings: Surroundings) -> Self {
        Self {
            surroundings,
            progress: Arc::default(),
        }
    }

    /// What to give the command, so that its spinner shows each step as it starts.
    pub(crate) fn steps(&self) -> Steps {
        let surroundings = self.surroundings;
        let progress = Arc::clone(&self.progress);
        Steps::new(move |step| {
            let mut slot = locked(&progress);
            match slot.as_ref() {
                Some(progress) => progress.set_message(step.message()),
                None => *slot = Some(Progress::start(step.message(), &surroundings)),
            }
        })
    }

    /// Replaces the spinner with `✓ <message>` if the command showed one for long enough.
    pub(crate) fn finish_ok(self, message: impl Into<Cow<'static, str>>) {
        if let Some(progress) = locked(&self.progress).take() {
            progress.finish_ok(message);
        }
    }
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // The guarded values are plain state that a panic can't leave half-written.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The spinner log lines go through, with the number `register` gave it.
type Active = Mutex<Option<(u64, ProgressBar)>>;

fn active() -> &'static Active {
    static ACTIVE: OnceLock<Active> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(None))
}

/// Makes `bar` the spinner that log lines go through, and returns its number.
fn register(bar: &ProgressBar) -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    *locked(active()) = Some((id, bar.clone()));
    id
}

/// Stops log lines going through the spinner numbered `id`, if it is still the active one.
fn unregister(id: u64) {
    let mut slot = locked(active());
    if slot.as_ref().is_some_and(|(active, _)| *active == id) {
        *slot = None;
    }
}

/// Writes log lines to stderr, around the active spinner so that they don't tear its line.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SuspendingStderr;

impl<'writer> MakeWriter<'writer> for SuspendingStderr {
    type Writer = Self;

    fn make_writer(&'writer self) -> Self::Writer {
        *self
    }
}

impl io::Write for SuspendingStderr {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Cloned out of the slot, so that the lock isn't held while writing.
        let bar = locked(active()).as_ref().map(|(_, bar)| bar.clone());
        match bar {
            Some(bar) => bar.suspend(|| io::stderr().write(buf)),
            None => io::stderr().write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stderr().flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every fact that allows a spinner.
    const ALLOWED: Surroundings = Surroundings {
        stdout_terminal: true,
        stderr_terminal: true,
        machine_output: false,
        quiet: false,
        no_color: false,
        term_dumb: false,
        ci: false,
    };

    #[test]
    fn spinners_need_both_streams_to_be_terminals() {
        assert!(ALLOWED.spinners());
        for (stdout_terminal, stderr_terminal) in [(false, true), (true, false), (false, false)] {
            let surroundings = Surroundings {
                stdout_terminal,
                stderr_terminal,
                ..ALLOWED
            };
            assert!(
                !surroundings.spinners(),
                "stdout {stdout_terminal}, stderr {stderr_terminal}"
            );
        }
    }

    #[test]
    fn spinners_are_off_for_json_quiet_no_color_dumb_and_ci() {
        let each = [
            (
                "json",
                Surroundings {
                    machine_output: true,
                    ..ALLOWED
                },
            ),
            (
                "quiet",
                Surroundings {
                    quiet: true,
                    ..ALLOWED
                },
            ),
            (
                "no color",
                Surroundings {
                    no_color: true,
                    ..ALLOWED
                },
            ),
            (
                "dumb terminal",
                Surroundings {
                    term_dumb: true,
                    ..ALLOWED
                },
            ),
            (
                "CI",
                Surroundings {
                    ci: true,
                    ..ALLOWED
                },
            ),
        ];
        for (name, surroundings) in each {
            assert!(!surroundings.spinners(), "{name}");
        }
    }

    #[test]
    fn a_hidden_progress_writes_nothing() {
        let progress = Progress::hidden();
        progress.set_message("still nothing");
        assert_eq!(progress.done_line("done"), None);
    }

    #[tokio::test]
    async fn surroundings_that_forbid_spinners_start_a_hidden_one() {
        let progress = Progress::start(
            "working",
            &Surroundings {
                ci: true,
                ..ALLOWED
            },
        );
        assert!(progress.live.is_none());
        assert_eq!(progress.done_line("done"), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_fast_command_leaves_no_line() {
        let progress = Progress::start("working", &ALLOWED);
        assert!(progress.live.is_some());
        tokio::time::advance(SHOW_AFTER / 2).await;
        assert_eq!(progress.done_line("done"), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_spinner_that_showed_is_replaced_by_a_check_line() {
        let progress = Progress::start("working", &ALLOWED);
        tokio::time::sleep(SHOW_AFTER * 2).await;
        assert_eq!(
            progress.done_line("created 'notes'").as_deref(),
            Some("\u{2713} created 'notes'")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_spinner_clears_the_log_slot() {
        let progress = Progress::start("working", &ALLOWED);
        let id = progress.live.as_ref().map(|live| live.id);
        drop(progress);
        assert!(
            locked(active())
                .as_ref()
                .is_none_or(|(active, _)| Some(*active) != id)
        );
    }
}
