---
id: FR-065
title: "Fault injection on real daemons: partitions, kills, pauses, and clock skew, judged by the history checker"
status: todo
priority: P1
size: L
milestone: M7 Durability and operations
depends_on: [FR-036, FR-046]
adrs: [ADR-0018, ADR-0020, ADR-0036]
---

# FR-065: Fault injection on real daemons: partitions, kills, pauses, and clock skew, judged by the history checker

## Context

The roadmap's first "safe to use" criterion asks for no known way to lose an acknowledged write, shown on every simulation run, on nightly sweeps, and on "a fault-injection run on real processes" (E1, E9). The simulation (ADR-0020) runs the real daemon code, but over turmoil's network and clock, with SQLite inline on one thread: it can't find bugs in real TCP and HTTP/2 timeouts, real fsync and thread scheduling, process pauses, or wall clocks that jump. ADR-0036 decides how to test real daemons: an in-repo Rust harness, `fleetring-chaos`, that runs five `fleetringd` processes in Docker containers, injects partitions with `iptables`, kills and pauses with signals, and skews the control node's clock with `libfaketime`, then judges the run with FR-036's history checker. It is Jepsen's method without the Clojure toolchain.

## Scope

- **`crates/fleetring-chaos`:** a binary that generates certificates and configuration, starts the containers, runs a seeded workload of creates, writes, and reads through `fleetringd::data::Client`, runs a seeded fault schedule, heals, waits for convergence, stops the fleet, reads every replica's rows from its data directory, and checks the history.
- **Faults:** partitions (random halves, a primary isolated, the control node isolated, a bridge), `SIGKILL` and restart, `SIGSTOP` and `SIGCONT`, and clock skew and strobe on the control node.
- **Checking:** FR-036's `history::check` and `check::control_log_replays`, reused from `fleetringd::sim`; a self-test mode that must find a violation, so a harness that checks nothing can't pass.
- **Output:** a run directory with the history, the fault schedule, every node's log, and the verdict; the seed and the command that replays the schedule.
- **CI:** a nightly workflow and manual dispatch, never on pull requests. Unit tests of the harness's pure parts run in normal CI.
- **Not in scope:** disk faults (lost fsyncs, torn writes), Jepsen itself, and control-group faults, which follow FR-064 (the plan's last step).

## Acceptance criteria

- [ ] ADR-0036 is accepted and listed in `docs/adr/README.md`.
- [ ] `cargo run -p fleetring-chaos -- run --scenario mixed --time-limit 600s` on a Linux host with Docker starts five real daemons, runs the workload and the faults, heals, and exits 0 with the checker's verdict `valid`.
- [ ] Every fault kind runs in at least one scenario, and each appears in the run's `schedule.txt` with its start and end times and targets.
- [ ] The verdict comes from FR-036's checker over the recorded history and the rows read from every node's data directory after the run, plus the control log replay check; exit code 1 means a violation, 2 a harness failure.
- [ ] `--self-test` creates `acks = local` databases but checks them as `quorum`, and reports a lost acknowledged write within the default time limit on the `partitions` scenario.
- [ ] A failing run prints its seed and a command that reruns its fault schedule, and its run directory holds the history, schedule, verdict, and node logs.
- [ ] `.github/workflows/chaos.yml` runs the five scenarios nightly and on manual dispatch, uploads run directories on failure, and doesn't run on pull requests.
- [ ] The harness's schedule generation and command building have unit tests that run in normal CI without Docker.
- [ ] The harness README, `docs/roadmap.md` (E1, E9), and the lexicon are updated.

Note on the second criterion: the binary needs the crate's `harness` feature (ADR-0036, Consequences), so the command that runs is `cargo run -p fleetring-chaos --features harness -- run --scenario mixed --time-limit 600s`. The issue's text is to be updated to that form when ADR-0036 is accepted.

## Implementation plan

Each step is about one commit, with the subject given and a `Refs: FR-065` trailer. Paths are relative to the repository root. Rules for every step:

- New public types derive `Debug`; errors are `#[non_exhaustive]`, derive `thiserror::Error`, and have lowercase messages. No `unwrap()` outside tests; no `bool` parameters (presence flags map to two-variant enums); `BTreeMap`/`BTreeSet`, never `HashMap`/`HashSet`.
- The harness reads time only through `tokio::time` (`tokio::time::Instant`, `sleep`, `sleep_until`, `timeout`), and randomness only from `fleetringd::sim::SimRng`.
- Docker is invoked only through `std::process::Command::new("docker")`, called from `tokio::task::spawn_blocking` inside async code.

### Step 1: expose replica rows and data operations to harnesses

Subject: `refactor(fleetringd): let harnesses read replica rows and run data operations`

- `crates/fleetringd/src/sim/check.rs`: add, moved from the private `read_rows` in `src/sim/fleet.rs` (same body; the `tempfile` scratch copy stays):

  ```rust
  /// Reads the values in the [`WRITES_TABLE`] of `database`'s copy under the data directory
  /// `data_dir`, from a scratch copy of its files, so a follower's mirrored log is replayed as a
  /// promotion would replay it. A node with no copy holds no rows.
  ///
  /// # Errors
  ///
  /// If the copy exists but can't be copied or read.
  pub fn read_replica_rows(
      data_dir: &Path,
      database: &DatabaseName,
  ) -> Result<BTreeSet<i64>, Box<dyn std::error::Error + Send + Sync>>;
  ```

  The function computes the copy's directory as `data_dir.join(crate::daemon::DATABASES_DIR).join(crate::layout::database_dir(database))`. Change the visibility of `DATABASES_DIR` in `src/daemon.rs` only if it isn't already `pub(crate)` (it is). Delete `read_rows` from `src/sim/fleet.rs`; `Fleet::replica_rows` calls `check::read_replica_rows(&host.data_dir, database)` and maps the error exactly as before.
- `crates/fleetringd/src/sim/client.rs`: extract the `Operation::Create`, `Operation::Write`, and `Operation::Read` arms of the private `execute` into:

  ```rust
  /// Sends a data-plane `operation` (create, write, or read) through `data` and returns its
  /// outcome, exactly as a simulated client does. Any other operation returns `None`.
  pub async fn data_operation(data: &Client, operation: &Operation) -> Option<Outcome>;
  ```

  `execute` calls it first and returns its outcome when it is `Some`. The private `create` and `read` helpers stay where they are and are called from `data_operation`.
- `crates/fleetringd/src/sim/mod.rs`: `pub use client::data_operation;` (make `mod client` stay private).
- Tests in `crates/fleetringd/src/sim/check.rs` (`#[cfg(test)]`):
  - `a_node_without_a_copy_holds_no_rows`: an empty temporary directory gives an empty set for `"chaos/db-0"`.
  - `rows_are_read_from_the_encoded_database_directory`: create `<tmp>/databases/chaos%2fdb-0/data.db` with `rusqlite`, `CREATE TABLE writes (value INTEGER NOT NULL)`, insert 1 and 2; `read_replica_rows(tmp, "chaos/db-0")` returns `{1, 2}`.
- The simulation suite (`cargo test -p fleetringd --features simulation --test sim`) passes unchanged.

### Step 2: the crate, its image, and the MSRV checks

Subject: `feat(chaos): add the fleetring-chaos crate and its node image`

- `crates/fleetring-chaos/Cargo.toml`:

  ```toml
  [package]
  name = "fleetring-chaos"
  description = "Fault injection on real fleetringd processes, judged by the history checker (ADR-0036)."
  version = "0.0.0"
  publish = false
  edition.workspace = true
  # Needs fleetringd, which needs 1.89 (ADR-0015, point 7).
  rust-version = "1.89"
  license.workspace = true
  repository.workspace = true
  homepage.workspace = true
  authors.workspace = true

  [features]
  # The harness itself. Off by default: it turns on fleetringd's `simulation` feature, which
  # Cargo would otherwise unify into every workspace build (ADR-0036, Consequences).
  harness = ["fleetringd/simulation"]

  [dependencies]
  clap = { version = "4.6.7", features = ["derive"] }
  fleetring = { path = "../fleetring", features = ["serde"] }
  fleetring-control = { version = "0.0.0", path = "../fleetring-control" }
  fleetringd = { version = "0.2.0", path = "../fleetringd" }
  serde = { workspace = true, features = ["derive"] }
  serde_json.workspace = true
  thiserror.workspace = true
  tokio = { version = "1.54.1", features = ["rt-multi-thread", "macros", "signal", "sync", "time"] }
  tonic = "0.14"

  [[bin]]
  name = "fleetring-chaos"
  path = "src/main.rs"
  required-features = ["harness"]

  [lints]
  workspace = true
  ```

  Use the same versions `crates/fleetringd/Cargo.toml` pins; `fleetringd`'s version is whatever its manifest says at the time.
- `crates/fleetring-chaos/clippy.toml`: a copy of `crates/fleetringd/clippy.toml`.
- `crates/fleetring-chaos/src/lib.rs`: crate docs summarizing ADR-0036, and the modules, each added by the step that writes it:

  ```rust
  pub mod config;      // step 3, always built
  pub mod docker;      // step 4, always built
  pub mod duration;    // this step, always built
  pub mod node;        // this step, always built
  #[cfg(feature = "harness")] pub mod schedule;  // step 5
  #[cfg(feature = "harness")] pub mod inject;    // step 6
  #[cfg(feature = "harness")] pub mod workload;  // step 7
  #[cfg(feature = "harness")] pub mod run;       // step 8
  ```

  and the `assert_debug_send_sync` block of `crates/fleetringd/src/lib.rs`'s kind for every public type.
- `crates/fleetring-chaos/src/node.rs`:

  ```rust
  /// One of the five nodes: its name, container, address, and node role.
  #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
  pub enum Node { N1, N2, N3, N4, N5 }

  impl Node {
      pub const ALL: [Node; 5] = [Node::N1, Node::N2, Node::N3, Node::N4, Node::N5];
      /// The control node: `n1`, which runs node role `both`.
      pub const CONTROL: Node = Node::N1;
      pub fn name(self) -> &'static str;          // "n1" … "n5"
      pub fn container(self) -> String;           // "fleetring-chaos-n1"
      pub fn ip(self) -> Ipv4Addr;                // 10.77.0.11 … 10.77.0.15
      pub fn address(self) -> String;             // "10.77.0.11:7400"
      pub fn role(self) -> NodeRole;              // N1: Both; others: Data
      pub fn from_node_id(id: &NodeId) -> Option<Node>; // "n1" … "n5"
  }

  pub const NETWORK: &str = "fleetring-chaos";
  pub const SUBNET: &str = "10.77.0.0/24";
  pub const GATEWAY: &str = "10.77.0.1";
  pub const PORT: u16 = 7400;
  pub const FLEET: &str = "chaos";
  pub const PRINCIPAL: &str = "chaos";
  pub const DEFAULT_IMAGE: &str = "fleetring-chaos:local";
  ```

- `crates/fleetring-chaos/src/duration.rs`: `pub fn parse_duration(text: &str) -> Result<Duration, ParseDurationError>` accepting `<n>s`, `<n>m`, `<n>h` (the CLI style guide's form; `d` too), with `#[non_exhaustive] pub enum ParseDurationError { Empty, Unit { text: String }, Number { text: String } }`. Test `time_limits_parse_like_ttls`: `"600s"` is 600 s, `"10m"` is 600 s, `"1h"` is 3600 s; `""`, `"10"`, `"10x"`, `"-5s"` are errors.
- `crates/fleetring-chaos/src/main.rs` (built only with `harness`): the clap CLI below; every subcommand body is `todo!()` until its step, except `schedule`, which arrives in step 5.

  ```text
  fleetring-chaos run --scenario <partitions|kills|pauses|clock|mixed>
                      [--seed <u64>]            # random from /dev/urandom if unset; printed first
                      [--time-limit <duration>] # the fault phase; default 600s
                      [--self-test]             # ADR-0036 point 7
                      [--image <tag>]           # default fleetring-chaos:local
                      [--skip-build]            # use --image as it is
                      [--out <dir>]             # default target/chaos
                      [--keep]                  # leave containers and the network after the run
  fleetring-chaos schedule --scenario <s> --seed <u64> [--time-limit <d>] [--self-test]
                      # prints the planned schedule; pure, runs on any OS
  fleetring-chaos clean # removes leftover containers and the network
  ```

  The seed when unset: read 8 bytes from `/dev/urandom` with `std::fs::File` (Linux only; `run` refuses other hosts before this) as a little-endian `u64`. Presence flags map to enums: `--self-test` to `Mode::{Normal, SelfTest}`, `--skip-build` to `Build::{Image, Skip}`, `--keep` to `Teardown::{Remove, Keep}`.
- `deploy/chaos/Dockerfile`:

  ```dockerfile
  # syntax=docker/dockerfile:1

  # The node image for the fault-injection harness (FR-065, ADR-0036). Not a release image: it
  # runs as the harness's user, with iptables and libfaketime for faults.
  #
  #   docker build -f deploy/chaos/Dockerfile -t fleetring-chaos:local .

  # Keep equal to RUST_VERSION in the root Dockerfile and .github/workflows/release.yml.
  ARG RUST_VERSION=1.99

  FROM rust:${RUST_VERSION}-slim-bookworm AS build
  WORKDIR /fleetring
  COPY . .
  ENV CARGO_INCREMENTAL=0
  RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
      --mount=type=cache,target=/fleetring/target,sharing=locked \
      cargo build --locked --release -p fleetringd \
      && install -D -m 0755 target/release/fleetringd /out/fleetringd

  # glibc, dynamically linked, so LD_PRELOAD can load libfaketime.
  FROM debian:bookworm-slim
  RUN apt-get update \
      && apt-get install -y --no-install-recommends iptables libfaketime procps ca-certificates \
      && rm -rf /var/lib/apt/lists/* \
      && ln -s "$(find /usr/lib -name libfaketime.so.1 -print -quit)" /usr/local/lib/libfaketime.so.1 \
      && test -e /usr/local/lib/libfaketime.so.1
  COPY --from=build /out/fleetringd /usr/local/bin/fleetringd
  EXPOSE 7400
  ENTRYPOINT ["/usr/local/bin/fleetringd"]
  CMD ["--config", "/etc/fleetring/fleetringd.toml"]
  ```

  The root `.dockerignore` applies (it excludes `**/target`, so run directories never enter the build context).
- `.github/workflows/ci.yml`, job `msrv`: `cargo check --workspace --exclude fleetringd --exclude fleetring-chaos --all-features --all-targets` on 1.88, and `cargo +1.89 check -p fleetringd -p fleetring-chaos --all-features --all-targets`. `CONTRIBUTING.md`'s "Before you push" block gets the same two lines.
- Other CI jobs: `fmt`, `clippy`, `test` (all legs), `docs`, and `cache` build the workspace and pick the crate up with no change: Linux's `--all-features` legs build it with `harness`; macOS, Windows, and Linux's default-feature and no-default-feature legs build it without, so `fleetringd`'s `simulation` feature stays off there. `i686`, `s390x`, and `wasm` name their packages with `-p` and never build it. `cargo deny` sees no new dependency (every one above is already in the tree).

### Step 3: node configuration and certificates

Subject: `feat(chaos): render node configurations and issue the fleet's certificates`

- `crates/fleetring-chaos/src/config.rs`:

  ```rust
  /// The TOML configuration of `node`, as the container sees its files: data in `/data`,
  /// certificates in `/tls`.
  pub fn render_config(node: Node) -> String;

  /// What the harness writes into its clock file for an offset of `seconds`: `+0s`, `+42s`,
  /// `-42s`.
  pub fn faketime_offset(seconds: i64) -> String;
  ```

  `render_config(Node::N2)` returns exactly (with `n2`'s values substituted for each node; `n1` has `role = "both"` and no `[control] address` line):

  ```toml
  data_dir = "/data"
  listen = "0.0.0.0:7400"
  role = "data"

  [node]
  id = "n2"
  advertise = "10.77.0.12:7400"

  [control]
  address = "10.77.0.11:7400"

  [membership]
  on_shutdown = "stay"

  [tls]
  ca = "/tls/ca.crt"
  cert = "/tls/node.n2.crt"
  key = "/tls/node.n2.key"

  [[principal]]
  name = "chaos"
  scopes = ["admin:chaos/*", "fleet"]
  ```

  Every other setting keeps the daemon's default (10 s leases, 2 s failover margin, 5 s heartbeat timeout, 1 s heartbeats), so the run tests the defaults operators get.
- Certificates are issued by the image under test, in step 8's `run`, with these commands (`<tls>` is `<run>/tls`, `<uid>:<gid>` the owner of the run directory, read with `std::os::unix::fs::MetadataExt` under `#[cfg(unix)]`):

  ```text
  docker run --rm --user <uid>:<gid> -v <tls>:/tls <image> cert init --fleet chaos --dir /tls
  docker run --rm --user <uid>:<gid> -v <tls>:/tls <image> cert node nX --addresses 10.77.0.1X --dir /tls   # X = 1..5
  docker run --rm --user <uid>:<gid> -v <tls>:/tls <image> cert client chaos --dir /tls
  ```

  The builders for these argument lists live in `docker.rs` (step 4) as `cert_init_args`, `cert_node_args`, and `cert_client_args`.
- Tests in `config.rs`:
  - `rendered_configs_are_valid_daemon_configs`: for every node, `fleetringd::Config::from_toml(&render_config(node))` succeeds; the result has the node's role, `advertise` equal to `node.address()`, `membership.on_shutdown == OnShutdown::Stay`, and one principal named `chaos` with the two scopes.
  - `only_the_control_node_runs_control`: `render_config(Node::N1)` contains `role = "both"` and no `[control]` table; the others contain `address = "10.77.0.11:7400"`.
  - `offsets_are_signed_seconds`: `faketime_offset(0) == "+0s"`, `faketime_offset(42) == "+42s"`, `faketime_offset(-42) == "-42s"`.

### Step 4: Docker commands and the container fleet

Subject: `feat(chaos): build the docker commands that start, fault, and stop the nodes`

- `crates/fleetring-chaos/src/docker.rs`. Every builder is pure and returns the arguments after `docker`, as `Vec<String>`; paths are passed as `&str` and joined with `/`, since the harness runs only on Linux, so the builders' tests pass on every OS.

  ```rust
  pub fn network_create_args() -> Vec<String>;
  //   network create --driver bridge --subnet 10.77.0.0/24 --gateway 10.77.0.1 fleetring-chaos
  pub fn network_rm_args() -> Vec<String>;            // network rm fleetring-chaos
  pub fn build_args(image: &str) -> Vec<String>;      // build -f deploy/chaos/Dockerfile -t <image> .
  pub fn run_args(node: Node, run_dir: &str, image: &str, user: &str) -> Vec<String>;
  pub fn partition_args(container: &str, peer: Ipv4Addr) -> [Vec<String>; 2];
  pub fn heal_args(container: &str) -> [Vec<String>; 2];
  pub fn signal_args(container: &str, signal: Signal) -> Vec<String>; // kill --signal <KILL|STOP|CONT> <c>
  pub fn start_args(container: &str) -> Vec<String>;  // start <c>
  pub fn stop_args(container: &str) -> Vec<String>;   // stop --time 10 <c>
  pub fn logs_args(container: &str) -> Vec<String>;   // logs <c>
  pub fn rm_args(containers: &[String]) -> Vec<String>; // rm --force <c>...
  pub fn cert_init_args(tls_dir: &str, image: &str, user: &str) -> Vec<String>;
  pub fn cert_node_args(node: Node, tls_dir: &str, image: &str, user: &str) -> Vec<String>;
  pub fn cert_client_args(tls_dir: &str, image: &str, user: &str) -> Vec<String>;

  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub enum Signal { Kill, Stop, Cont }
  ```

  `run_args(Node::N2, "/r", "img", "1000:1000")` is exactly:

  ```text
  run --detach --name fleetring-chaos-n2 --hostname n2
      --network fleetring-chaos --ip 10.77.0.12 --cap-add NET_ADMIN --user 1000:1000
      --env LD_PRELOAD=/usr/local/lib/libfaketime.so.1
      --env FAKETIME_TIMESTAMP_FILE=/var/lib/faketime/offset
      --env FAKETIME_NO_CACHE=1
      --env FAKETIME_DONT_FAKE_MONOTONIC=1
      --env RUST_LOG=info
      --volume /r/nodes/n2/data:/data
      --volume /r/tls:/tls:ro
      --volume /r/nodes/n2/fleetringd.toml:/etc/fleetring/fleetringd.toml:ro
      --volume /r/nodes/n2/faketime:/var/lib/faketime
      img
  ```

  `partition_args("fleetring-chaos-n2", 10.77.0.13)` is `exec --user 0:0 fleetring-chaos-n2 iptables -A INPUT -s 10.77.0.13 -j DROP` and `exec --user 0:0 fleetring-chaos-n2 iptables -A OUTPUT -d 10.77.0.13 -j DROP`. `heal_args(c)` is `exec --user 0:0 <c> iptables -F INPUT` and `… iptables -F OUTPUT`.
- The executor, in the same module:

  ```rust
  /// Runs `docker` with arguments, and fails with its stderr when it exits non-zero.
  #[derive(Debug, Clone, Default)]
  pub struct Docker;

  impl Docker {
      pub fn run(&self, args: &[String]) -> Result<String, DockerError>;      // stdout, trimmed
      pub async fn run_async(&self, args: Vec<String>) -> Result<String, DockerError>; // spawn_blocking
      pub fn run_ignoring_errors(&self, args: &[String]);                     // cleanup only
  }

  #[derive(Debug, thiserror::Error)]
  #[non_exhaustive]
  pub enum DockerError {
      #[error("can't run docker: {source}")]
      Spawn { source: std::io::Error },
      #[error("docker {command} failed with {status}: {stderr}")]
      Failed { command: String, status: String, stderr: String },
      #[error("docker blocking task failed: {reason}")]
      Join { reason: String },
  }
  ```

- Tests in `docker.rs` (run on every OS):
  - `run_args_mount_the_node_files_and_set_libfaketime`: the exact vector above for `n2`, and for `n1` the IP `10.77.0.11` and its own mounts.
  - `partitions_drop_both_directions`: `partition_args` returns the two `iptables` invocations above; each starts with `exec --user 0:0 <container>`.
  - `a_heal_flushes_both_chains`.
  - `signals_use_docker_kill`: `signal_args(c, Signal::Stop)` is `kill --signal STOP <c>`, and likewise `KILL` and `CONT`.
  - `certificates_come_from_the_image_under_test`: `cert_node_args(Node::N4, "/r/tls", "img", "1:2")` is `run --rm --user 1:2 --volume /r/tls:/tls img cert node n4 --addresses 10.77.0.14 --dir /tls`.

### Step 5: seeded fault schedules

Subject: `feat(chaos): generate fault schedules from a seed`

- `crates/fleetring-chaos/src/schedule.rs` (feature `harness`):

  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
  pub enum Scenario { Partitions, Kills, Pauses, Clock, Mixed }

  #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
  pub enum FaultKind {
      PartitionHalves, IsolatePrimary, IsolateControl, Bridge, Kill, Pause, ClockSkew, ClockStrobe,
  }

  /// A fault, with every random choice already made. Only `IsolatePrimary` resolves its target
  /// when it starts, from the database's current primary.
  #[derive(Debug, Clone, PartialEq, Eq)]
  #[non_exhaustive]
  pub enum Fault {
      /// Drops traffic between every pair in `cut`, both ways.
      Partition { kind: FaultKind, cut: Vec<(Node, Node)> },
      /// Isolates the primary of the database with this index (0..8) from every other node.
      IsolatePrimary { database: usize },
      Kill { node: Node },
      Pause { node: Node },
      /// Holds the control node's wall clock `offset_s` seconds off.
      ClockSkew { offset_s: i64 },
      /// Flips the control node's wall clock between `+offset_s` and `-offset_s` every 500 ms.
      ClockStrobe { offset_s: i64 },
  }

  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct PlannedFault { pub start: Duration, pub end: Duration, pub fault: Fault }

  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct Schedule { pub scenario: Scenario, pub seed: u64, pub faults: Vec<PlannedFault> }

  /// When the first fault may start, after the workload starts.
  pub const FIRST_FAULT: Duration = Duration::from_secs(10);
  pub const STROBE_PERIOD: Duration = Duration::from_millis(500);
  pub const MAX_CLOCK_OFFSET_S: u64 = 60;

  impl Scenario {
      /// The fault kinds the scenario draws from, in `FaultKind` order.
      pub fn kinds(self, mode: Mode) -> &'static [FaultKind];
  }

  impl Schedule {
      /// The faults of a run of `scenario` whose fault phase lasts `time_limit`.
      pub fn generate(scenario: Scenario, seed: u64, time_limit: Duration, mode: Mode) -> Self;
  }

  impl fmt::Display for PlannedFault { … } // one schedule.txt line, see below
  ```

  `Mode` is defined in `lib.rs` (`pub enum Mode { Normal, SelfTest }`), so `main.rs` and the always-built modules share it.
- `Scenario::kinds`: `Partitions` → `[PartitionHalves, IsolatePrimary, IsolateControl, Bridge]`; `Kills` → `[Kill]`; `Pauses` → `[Pause]`; `Clock` → `[ClockSkew, ClockStrobe]`; `Mixed` → all eight. With `Mode::SelfTest` every scenario → `[IsolatePrimary]`.
- The RNG: `let mut root = SimRng::new(seed); let mut rng = root.fork();` The workload (step 7) forks its ten client generators from the same `root` afterwards, in client order, so the schedule and the workload never share a stream.
- `generate`, exactly:

  ```text
  t = FIRST_FAULT
  loop:
      quiet  = 5_000 + rng.below(5_001)                ms
      length = if SelfTest { 15_000 + rng.below(5_001) } else { 5_000 + rng.below(15_001) } ms
      start = t + quiet; end = start + length
      if end > time_limit: stop
      kind = kinds[rng.index(kinds.len())]
      fault = resolve(kind, &mut rng)
      push PlannedFault { start, end, fault }; t = end
  ```

  `resolve`:
  - `PartitionHalves`: shuffle `Node::ALL` with Fisher–Yates (`for i in (1..5).rev() { j = rng.index(i + 1); swap(i, j) }`); the first two against the last three; `cut` is every pair `(a, b)` with `a` from the first side, `b` from the second, each pair ordered `min, max`, sorted.
  - `Bridge`: shuffle the same way; sides `[0, 1]` and `[3, 4]`; node `2` is the bridge and appears in no pair.
  - `IsolateControl`: `cut` is `(N1, n)` for `n` in `N2..=N5`.
  - `IsolatePrimary`: `database = rng.index(8)`.
  - `Kill` and `Pause`: `node = Node::ALL[rng.index(5)]`.
  - `ClockSkew` and `ClockStrobe`: `magnitude = 1 + rng.below(MAX_CLOCK_OFFSET_S)`; negative if `rng.chance(1, 2)`; skew uses the signed value, strobe its magnitude.
- A `schedule.txt` line is `{start_ms:>8}ms {end_ms:>8}ms {kind} {targets}`, with `kind` in kebab case (`partition-halves`, `isolate-primary`, `isolate-control`, `bridge`, `kill`, `pause`, `clock-skew`, `clock-strobe`) and `targets` as `n1,n4 | n2,n3,n5` for partitions (bridge: `n1,n4 | n2 | n3,n5`, the middle one the bridge), `chaos/db-3` for a planned isolation (the run appends `primary n4` once resolved, or `skipped: no primary`), `n3` for kills and pauses, and `n1 -42s` or `n1 ±42s` for clocks.
- `main.rs`'s `schedule` subcommand prints `Schedule::generate(…)`'s lines.
- Tests in `schedule.rs`:
  - `a_seed_gives_the_same_schedule`: two `generate(Mixed, 7, 600 s, Normal)` are equal; seed 8 differs.
  - `faults_stay_inside_the_window_and_never_overlap`: for seeds 0..200 and every scenario, every fault has `FIRST_FAULT <= start < end <= time_limit`, consecutive faults are at least 5 s apart, and lengths are within 5–20 s.
  - `each_scenario_uses_only_its_fault_kinds`: seeds 0..200.
  - `mixed_uses_every_kind`: the union of kinds over seeds 0..100 of `Mixed` is all eight.
  - `the_self_test_isolates_primaries_long_enough_to_fail_over`: `SelfTest` schedules hold only `IsolatePrimary` faults of 15–20 s, and at least 20 of them in 600 s.
  - `partitions_cut_only_across_their_sides`: every halves cut has 6 pairs with no pair inside a side; every bridge cut has 4 pairs and the bridge node in none; every control isolation has 4 pairs all containing `N1`.
  - `clock_offsets_stay_within_a_minute`: every offset is nonzero and within ±60.

### Step 6: the fault injector

Subject: `feat(chaos): inject partitions, kills, pauses, and clock faults`

- `crates/fleetring-chaos/src/inject.rs` (feature `harness`):

  ```rust
  /// Finds the node that holds a database's lease now, by the database's index; `None` if it
  /// has no primary. In a run it wraps the harness's own `data::Client::locate`, whose calls
  /// aren't in the history.
  pub type LocatePrimary =
      Arc<dyn Fn(usize) -> Pin<Box<dyn Future<Output = Option<Node>> + Send>> + Send + Sync>;

  /// Applies a schedule's faults to the running containers and records what it did.
  pub struct Injector {
      runner: Arc<dyn Runner>,       // `Docker` in a run; a recorder in tests (below)
      run_dir: PathBuf,
      locate: LocatePrimary,
  }
  // `Debug` by hand: prints `run_dir` and elides the closure.

  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct AppliedFault {
      pub started: Duration,         // since the workload's start
      pub ended: Duration,
      pub planned: PlannedFault,
      pub resolved: String,          // the targets as schedule.txt prints them
  }

  impl Injector {
      pub fn new(runner: Arc<dyn Runner>, run_dir: PathBuf, locate: LocatePrimary) -> Self;
      /// Runs every fault at its planned time after `t0` until `deadline`, ending the active
      /// fault at the deadline. Returns what it applied.
      pub async fn run(self, schedule: &Schedule, t0: Instant, deadline: Instant)
          -> Result<Vec<AppliedFault>, InjectError>;
      /// Undoes every fault on every node: flushes iptables, continues paused processes, starts
      /// stopped containers, and sets the clock offset back to `+0s`. Safe to call at any time.
      pub async fn heal_all(&self) -> Result<(), InjectError>;
  }
  ```

- Applying and ending each fault, with `c` a container and `ip` an address:
  - `Partition`: for each `(a, b)` in `cut`, `partition_args(a.container(), b.ip())` and `partition_args(b.container(), a.ip())`. End: `heal_args` on all five containers.
  - `IsolatePrimary`: `locate.locate(&database_name(index))`, retried every 200 ms for up to 2 s while `primary` is `None` or the call fails; then `Node::from_node_id(primary)` and partition it from the other four as above. If no primary was found, record `skipped: no primary` and do nothing until the fault's end.
  - `Kill`: `signal_args(c, Signal::Kill)`. End: `start_args(c)`. A container restart gets a fresh network namespace, so no `iptables` rule outlives it; kills and partitions never overlap.
  - `Pause`: `signal_args(c, Signal::Stop)`. End: `signal_args(c, Signal::Cont)`.
  - `ClockSkew`: write `faketime_offset(offset_s)` to `<run>/nodes/n1/faketime/offset` by writing `offset.tmp` and renaming it over `offset`. End: write `+0s`.
  - `ClockStrobe`: a loop that writes `+X`, waits `STROBE_PERIOD`, writes `-X`, waits, until the fault's end; then `+0s`.
- `heal_all` runs every `heal_args`, `signal_args(c, Cont)` and `start_args(c)` on every container ignoring errors (a running container's `start` and a running process's `CONT` are no-ops), and writes `+0s`.
- `InjectError`: `#[non_exhaustive]` with `Docker(#[from] DockerError)` (`"docker failed"`) and `Io { path: PathBuf, source: io::Error }` (`"can't write {path}"`).
- The runner trait, in `docker.rs`, so the injector runs without Docker in tests:

  ```rust
  pub trait Runner: fmt::Debug + Send + Sync + 'static {
      fn run(&self, args: Vec<String>) -> Pin<Box<dyn Future<Output = Result<String, DockerError>> + Send + '_>>;
  }
  ```

  implemented by `Docker` (through `run_async`) and, in the tests, by `Recording(Arc<Mutex<Vec<Vec<String>>>>)`, which records each argument list and answers `Ok(String::new())`. Tests (Tokio, `start_paused = true`, a `LocatePrimary` closure that returns a fixed answer):
  - `a_partition_cuts_then_heals_every_pair`: the recorded commands are the partition commands at `start`, then the ten heal commands at `end`.
  - `a_kill_restarts_the_container_at_the_end`.
  - `a_skew_writes_the_offset_then_zero`: the file holds `-42s` during the fault and `+0s` after.
  - `a_strobe_flips_every_half_second`: after 1.2 s of a strobe of 30 s the file has held `+30s`, `-30s`, `+30s` in that order.
  - `an_isolation_without_a_primary_is_skipped`: the locate function returns `None`; no command runs and `resolved` is `chaos/db-3 skipped: no primary`.

### Step 7: the workload

Subject: `feat(chaos): run a seeded workload and record its history`

- `crates/fleetring-chaos/src/workload.rs` (feature `harness`):

  ```rust
  pub const DATABASES: usize = 8;
  pub const CLIENTS: usize = 10;
  pub const WRITE_PERCENT: u64 = 70;
  pub const PAUSE_BETWEEN_OPERATIONS: Duration = Duration::from_millis(50);
  pub const OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
  pub const REPLICATION_FACTOR: u32 = 3;

  /// `chaos/db-<index>`.
  pub fn database_name(index: usize) -> DatabaseName;
  /// How database `index` is created: quorum for 0..6, all for 6 and 7; local for every
  /// database in the self-test.
  pub fn created_durability(index: usize, mode: Mode) -> DurabilityMode;
  /// What the checker holds database `index` to: quorum for 0..6 (and for every database in the
  /// self-test), all for 6 and 7.
  pub fn checked_guarantee(index: usize) -> Guarantee;

  /// The shared history, with the instant its times count from.
  #[derive(Debug, Clone)]
  pub struct Recorder { history: Arc<Mutex<History>>, t0: Instant }

  impl Recorder {
      pub fn new(t0: Instant) -> Self;
      /// Sends `operation` through `data`, recording it when sent and when it ends; an
      /// operation without an outcome within `OPERATION_TIMEOUT` ends indeterminate.
      pub async fn perform(&self, client: &str, data: &Client, operation: Operation) -> Outcome;
      pub fn history(&self) -> History; // a clone
  }

  /// Creates the eight databases and their writes tables, retrying each until it is
  /// acknowledged, for up to 60 s in all.
  pub async fn create_databases(recorder: &Recorder, data: &Client, mode: Mode)
      -> Result<(), WorkloadError>;

  /// Runs the ten clients until `deadline`.
  pub async fn run_clients(recorder: Recorder, network: Network, root: &mut SimRng, deadline: Instant);

  /// Reads every database once more, retrying each until it is acknowledged, for up to 60 s.
  pub async fn final_reads(recorder: &Recorder, data: &Client) -> Result<(), WorkloadError>;
  ```

- `Recorder::perform`: lock, `history.begin(client, t0.elapsed(), operation.clone())`, unlock; `tokio::time::timeout(OPERATION_TIMEOUT, fleetringd::sim::data_operation(data, &operation))`; `Ok(Some(outcome))` → that outcome; `Ok(None)` can't happen for the three kinds used (treat it as `Outcome::Rejected { code: Code::Internal, message: "not a data operation" }`); `Err(_)` → `Outcome::Indeterminate { code: None }`; lock, `history.complete(id, t0.elapsed(), outcome.clone())`.
- `create_databases`: client name `setup`, one `Client::new(Node::CONTROL.address(), network)`, operations `Operation::Create { database: database_name(i), replication_factor: 3, durability: created_durability(i, mode) }` in index order, each repeated every 500 ms until `Outcome::Acknowledged`.
- `run_clients`: client `k` in `0..10` is named `client-k`, gets `rng = root.fork()` in order, and its own `Client::new(Node::CONTROL.address(), network.clone())`. Its loop until `deadline`: `index = rng.index(8)`; if `rng.chance(70, 100)`, `Operation::Write { database: database_name(index), value: k as i64 * 1_000_000_000 + n }` with `n` counting from 1; else `Operation::Read { database: database_name(index), value: None }`; `perform`; `sleep(PAUSE_BETWEEN_OPERATIONS)`. Ten clients at one operation per 50 ms plus latency is about 200 operations a second.
- `final_reads`: client name `final`, `Operation::Read { value: None }` for each database, repeated every 500 ms until acknowledged.
- `WorkloadError`: `#[non_exhaustive]`, `NotCreated { database: DatabaseName }` (`"database {database} wasn't created within 60 s"`), `NotRead { database }` (`"database {database} couldn't be read within 60 s"`).
- The network the clients use: `Network::default().with_security(Security::Tls(Identity::load(tls/ca.crt, tls/client.chaos.crt, tls/client.chaos.key)?))`.
- Tests in `workload.rs`:
  - `self_tests_create_local_databases_but_check_them_as_quorum`: `created_durability(i, SelfTest) == Local` and `checked_guarantee(i).mode() == Quorum` for `i` in `0..6`.
  - `two_databases_acknowledge_on_every_replica`: `created_durability(6, Normal) == All`, likewise 7.
  - `values_are_unique_across_clients`: the value formula never collides for `k < 10`, `n < 10^9`.

### Step 8: the run, the checks, the verdict

Subject: `feat(chaos): run a whole fault-injection run and judge it with the history checker`

- `crates/fleetring-chaos/src/run.rs` (feature `harness`):

  ```rust
  #[derive(Debug, Clone)]
  pub struct RunOptions {
      pub scenario: Scenario,
      pub seed: u64,
      pub time_limit: Duration,
      pub mode: Mode,
      pub image: String,
      pub build: Build,
      pub out: PathBuf,
      pub teardown: Teardown,
  }

  #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
  pub struct Verdict {
      pub scenario: String,           // kebab case
      pub seed: u64,
      pub self_test: bool,            // a field, not a parameter
      pub verdict: VerdictKind,       // "valid" | "invalid" | "unknown"
      pub invariant: Option<String>,  // the Invariant's Display, when invalid
      pub detail: Option<String>,     // the violation's or the harness error's text
      pub operations: Counts,         // { ok, fail, info } over the history
      pub faults: usize,              // faults applied
      pub time_limit_s: u64,
  }

  pub enum VerdictKind { Valid, Invalid, Unknown }  // serde rename_all = "lowercase"

  /// Runs everything and returns the process exit code: 0, 1, or 2 (ADR-0036 point 6).
  pub async fn run(options: RunOptions) -> u8;
  ```

- `run`, in order (each failure before step 13 ends the run with exit code 2 and verdict `unknown`, after teardown):
  1. Refuse a host that isn't Linux: `fleetring-chaos run needs Linux with Docker; this host runs <std::env::consts::OS>`.
  2. `docker version --format {{.Server.Version}}` must succeed.
  3. Run directory `<out>/<scenario>-<seed>/`: remove it if present, create `tls/`, and for each node `nodes/nX/data/`, `nodes/nX/faketime/` (holding `offset` = `+0s`), and `nodes/nX/fleetringd.toml` from `render_config`. `user` is the directory's `uid:gid`.
  4. Unless `Build::Skip`, `build_args(image)` from the workspace root (`concat!(env!("CARGO_MANIFEST_DIR"), "/../..")`).
  5. Remove leftovers: `rm_args` for the five containers and `network_rm_args`, ignoring errors.
  6. `network_create_args`.
  7. Certificates (step 3's commands); remember `issued = Instant::now()`.
  8. `run_args` for each node, `n1` first.
  9. Ready: poll `Client::status()` every 500 ms until all five members are live and the view has a ring, for up to 60 s.
  10. `create_databases`.
  11. Wait until `issued + 55 s`, so the first fault (at `FIRST_FAULT` = 10 s) starts at least 65 s after the certificates' not-before (ADR-0036 point 4).
  12. `t0 = Instant::now()`, `deadline = t0 + time_limit`; spawn `run_clients` and `Injector::run` together and await both.
  13. `heal_all`; `sleep(30 s)`; `final_reads`.
  14. Convergence: for each database, for up to 60 s: `locate`; the primary's `verify(primary_address, db, None)` gives `p`; every other replica's `verify(address, db, p.as_ref())` must return the same `chain_hash`. Remember each database's final replica set from the last `locate`. A timeout isn't an error here: the checks below judge the copies.
  15. For each container, `logs_args` into `nodes/nX.log` (stdout then stderr); then `stop_args` for every container.
  16. Rows: for each database and each node, `read_replica_rows(<run>/nodes/nX/data, db)` into a `ReplicaRows` keyed by node name. History checks: `history::check(&history, &databases)` with `Replicated::new(checked_guarantee(i), rows)` for every database; then for each database `check::replicas_converged(db, rows restricted to its final replica set)`; then `check::control_log_replays(<run>/nodes/n1/data/control.db)`. The first violation decides.
  17. A run with fewer than 100 acknowledged writes is a harness failure (`the workload acknowledged only {n} writes`), exit 2, whatever the checks say.
  18. Write `history.txt` (`history.events()`, one `Display` per line), `schedule.txt` (the applied faults' lines, then any planned fault the deadline cut off, marked `not run`), and `verdict.json` (pretty JSON).
  19. Unless `Teardown::Keep`: `rm_args` for the containers and `network_rm_args`.
  20. Exit code: `Mode::Normal`: 0 valid, 1 invalid. `Mode::SelfTest`: 0 if invalid with the invariant `AcknowledgedWritesReplicated` or `AcknowledgedWritesRead` (printed as `self-test passed: the checker found …`), else 1 (`self-test failed: a run that loses acknowledged writes looked valid`).
- On exit codes other than 0, stderr ends with:

  ```text
  fleetring-chaos: <scenario> seed <seed>: <verdict and detail>
    run directory: <path>
    rerun: cargo run -p fleetring-chaos --features harness --release -- run --scenario <scenario> --seed <seed> --time-limit <limit>[ --self-test]
    the rerun replays the fault schedule and the workload's choices, not the processes' timing
  ```

- Ctrl-C during a run: `tokio::signal::ctrl_c` in a `select!` with the run; on it, `heal_all` and teardown, exit 130.
- Tests in `run.rs` (no Docker):
  - `a_lost_acknowledged_write_is_invalid`: build a `History` with `begin`/`complete`: a `Create` acknowledged as `Opened`, a `Write` of 7 acknowledged as `Written { acknowledged_by: 2, replicas: 3, .. }`; rows where no node holds 7; the judging function `judge(&history, rows, Mode::Normal) -> Verdict` returns `Invalid` with invariant `acknowledged writes are on a write quorum`, and `exit_code` 1.
  - `the_self_test_passes_only_on_a_lost_write`: the same history judged with `Mode::SelfTest` gives exit code 0; a history whose write is held by three nodes gives exit code 1.
  - `a_quiet_workload_is_a_harness_failure`: a valid history with 99 acknowledged writes gives `Unknown` and exit code 2.

  To make these possible, steps 16, 17, and 20 live in `pub fn judge(history: &History, databases: &BTreeMap<DatabaseName, Replicated>, final_sets: &BTreeMap<DatabaseName, Vec<String>>, control_log: Option<&Path>, mode: Mode) -> (Verdict, u8)`, which `run` calls.
- An ignored end-to-end test, `crates/fleetring-chaos/tests/docker.rs`: `#[ignore = "needs Linux and Docker; run with --ignored"]` `a_two_minute_mixed_run_is_valid`, which calls `run` with `time_limit = 120 s`, `out` a temporary directory, and asserts exit code 0. `#![cfg(feature = "harness")]` at the top.

### Step 9: the nightly workflow

Subject: `ci(chaos): run the fault-injection scenarios nightly`

- `.github/workflows/chaos.yml`:

  ```yaml
  # Nightly fault injection on real daemons (FR-065, ADR-0036). Never on pull requests: each
  # scenario runs for ten minutes on five containers. A failing run uploads its directory, with
  # the history, the fault schedule, the verdict, and every node's log.
  name: Chaos

  on:
    schedule:
      - cron: "53 3 * * *"
    workflow_dispatch:
      inputs:
        time-limit:
          description: "How long each scenario's fault phase runs"
          required: false
          default: "600s"
        seed:
          description: "A seed to run every scenario with; random if empty"
          required: false
          default: ""

  permissions:
    contents: read

  env:
    CARGO_TERM_COLOR: always
    CARGO_INCREMENTAL: 0

  jobs:
    image:
      name: image
      runs-on: ubuntu-24.04
      timeout-minutes: 40
      steps:
        - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        - uses: docker/setup-buildx-action@f87e5991a6d7451dcb8d9637bfbc97413f497069 # v4.4.1
        - uses: docker/build-push-action@c3c9e263c25d99ce0380d002d59b67737d91b0dc # v7.4.0
          with:
            context: .
            file: deploy/chaos/Dockerfile
            tags: fleetring-chaos:ci
            outputs: type=docker,dest=/tmp/fleetring-chaos.tar
        - uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2
          with:
            name: fleetring-chaos-image
            path: /tmp/fleetring-chaos.tar
            retention-days: 1

    run:
      name: chaos (${{ matrix.run }})
      needs: image
      runs-on: ubuntu-24.04
      timeout-minutes: 40
      strategy:
        fail-fast: false
        matrix:
          run: [partitions, kills, pauses, clock, mixed, self-test]
      env:
        TIME_LIMIT: ${{ github.event.inputs.time-limit || '600s' }}
        SEED: ${{ github.event.inputs.seed }}
      steps:
        - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        - uses: dtolnay/rust-toolchain@683b588c487a69d01f6c6ddf40f036ff682cc2c8 # master
          with:
            toolchain: stable
        - uses: Swatinem/rust-cache@d1527c8f67ef9af357290475d4af4b0fab8f12e0 # v2.9.3
        - uses: actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333 # v8.0.2
          with:
            name: fleetring-chaos-image
            path: /tmp
        - run: docker load --input /tmp/fleetring-chaos.tar
        - run: cargo build --release -p fleetring-chaos --features harness
        - name: run
          run: |
            if [ "${{ matrix.run }}" = self-test ]; then
              args="--scenario partitions --self-test"
            else
              args="--scenario ${{ matrix.run }}"
            fi
            if [ -n "$SEED" ]; then args="$args --seed $SEED"; fi
            # shellcheck disable=SC2086
            target/release/fleetring-chaos run $args --time-limit "$TIME_LIMIT" \
              --image fleetring-chaos:ci --skip-build 2> >(tee stderr.log >&2)
        - name: summary
          if: failure()
          run: |
            {
              echo "## Chaos run failed: ${{ matrix.run }}"
              echo
              echo '```'
              tail -n 6 stderr.log
              echo '```'
            } >> "$GITHUB_STEP_SUMMARY"
        - uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2
          if: failure()
          with:
            name: chaos-${{ matrix.run }}
            path: target/chaos/
  ```

  Run `actionlint` on it (CONTRIBUTING.md). The pins are the ones `container.yml` and `simulation-sweep.yml` already use.

### Step 10: documentation

Subject: `docs(chaos): document the fault-injection harness`

- `crates/fleetring-chaos/README.md`: what it tests and why (ADR-0036 in three sentences); requirements (Linux, Docker 24 or later, about 4 GiB free disk for the image build); how to run one scenario, the self-test, and `schedule`; the layout of a run directory; the exit codes; how to read `history.txt` (`ok`, `fail`, `info`) and `schedule.txt`; that a seed replays the schedule, not the timing; how to turn a finding into a simulation regression test (reproduce it with `sim::Fleet` faults, add the seed to the fixed list).
- `CONTRIBUTING.md`: under "Before you push", the MSRV lines from step 2; a short "Fault injection" paragraph saying the nightly `Chaos` workflow runs on `main`, how to run it locally, and that a failing nightly run gets an issue with its seed and run directory attached, as a flaky test does.
- `docs/roadmap.md`: E1's Jepsen-style bullet links FR-065 and says "an in-repo harness, `fleetring-chaos` (ADR-0036)"; E9's chaos bullet links FR-065.
- `docs/lexicon.md`, two new entries in alphabetical order:

  ```markdown
  ### Fault

  A failure caused on purpose by the simulation harness or the fault-injection harness: a network partition, a crash or kill, a paused process, added latency or loss, or a skewed clock. A *fault schedule* is the seeded list of faults one run injects, each with when it starts and ends (ADR-0020, ADR-0036).

  - **Applies to:** daemon (tests and harnesses).
  - **Code:** `sim::Fault`, `fleetring_chaos::schedule::Fault`, `Schedule`. **CLI:** `fleetring-chaos run --scenario <name>`; `schedule.txt` in a run directory.
  - **Not:** nemesis, disruption, failure (for one caused on purpose), chaos (except in the harness's name).

  ### History

  Every operation clients sent during a run, each with when it was sent, when it ended, and how: `ok` (acknowledged), `fail` (refused, so it had no effect), or `info` (unknown, so it may or may not have taken effect). The history checker (FR-036) judges a history against what each database promises.

  - **Applies to:** daemon (tests and harnesses).
  - **Code:** `sim::History`, `sim::history::check`, `OutcomeClass`. **CLI:** `history.txt` in a fault-injection run's directory.
  - **Not:** log (a control log and a database's log are other things), trace (the simulation's `Trace` is its event order), journal, record (one operation in a history is a record).
  ```

- This plan's Progress section, and ADR-0036's status once accepted.

### Step 11 (gated on FR-064): control-group faults

Subject: `feat(chaos): fault the control group`

Do this step only once FR-064 (#67) has merged; FR-065 is complete without it.

- `node.rs`: `n1`, `n2`, and `n3` run node role `both` and form the control group (bootstrap `n1` with `[control] bootstrap = "new"`, `n2` and `n3` with `"join"`, then `fleetringd control add` and `control set-voters` through the harness's client, as FR-064's plan specifies); `n4` and `n5` stay data nodes. Data nodes' configurations list `[control] addresses = ["10.77.0.11:7400", "10.77.0.12:7400", "10.77.0.13:7400"]` instead of `address`. The clients' `Client::new` takes the same three addresses as FR-064's client accepts them.
- `schedule.rs`: three kinds, added to `Partitions` (the first) and `Mixed` (all three): `IsolateControlLeader` (the leader, found with FR-064's `DescribeControlGroup`, cut from every node), `KillControlMajority` (two of the three control nodes, chosen with `rng`, killed and restarted together), and `ControlClockSkew` (each control node a different offset within ±60 s, chosen with `rng`). `IsolateControl` then means isolating `n1`, which may or may not be the leader.
- `run.rs`: `control_log_replays` runs on each control node's `control.db`, and FR-064's `ControlLogsAgree` check runs across the three.
- Tests: the schedule tests of step 5 extended to the new kinds; `a_control_majority_kill_names_two_distinct_control_nodes`.

## CI placement

- **Every pull request:** the crate's unit tests in the normal `test` jobs. On Linux's `--all-features` legs they include the `harness` modules (schedules, the injector with a recording runner, the workload's pure parts, `judge`); on macOS, Windows, and Linux's default-feature and no-default-feature legs they cover `config`, `docker`, `duration`, and `node`. Clippy and docs build it with every feature. No job on a pull request starts Docker.
- **Nightly and on dispatch:** `.github/workflows/chaos.yml`, five scenarios and the self-test, 600 s each, in parallel after one image build. About 15 minutes of wall time per leg: the fault phase, about 2 minutes of start-up and certificate waiting, and up to 3 minutes of healing, convergence, and checking.
- **Locally:** `cargo run -p fleetring-chaos --features harness --release -- run --scenario mixed` on Linux with Docker.

## Documentation

Covered by step 10: the harness README, `CONTRIBUTING.md`, `docs/roadmap.md`, and the lexicon. `docs/adr/README.md` lists ADR-0036 with the plan's branch; its status moves to Accepted when the owner accepts it. `SECURITY.md` doesn't change: the harness adds no surface to the daemon.

## Owner decisions (2026-11-03)

The open questions this plan raised were decided as recommended:

- Failing nightly runs don't file issues automatically: the job summary and artifacts are enough, and issues are filed by hand.
- Clock skew is injected on the control node only in v1; data-node and control-group skew come with FR-064.
- If `libfaketime` doesn't move Rust's clock in containers, fall back to `unshare --time`, recorded as an amendment to ADR-0036, not a clock hook in the daemon.

## Open risks

- **`libfaketime` and Rust's clock calls.** Rust's `SystemTime::now` calls glibc's `clock_gettime`, which `libfaketime` intercepts when preloaded, but glibc may answer through the vDSO without the symbol lookup in some builds. Step 6's skew must be confirmed working on the first real run: the control node's logged command stamps (`RUST_LOG=fleetringd::control=debug` in a local run) must move by the offset. If they don't, the fallback is to skew the container's clock namespace with `unshare --time` from the harness, which needs no daemon change either; record the switch in ADR-0036 as an amendment.
- **`FAKETIME_NO_CACHE=1` costs a file read on every wall-clock call.** The control node reads the clock once per command, which is cheap; if a profile shows otherwise, set `FAKETIME_CACHE_DURATION=1` instead and accept up to a second of delay before an offset applies.
- **`iptables` backends.** Bookworm's `iptables` is the `nf_tables` variant, which needs the host kernel's `nf_tables` modules; GitHub's Ubuntu runners have them. A host with only legacy `iptables` needs `iptables-legacy` in the image; the harness reports the `docker exec` failure with its stderr, so this shows as exit code 2, not as a pass.
- **Real timing doesn't replay.** A seed reproduces the schedule and the workload's choices only. A nightly violation that won't recur must be diagnosed from its run directory, then reproduced in the simulation.
- **A pass proves only what was exercised.** Ten minutes per scenario hits a few dozen faults. The nightly cadence accumulates coverage; widening the workload (more databases, durability changes, drains during faults) is follow-up work.
- **The containers' clients aren't partitioned.** Clients on the host always reach every node, which is the harshest case for a cut-off primary but never tests a client that can reach only a minority. A client container per side is a possible extension.
- **Bind-mounted files and ownership.** Containers run as the run directory's owner, so everything they write stays readable and removable by the harness. A host where Docker remaps user namespaces breaks that; the README says to run without user-namespace remapping.
- **Build time.** The image's release build takes several minutes on a clean runner; the nightly job builds it once and shares it. If it grows past the job's budget, cache the build stage with `cache-from: type=gha`.

## Progress

- Not started.
