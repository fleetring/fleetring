# Contributing

Thanks for helping. This page says how a change gets from your branch onto `main`, and the rules that keep placement identical on every platform and the daemon safe. It applies to every contributor and every tool they use.

## Read first

- **[`docs/lexicon.md`](docs/lexicon.md)**: the names for everything. Code, docs, CLI output, and tickets use its words. A new concept gets an entry there before it gets a type or a flag.
- **[`docs/cli.md`](docs/cli.md)**: the CLI style guide, for any change to a `fleetring` command, flag, or output.
- **[`docs/adr/`](docs/adr/README.md)**: the decisions behind the code you are changing. Read the ones the index lists for your area.
- **[GitHub Issues](https://github.com/fleetring/fleetring/issues) and the FleetRing roadmap project**: planned work and its acceptance criteria. [`docs/tickets/`](docs/tickets/README.md) archives the finished tickets and holds the definition of done, and [`docs/plans/`](docs/plans/) holds implementation plans.

## Setup

```sh
git config core.hooksPath .githooks   # checks commit messages
```

Never bypass the hooks (`--no-verify`). If a hook is wrong, fix the hook in its own commit.

## Before you push

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo +1.88 check --workspace --exclude fleetringd --exclude fleetring-client --exclude fleetring-wal-sqlite-tests --all-features --all-targets
cargo +1.89 check -p fleetringd -p fleetring-client -p fleetring-wal-sqlite-tests --all-features --all-targets
```

For workflow changes, run [`actionlint`](https://github.com/rhysd/actionlint) too.

## How changes land

`main` is protected by a ruleset: every required check must pass, and the branch must be up to date with `main` before it merges. Merges are **rebase and merge only**, so each commit of a pull request lands on `main` as it is.

- **Every merge makes every other open pull request out of date,** and each one must update and run CI again before it can merge. So:
  - **Batch related changes into one pull request,** as separate commits, rather than opening several small ones.
  - **Push once** when the branch is ready, not after every commit: each push runs the full matrix.
- **Update an out-of-date branch with "Update with rebase"** (the arrow beside the button), never the default "Update branch", which adds a merge commit. Or rebase locally onto `origin/main` and force-push with `--force-with-lease`.
- **Each commit stands on its own:** it builds, passes tests, and has its own Conventional Commit message, since it lands on `main` unchanged.
- **`main` runs only the job that keeps the build cache warm.** Pull requests and merge groups run every check; a push to `main` doesn't run them again, because the tree a merge lands is the tree its pull request's CI already tested. The comment at the top of `.github/workflows/ci.yml` explains it. After a Rust toolchain or `Cargo.lock` change, run CI by hand on `main` (Actions → CI → Run workflow) to refresh every job's cache.

## When CI fails

- **Re-run only the failed jobs, and only for a known flake** that has an issue. Otherwise, find the cause: a failure you don't understand is a bug until shown otherwise.
- **A flaky test gets an issue, never just a re-run.** Record the job, the seed or the log, and how often it fails, as [FR-051](https://github.com/fleetring/fleetring/issues/54) does. Label it `flaky-test`. Statistical and simulation tests run on fixed corpora and seeds so they can't flake; one that does has a real cause.

## Dependabot

Dependabot opens one grouped pull request a week for GitHub Actions updates (`.github/dependabot.yml`).

- **If it touches `.github/workflows/release.yml` or `.github/workflows/container.yml`, run both by hand before merging.** Run Actions → Release → Run workflow and Actions → Container → Run workflow on the Dependabot branch. Pull request CI never runs those workflows, so a broken pin would otherwise surface only on the next release tag. A manual run builds everything and publishes nothing.
- Check that each new pin is a full commit SHA with the release tag in a comment, as the existing ones are.

## Security-sensitive changes

Changes to authentication and authorization (`crates/fleetringd/src/auth`, `crates/fleetringd/src/tls`), replication and failover (`crates/fleetringd/src/replication`, leases, epochs, fencing), engine adapters (`crates/fleetringd/src/engine`, `crates/fleetring-wal`), the protocol (`crates/fleetring-proto`), and the release and container workflows **get an independent review before merging**: a review by someone other than the author, who reads the change and its tests against the ADRs it touches. Say in the pull request which ADRs apply and what the tests show. [`SECURITY.md`](SECURITY.md)'s threat model is updated in the same pull request when a change moves what an actor can do.

## Rules for `crates/fleetring`

- **Placement is bit-identical everywhere.** No floating point, no `std::hash` hashers, `HashMap`, or `HashSet`. Hash inputs are explicit little-endian integers, and products that could overflow are computed in `u128`.
- **The crate is pure:** no I/O, clocks, randomness, or async runtime. Docs say "key", never "database".
- **Placement output is a wire format.** [`docs/spec/placement-v1.md`](docs/spec/placement-v1.md) is normative. Changing which nodes a key maps to needs a new spec version, and the golden vectors are append-only.
- **Lookups don't allocate** for up to 8 replicas. `tests/alloc_free.rs` checks it.
- **Library hygiene:** no `unwrap()` outside tests, no `bool` parameters, and `#[expect(lint, reason = "…")]` instead of a bare `allow`.

## Rules for `fleetringd`

- **Engine-neutral above the adapter.** Leases, quorums, control state, the protocol, and migration see only opaque positions and payloads; SQLite's WAL details stay in the SQLite adapter and `fleetring-wal` (ADR-0016).
- **Safety never depends on clocks:** epochs fence, and W + Q > R (ADR-0018).
- **All control changes go through `fleetring-control`'s pure `apply`,** with time inside the command (ADR-0017), and the replay test passes.
- **Simulation first:** daemon code reads time only from `tokio::time` and randomness only from an injected RNG (ADR-0020).

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): summary`, for example `feat(ring): add bounded loads`. The hook and CI check the first line. End the message with a trailer naming the issue: `Refs: FR-NNN` (the number in the issue's title; archived tickets keep theirs) or `Refs: #N`, or several, comma-separated. The hook and CI accept both forms. Put `Fixes #N` in the pull request body to close the issue when it merges.

## Design changes and issues

- **Decisions are ADRs** in [`docs/adr/`](docs/adr/README.md). Propose a change as a new ADR, with status `Proposed`, and add it to the index. Never rewrite an accepted decision; supersede or amend it with a new one.
- **Work is tracked in [GitHub Issues](https://github.com/fleetring/fleetring/issues) and the FleetRing roadmap project.** Each issue carries a priority label (`P0`, `P1`, `P2`), a size label (`size:S`, `size:M`, `size:L`), and a milestone, and its title keeps its `FR-NNN` number. Record progress as a comment on the issue, and move it on the project board. An issue is done when its acceptance criteria are checked and the [definition of done](docs/tickets/README.md#definition-of-done) holds. A larger issue links an implementation plan in [`docs/plans/`](docs/plans/).
- **Dependencies** between issues are GitHub's "blocked by" relations, so the project shows what is ready.
- **A pull request that adds or ships a capability updates [`docs/capabilities.json`](docs/capabilities.json)** in the same pull request: a `planned` entry once it is ticketed, then `built`, with `since` set to the release it ships in. [`docs/capabilities.schema.json`](docs/capabilities.schema.json) says what each field holds. fleetring.dev builds its statuses and roadmap from this file and rebuilds when it changes (`.github/workflows/site.yml`).
