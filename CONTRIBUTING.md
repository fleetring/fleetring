# Contributing

Thanks for helping. A few rules keep placement identical on every platform.

## Setup

```sh
git config core.hooksPath .githooks   # checks commit messages
```

## Before you open a pull request

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

## Rules for `crates/fleetring`

- **Placement is bit-identical everywhere.** No floating point, no `std::hash` hashers, `HashMap`, or `HashSet`. Hash inputs are explicit little-endian integers, and products that could overflow are computed in `u128`.
- **The crate is pure:** no I/O, clocks, randomness, or async runtime.
- **Placement output is a wire format.** [`docs/spec/placement-v1.md`](docs/spec/placement-v1.md) is normative. Changing which nodes a key maps to needs a new spec version, and the golden vectors are append-only.
- **Lookups don't allocate** for up to 8 replicas. `tests/alloc_free.rs` checks it.
- **Library hygiene:** no `unwrap()` outside tests, no `bool` parameters, and `#[expect(lint, reason = "…")]` instead of a bare `allow`.

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): summary`, for example `feat(ring): add bounded loads`. Add a `Refs: FR-NNN` trailer when the change belongs to a ticket.

## Design changes

Decisions are recorded in [`docs/adr/`](docs/adr/README.md). Propose a change as a new ADR rather than editing an accepted one. Planned work lives in [`docs/tickets/`](docs/tickets/README.md).
