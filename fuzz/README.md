# Fuzzing `fleetring`

Three libFuzzer targets check that no input reaching `fleetring` from outside can cause a panic, and that every ring keeps its placement invariants (FR-017).

| Target | Input | Checks |
|---|---|---|
| `spec_json` | arbitrary bytes, parsed as a JSON `RingSpec` and as a `CellsSpec` | `Ring::from_spec` and `Cells::from_spec` never panic; every spec that loads round-trips to the same fingerprint |
| `builder` | arbitrary memberships and keys | `build()` returns `Ok` or `Err`, never panics; replica sets are distinct, sized `min(R, N)`, and a prefix of the preference list; filters exclude rejected nodes |
| `lookup` | an arbitrary ring, key, liveness bitmap, and loads | filters exclude down nodes; bounded placements are full, distinct, and never over capacity unless flagged as saturated |

## Run

Fuzzing needs nightly Rust and [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz). Run from the repository root:

```sh
cargo install cargo-fuzz
cargo +nightly fuzz run spec_json fuzz/corpus/spec_json fuzz/seeds/spec_json -- -max_total_time=300
cargo +nightly fuzz run builder -- -max_total_time=300
cargo +nightly fuzz run lookup -- -max_total_time=300
```

`fuzz/seeds/` holds committed seed inputs. `fuzz/corpus/` is where the fuzzer saves what it discovers; it is gitignored.

The `Fuzz` GitHub workflow runs each target for five minutes every night, and on demand.

## Triage

1. A crash writes its input to `fuzz/artifacts/<target>/`. In CI, download the `fuzz-<target>-artifacts` artifact.
2. Reproduce it with `cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<file>`.
3. Shrink it with `cargo +nightly fuzz tmin <target> fuzz/artifacts/<target>/<file>`.
4. Turn the minimized input into a regular unit or integration test, and only then fix the bug. The fuzzer stays the safety net, not the regression suite.
