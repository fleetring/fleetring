#!/bin/sh
# Writes the control-log fixtures with fleetringd 0.9.0's own code (FR-064, ADR-0040 points 5
# and 8).
#
#   scripts/write-control-fixtures.sh          # the format-1 fixture and fields-0.9.0.txt
#   scripts/write-control-fixtures.sh stray    # the format-2 file 0.9.0 opened
#
# It adds a git worktree at `fleetringd-v0.9.0` in a temporary directory, copies the writers
# from `crates/fleetringd/tests/fixtures/control-writers/` into it as test targets, runs them
# there, copies what they write into this tree, and removes the worktree.
#
# - With no argument, `writer_0_9_0.rs` writes `control.db` and `commands.json` into
#   `crates/fleetringd/tests/fixtures/control-0.9.0/`, and `fields_0_9_0.rs` writes
#   `crates/fleetring-control/tests/it/fixtures/fields-0.9.0.txt` from those commands.
# - With `stray`, `open_format2_0_9_0.rs` runs 0.9.0's `CommandLog::open` on a copy of
#   `crates/fleetringd/tests/fixtures/control-0.9.0-migrated/control.db` and saves the result,
#   with the empty `commands` table 0.9.0 leaves behind, in
#   `crates/fleetringd/tests/fixtures/control-format2-opened-by-0.9.0/`.
#
# Each is run once, and its output is committed; the fixtures are never regenerated. Set
# CARGO_TARGET_DIR to reuse a build between runs.

set -eu

say() {
    printf 'write-control-fixtures.sh: %s\n' "$*" >&2
}

tag=fleetringd-v0.9.0
root=$(git rev-parse --show-toplevel)
writers="$root/crates/fleetringd/tests/fixtures/control-writers"
mode="${1:-fixture}"
case "$mode" in
fixture | stray) ;;
*)
    say "usage: write-control-fixtures.sh [stray]"
    exit 2
    ;;
esac

tmp=$(mktemp -d)
tree="$tmp/tree"
out="$tmp/out"
mkdir -p "$out"
cleanup() {
    git -C "$root" worktree remove --force "$tree" 2>/dev/null || true
    rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

say "adding a worktree at $tag"
git -C "$root" worktree add --detach --quiet "$tree" "$tag"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$tmp/target}"

if [ "$mode" = fixture ]; then
    cp "$writers/writer_0_9_0.rs" "$tree/crates/fleetringd/tests/control_fixture_writer.rs"
    cp "$writers/fields_0_9_0.rs" "$tree/crates/fleetring-control/tests/fields_writer.rs"
    say "writing the format-1 fixture with $tag's code"
    (cd "$tree" && FIXTURE_OUT="$out" cargo test --quiet -p fleetringd \
        --test control_fixture_writer -- --ignored --exact write)
    say "writing fields-0.9.0.txt with $tag's code"
    (cd "$tree" && FIXTURE_OUT="$out" FIXTURE_COMMANDS="$out/commands.json" cargo test --quiet \
        -p fleetring-control --test fields_writer -- --ignored --exact write)
    fixture="$root/crates/fleetringd/tests/fixtures/control-0.9.0"
    mkdir -p "$fixture"
    cp "$out/control.db" "$out/commands.json" "$fixture/"
    cp "$out/fields-0.9.0.txt" "$root/crates/fleetring-control/tests/it/fixtures/"
    say "wrote $fixture and fields-0.9.0.txt"
else
    migrated="$root/crates/fleetringd/tests/fixtures/control-0.9.0-migrated/control.db"
    [ -f "$migrated" ] || {
        say "error: $migrated is missing; run the ignored test that migrates the 0.9.0 fixture"
        exit 1
    }
    cp "$writers/open_format2_0_9_0.rs" "$tree/crates/fleetringd/tests/control_fixture_stray.rs"
    cp "$migrated" "$out/control.db"
    say "opening the migrated fixture with $tag's CommandLog"
    (cd "$tree" && FIXTURE_OUT="$out" cargo test --quiet -p fleetringd \
        --test control_fixture_stray -- --ignored --exact open)
    fixture="$root/crates/fleetringd/tests/fixtures/control-format2-opened-by-0.9.0"
    mkdir -p "$fixture"
    cp "$out/control.db" "$fixture/"
    say "wrote $fixture"
fi
