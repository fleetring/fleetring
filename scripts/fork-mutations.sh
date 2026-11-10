#!/bin/sh
# FR-087's mutation check: runs the fork simulation matrix once per deliberate bug and reports
# which property and scenario failed for each (ADR-0046 point 12). A mutation that makes no
# property fail means the matrix has a gap.
#
# Each mutation is compiled in through FLEETRING_MUTATION, which only a debug build accepts
# (fleetring_control::mutation). Usage:
#
#   scripts/fork-mutations.sh                # every mutation
#   scripts/fork-mutations.sh pin_at_tip     # the named ones
#
# Logs go to target/fork-mutations/<mutation>.log. Exits non-zero if any mutation survives.
set -u

all="pin_at_tip complete_on_one_holder non_holder_first_lease skip_source_incarnation \
skip_target_identity_apply skip_target_identity_binding unconditional_reset serve_at_seeded \
disk_without_reservations"
mutations=${*:-$all}
logs=target/fork-mutations
mkdir -p "$logs"
export RUSTFLAGS="--cfg tokio_unstable"
survived=""

for mutation in $mutations; do
  log="$logs/$mutation.log"
  FLEETRING_MUTATION="$mutation" cargo test -p fleetringd --features simulation,fault-hooks \
    --test sim -- fork:: > "$log" 2>&1
  failures=$(grep -E "^test fork::.* FAILED$" "$log" | sed -e 's/^test //' -e 's/ \.\.\. FAILED$//')
  if [ -z "$failures" ]; then
    echo "$mutation: SURVIVED (no property failed)"
    survived="$survived $mutation"
    continue
  fi
  echo "$mutation:"
  for test in $failures; do
    # The first failing point's property, from the panic message.
    property=$(grep -A 3 "^---- $test stdout" "$log" | grep -oE "invariant violated \([^)]*\)|not done within [0-9]+s|panicked" | head -n 1)
    point=$(grep -A 3 "^---- $test stdout" "$log" | grep -oE "at [A-Za-z]+:" | head -n 1)
    echo "  $test $point ${property:-failed}"
  done
done

if [ -n "$survived" ]; then
  echo "mutations no property caught:$survived"
  exit 1
fi
