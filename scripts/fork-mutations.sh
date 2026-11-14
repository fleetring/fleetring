#!/bin/sh
# FR-087's mutation check: runs the fork simulation matrix, and FR-102's hand-over scenarios
# (`membership_lag::`), once per deliberate bug and reports
# which property and scenario failed for each (ADR-0046 point 12). A mutation counts as caught
# only when the history checker reports a property violation ("invariant violated (...)"); a
# scenario that fails otherwise, by a timeout or an outcome it didn't expect, is listed but
# catches nothing. A mutation that makes no property fail means the matrix has a gap.
#
# Each mutation is compiled in through FLEETRING_MUTATION, which only a build with debug
# assertions accepts (fleetring_control::mutation). Usage:
#
#   scripts/fork-mutations.sh                # every mutation
#   scripts/fork-mutations.sh pin_at_tip     # the named ones
#
# Logs go to target/fork-mutations/<mutation>.log. Exits non-zero if any mutation survives.
# With REPORT_ONLY=1 it reports from the logs of an earlier run instead of running again.
set -u

all="pin_at_tip complete_on_one_holder non_holder_first_lease skip_source_incarnation \
skip_target_identity_apply skip_target_identity_binding unconditional_reset serve_at_seeded \
disk_without_reservations move_seeded_replica_set reset_committed_on_one \
follower_reports_reset_early move_kept_replica_set count_placement_alone"
mutations=${*:-$all}
logs=target/fork-mutations
mkdir -p "$logs"
export RUSTFLAGS="--cfg tokio_unstable"
survived=""

for mutation in $mutations; do
  log="$logs/$mutation.log"
  if [ "${REPORT_ONLY:-0}" != 1 ]; then
    FLEETRING_MUTATION="$mutation" cargo test -p fleetringd --features simulation,fault-hooks \
      --test sim -- fork:: membership_lag:: > "$log" 2>&1
  fi
  failures=$(grep -E "^test (fork|membership_lag)::.* FAILED$" "$log" | sed -e 's/^test //' -e 's/ \.\.\. FAILED$//')
  report=""
  for test in $failures; do
    # Each failing point, and the property it violated, from the panic message.
    lines=$(sed -n "/^---- $test stdout/,/rerun it alone/p" "$log" \
      | grep -E "^(seed [0-9]+ failed: )?at [A-Za-z]+:" \
      | sed -E 's/^seed [0-9]+ failed: //')
    if [ -z "$lines" ]; then
      lines=$(sed -n "/^---- $test stdout/,/^$/p" "$log" | grep -E "panicked|Error" | head -n 1)
    fi
    report="$report$(printf '%s\n' "$lines" | while IFS= read -r line; do
      [ -n "$line" ] || continue
      point=$(printf '%s' "$line" | grep -oE "^at [A-Za-z]+" || true)
      property=$(printf '%s' "$line" | grep -oE "invariant violated \(([^()]|\([a-z]\))*\)" | head -n 1)
      if [ -n "$property" ]; then
        echo "  caught: $test $point: $property"
      else
        echo "  failed, no property: $test $point: $(printf '%s' "$line" | cut -c1-140)"
      fi
    done)
"
  done
  if printf '%s' "$report" | grep -q "^  caught:"; then
    echo "$mutation:"
  else
    echo "$mutation: SURVIVED (no property violation reported)"
    survived="$survived $mutation"
  fi
  printf '%s' "$report" | grep -v '^$' || true
done

if [ -n "$survived" ]; then
  echo "mutations no property caught:$survived"
  exit 1
fi
