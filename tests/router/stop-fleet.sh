#!/usr/bin/env bash
# Stops what tests/router/start-fleet.sh started under a work directory.
#
#   tests/router/stop-fleet.sh <work directory>
set -uo pipefail

pids="$1/pids"
[ -f "$pids" ] || exit 0
while read -r pid; do
  kill "$pid" 2> /dev/null || true
done < "$pids"
for _ in $(seq 40); do
  alive=no
  while read -r pid; do
    if kill -0 "$pid" 2> /dev/null; then
      alive=yes
    fi
  done < "$pids"
  [ "$alive" = no ] && exit 0
  sleep 0.5
done
while read -r pid; do
  kill -9 "$pid" 2> /dev/null || true
done < "$pids"
