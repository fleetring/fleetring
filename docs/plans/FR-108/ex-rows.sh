#!/bin/bash
# The acceptance rows on one driver: B's positive controls, A's own work, the hostile agent,
# replay from outside, the router's own scope check, the containers' configuration, and the
# supervisor's log of A's denials. Writes results/rows-<driver>.txt.
#
#   DRIVER=docker|vm GW=<gateway name> bash ex-rows.sh
set -u
. "$(dirname "$0")/lib.sh"
: "${DRIVER:?}" "${GW:?}"
OUT="$WORK/raw/rows-$DRIVER.txt"
osh openshell gateway select "$GW" > /dev/null
for s in agent-a agent-b; do
  for f in own-work.sh hostile-agent.sh probe-loop.sh http-layer.sh exhaust.sh; do
    osh openshell sandbox upload "$s" "$HERE/$f" /sandbox/ > /dev/null 2>&1
  done
done
exec > "$OUT" 2>&1

step "versions and clocks"
osh openshell --version; osh openshell-gateway --version; docker version --format 'docker {{.Server.Version}}'
echo "mac  $(now)"; ax agent-a 'echo "sandbox A $(date -u +%s.%N | cut -c1-14); kernel $(uname -r)"'

step "B's positive controls, inside B: its canaries exist and its socket answers"
ax agent-b 'id; ls -la /sandbox/work/.fleetring/agents/b /sandbox/work/b-local.sqlite; python3 -c "
import socket; s = socket.socket(socket.AF_UNIX); s.connect(\"/tmp/b-mux.sock\"); print(\"B connects to its own socket:\", s.recv(64))"'
B=$(ax agent-b 'echo $FLEETRING_TOKEN' | tail -1)
A=$(ax agent-a 'echo $FLEETRING_TOKEN' | tail -1)
echo "B's placeholder: $B"

step "own-work.sh in A"
ax agent-a "sh /sandbox/own-work.sh $ROUTER_IP:8443"

step "hostile-agent.sh in A"
ax agent-a "sh /sandbox/hostile-agent.sh $ROUTER_IP '$B'"

step "replay: A's and B's placeholders sent to the router from outside any sandbox (a container on the host)"
for p in "$A" "$B"; do
  docker run --rm -v "$DEV/tls/ca.crt:/ca.crt:ro" alpine:3 sh -c "apk add -q curl > /dev/null 2>&1; curl -sS --cacert /ca.crt -H 'Authorization: Bearer $p' -H 'content-type: application/json' https://$ROUTER_IP:8443/db/agent-a-memory/v3/pipeline -d '{\"requests\":[{\"type\":\"execute\",\"stmt\":{\"sql\":\"SELECT body FROM notes\"}}]}' -w '\nHTTP %{http_code}\n'"
done

step "the router alone: A's rule widened to POST /db/**, A's token at B's database"
base_policy agent-a "$WORK/run/base-a.yaml"
python3 -I "$HERE/policy_edit.py" "$WORK/run/base-a.yaml" "$WORK/run/wide-a.yaml" widen
grep -A3 'rules:' "$WORK/run/wide-a.yaml"
set_policy agent-a "$WORK/run/wide-a.yaml"
ax agent-a "for db in agent-b-memory agent-a-memory; do echo \"\$db:\"; curl -sS -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H 'content-type: application/json' https://$ROUTER_IP:8443/db/\$db/v3/pipeline -d '{\"requests\":[{\"type\":\"execute\",\"stmt\":{\"sql\":\"SELECT count(*) FROM sqlite_master\"}},{\"type\":\"close\"}]}' -w '\nHTTP %{http_code}\n'; done"
set_policy agent-a "$WORK/run/base-a.yaml"

if [ "$DRIVER" = docker ]; then
  step "docker inspect: A's workload and supervisor containers"
  for c in $(docker ps --format '{{.Names}}' | grep -E '^openshell-default--agent-a-'); do
    echo "-- $c"
    docker inspect "$c" | python3 -I -c '
import json, sys
c = json.load(sys.stdin)[0]; h = c["HostConfig"]
for m in c["Mounts"]:
    print(m["Type"], m.get("Name") or m.get("Source"), "->", m["Destination"], "rw" if m["RW"] else "ro")
print("network", h["NetworkMode"], "user", c["Config"]["User"], "capadd", h["CapAdd"], "capdrop", h["CapDrop"],
      "security", h["SecurityOpt"], "pids", h["PidsLimit"], "memory", h["Memory"], "nanocpus", h["NanoCpus"],
      "privileged", h["Privileged"], "readonlyroot", h["ReadonlyRootfs"])'
  done
  step "the sandbox protocol channel, seen from the host (operator view, uid 0 in A's workload container, no capabilities)"
  W=$(docker ps --format '{{.Names}}' | grep -E '^openshell-default--agent-a-[0-9a-f-]+$')
  docker exec -u 0 "$W" sh -c 'grep -E "Path|/" /proc/net/unix; ls -la /.openshell/channel /.openshell/channel/control 2>&1'
fi

step "the supervisor's log for A: denials and failures during this run"
osh openshell logs agent-a --since 15m --source sandbox 2>&1 | grep -E "DENIED|FAIL|mismatch|rejected|credential" | tail -60

scrub < "$OUT" > "$RES/rows-$DRIVER.txt"
