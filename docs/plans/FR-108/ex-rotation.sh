#!/bin/bash
# Rotation, expiry, detach and stable placeholders, measured on processes already running in
# sandbox A (Docker). Each experiment starts probe-loop.sh, changes the provider from the host,
# and keeps the loop's log: results/rotation.txt, and each log in results/rotation-logs/.
set -u
. "$(dirname "$0")/lib.sh"
OUT="$WORK/raw/rotation.txt"; LOGS="$RES/rotation-logs"; mkdir -p "$LOGS"
osh openshell gateway select fr108 > /dev/null
exec > "$OUT" 2>&1
TA=$(cat "$DEV/agents/a/token"); TRO=$(cat "$WORK/fr/token-a-ro")
ms() { echo $(( ($(date +%s) + $1) * 1000 )); }
loop_start() { # sandbox log
  ax "$1" "pkill -f '^sh /sandbox/probe-loop' ; nohup sh /sandbox/probe-loop.sh $ROUTER_IP:8443 > /tmp/$2.log 2>&1 & sleep 2; echo \"loop $2 started; its placeholder: \$FLEETRING_TOKEN\""
}
loop_stop() { # sandbox log
  ax "$1" "pkill -f '^sh /sandbox/probe-loop'; cat /tmp/$2.log" > "$WORK/raw/$2.log" 2>&1
  scrub < "$WORK/raw/$2.log" > "$LOGS/$2.txt"
  echo "loop $2: outcomes in order (count, HTTP status, outcome):"
  awk '{print $2, $3}' "$WORK/raw/$2.log" | uniq -c
  echo "loop $2: last success $(grep affected "$WORK/raw/$2.log" | tail -1 | cut -d' ' -f1), first failure $(grep -v affected "$WORK/raw/$2.log" | head -1 | cut -d' ' -f1)"
}
fresh() { # a new process's view: placeholder and one INSERT
  ax agent-a "echo \"new process placeholder: \$FLEETRING_TOKEN\"; curl -s -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H 'content-type: application/json' https://$ROUTER_IP:8443/db/agent-a-memory/v3/pipeline -d '{\"requests\":[{\"type\":\"execute\",\"stmt\":{\"sql\":\"INSERT INTO notes (body) VALUES (?)\",\"args\":[{\"type\":\"text\",\"value\":\"fresh\"}]}},{\"type\":\"close\"}]}' -w ' HTTP %{http_code}\n' | grep -o -E '\"affected_row_count\":1|\"code\":\"[A-Z_]+\"|\"error\":\"[a-z_]+\"| HTTP [0-9]+' | tr '\n' ' '; echo"
}
update() { # provider, then openshell provider update arguments; value in FLEETRING_TOKEN
  p=$1; shift
  echo "provider update $p $* at $(now)"
  osh openshell provider update "$p" "$@" --wait --timeout 60 2>&1 | grep -E "ready|withheld|Wait|rror|Installed"
  echo "returned at $(now)"
}

step "clocks: the Mac (gateway and CLI), the Docker Desktop VM, sandbox A"
echo "mac            $(now)"
echo "docker vm      $(docker run --rm alpine:3 date -u +%s)"
ax agent-a 'echo "sandbox A      $(date -u +%s.%N | cut -c1-14)"'
echo "mac            $(now)"

step "R1: provider update to a read-only token, with a process already running"
loop_start agent-a r1-update; sleep 5
FLEETRING_TOKEN="$TRO" update fleetring-a --credential FLEETRING_TOKEN
sleep 25; fresh; loop_stop agent-a r1-update
FLEETRING_TOKEN="$TA" update fleetring-a --credential FLEETRING_TOKEN > /dev/null

step "R2: expiry set BEFORE the process starts, so the process holds the generation that carries it"
E=$(ms 40); echo "expiry $E ms ($(( E / 1000 )) s)"
FLEETRING_TOKEN="$TA" update fleetring-a --credential FLEETRING_TOKEN --credential-expires-at "FLEETRING_TOKEN=$E"
osh openshell provider get fleetring-a -o json 2>&1 | grep -i -E "expir" | head -5
loop_start agent-a r2-expiry-before; echo "waiting past the expiry: now $(now)"
sleep 60; echo "now $(now)"; fresh; loop_stop agent-a r2-expiry-before

step "R3: expiry set AFTER the process starts (value unchanged, only --credential-expires-at)"
FLEETRING_TOKEN="$TA" update fleetring-a --credential FLEETRING_TOKEN --credential-expires-at FLEETRING_TOKEN=0 > /dev/null
loop_start agent-a r3-expiry-after; sleep 3
E=$(ms 25); echo "expiry $E ms ($(( E / 1000 )) s)"
update fleetring-a --credential-expires-at "FLEETRING_TOKEN=$E"
sleep 50; echo "now $(now)"; fresh; loop_stop agent-a r3-expiry-after
FLEETRING_TOKEN="$TA" update fleetring-a --credential FLEETRING_TOKEN --credential-expires-at FLEETRING_TOKEN=0 > /dev/null

step "R4: detach while the policy binds the provider; then unbind; then detach; then re-attach and re-bind"
loop_start agent-a r4-unbind; sleep 3
echo "detach at $(now):"; osh openshell sandbox provider detach agent-a fleetring-a --wait 2>&1 | tail -3
base_policy agent-a "$WORK/run/base-a.yaml"
python3 -I "$HERE/policy_edit.py" "$WORK/run/base-a.yaml" "$WORK/run/unbound-a.yaml" unbind
echo "unbind submitted at $(now)"; set_policy agent-a "$WORK/run/unbound-a.yaml"; echo "loaded at $(now)"
sleep 5; fresh
echo "detach at $(now):"; osh openshell sandbox provider detach agent-a fleetring-a --wait 2>&1 | tail -2
osh openshell sandbox provider attach agent-a fleetring-a --wait 2>&1 | tail -1
set_policy agent-a "$WORK/run/base-a.yaml"; sleep 3
osh openshell sandbox provider status agent-a fleetring-a 2>&1 | head -1
fresh; loop_stop agent-a r4-unbind

step "R5: stable_placeholder: true (profile fleetring-router-stable), in sandbox agent-s"
FLEETRING_TOKEN="$TA" osh openshell provider create --name fleetring-a-stable --type fleetring-router-stable --credential FLEETRING_TOKEN 2>&1 | tail -1
sed 's/provider: fleetring-a$/provider: fleetring-a-stable/' "$WORK/run/policy-a.yaml" > "$WORK/run/policy-s.yaml"
osh openshell sandbox create --name agent-s --from fr108-agent:spike --provider fleetring-a-stable --policy "$WORK/run/policy-s.yaml" --detach -- sleep infinity 2>&1 | tail -1
osh openshell sandbox upload agent-s "$HERE/probe-loop.sh" /sandbox/ > /dev/null 2>&1
loop_start agent-s r5-stable; sleep 5
FLEETRING_TOKEN="$TRO" update fleetring-a-stable --credential FLEETRING_TOKEN; sleep 15
FLEETRING_TOKEN="dead-value-not-a-token" update fleetring-a-stable --credential FLEETRING_TOKEN; sleep 15
FLEETRING_TOKEN="$TA" update fleetring-a-stable --credential FLEETRING_TOKEN; sleep 10
E=$(ms 20); echo "expiry $E ms ($(( E / 1000 )) s)"
update fleetring-a-stable --credential-expires-at "FLEETRING_TOKEN=$E"
sleep 40; echo "now $(now)"; loop_stop agent-s r5-stable
osh openshell sandbox delete agent-s 2>&1 | tail -1; sleep 3
osh openshell provider delete fleetring-a-stable 2>&1 | tail -1

step "clocks again"
echo "mac            $(now)"
echo "docker vm      $(docker run --rm alpine:3 date -u +%s)"
ax agent-a 'echo "sandbox A      $(date -u +%s.%N | cut -c1-14)"'

scrub < "$OUT" > "$RES/rotation.txt"
