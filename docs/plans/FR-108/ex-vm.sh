#!/bin/bash
# What differs on the microVM driver: clocks, the VM gateway's port, memory (--memory against
# mem_mib), and the host alias (the driver maps host.openshell.internal to host loopback).
# Run after setup-vm.sh and `DRIVER=vm GW=fr108vm bash ex-rows.sh`. Writes results/vm.txt.
set -u
. "$(dirname "$0")/lib.sh"
OUT="$WORK/raw/vm.txt"
osh openshell gateway select fr108vm > /dev/null
exec > "$OUT" 2>&1

step "clocks: the Mac (gateway) and the guest"
echo "mac      $(now)"; ax agent-a 'echo "guest A  $(date -u +%s.%N | cut -c1-14); kernel $(uname -r)"'; echo "mac      $(now)"

step "the VM gateway's own port, from A"
ax agent-a "for e in host.openshell.internal:17680 $ROUTER_IP:17680 127.0.0.1:17680; do curl -sS -m 5 https://\$e/ -o /dev/null -w \"\$e HTTP %{http_code}\n\" 2>&1; done"

step "the VM's channel to its supervisor is vsock: can A open a vsock socket, or see the guest's processes?"
ax agent-a 'python3 -c "
import socket
try:
    s = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM); s.settimeout(3); s.connect((2, 1024)); print(\"vsock connected\")
except Exception as e:
    print(\"vsock:\", repr(e))
"; ls /proc 2>&1 | head -3; ps -e 2>&1 | head -3'

step "memory: mem_mib = 1024 per guest; a sandbox created with --memory 256Mi allocates 512 MiB, then 1500 MiB"
osh openshell sandbox create --name agent-m --from fr108-agent:spike --policy "$WORK/run/policy-c.yaml" --memory 256Mi --detach -- sleep infinity 2>&1 | tail -1
osh openshell sandbox upload agent-m "$HERE/exhaust.sh" /sandbox/ > /dev/null 2>&1
ax agent-m 'grep MemTotal /proc/meminfo 2>&1; sh /sandbox/exhaust.sh mem 512'; echo "[exec exit $?]"
ax agent-m 'sh /sandbox/exhaust.sh mem 1500'; echo "[exec exit $?]"
osh openshell sandbox get agent-m 2>&1 | grep -i -E "phase" | head -2
osh openshell sandbox delete agent-m 2>&1 | tail -1

step "the host alias on the VM driver: the router on 127.0.0.1:8444 through host.openshell.internal"
base_policy agent-a "$WORK/run/vm-base-a.yaml"
for variant in no-allowed-ips; do
  cat > "$WORK/run/rules-vm-alias.yaml" <<EOF
  router_via_alias:
    endpoints:
      - host: host.openshell.internal
        port: 8444
        protocol: rest
        enforcement: enforce
        rules:
          - allow: { method: POST, path: "/db/agent-a-*/v3/pipeline" }
        credential_binding:
          provider: fleetring-a
    binaries:
      - path: /usr/bin/curl
EOF
  python3 -I "$HERE/policy_edit.py" "$WORK/run/vm-base-a.yaml" "$WORK/run/vm-alias-a.yaml" rule "$WORK/run/rules-vm-alias.yaml"
  echo "-- $variant"; cat "$WORK/run/rules-vm-alias.yaml"
  set_policy agent-a "$WORK/run/vm-alias-a.yaml"
  osh openshell sandbox upload agent-a "$HERE/own-work.sh" /sandbox/ > /dev/null 2>&1
  ax agent-a "getent hosts host.openshell.internal; sh /sandbox/own-work.sh host.openshell.internal:8444; curl -sS -m 5 https://host.openshell.internal:7410/ -w 'dev node port: HTTP %{http_code}\n'"
  osh openshell logs agent-a --since 1m --source sandbox 2>&1 | grep -E "8444|7410|host.openshell" | tail -6
done
set_policy agent-a "$WORK/run/vm-base-a.yaml"

scrub < "$OUT" > "$RES/vm.txt"
