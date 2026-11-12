#!/bin/bash
# Upstream verification without the fleet's authority, on Docker: the gateway restarts without
# additional_ca_bundle, a new sandbox (agent-n, A's provider and policy) calls the router, and
# the gateway goes back to its configuration. Writes results/tls-noca.txt.
set -u
. "$(dirname "$0")/lib.sh"
OUT="$WORK/raw/tls-noca.txt"
osh openshell gateway select fr108 > /dev/null
exec > "$OUT" 2>&1
sed '/^\[openshell.supervisor.network\]/,$d' "$WORK/os/gateway.toml" > "$WORK/os/gateway-noca.toml"
step "the gateway configuration for this run"
cat "$WORK/os/gateway-noca.toml"
gw_stop "$WORK/os/gateway.toml"; gw_start "$WORK/os/gateway-noca.toml"
osh openshell sandbox create --name agent-n --from fr108-agent:spike --provider fleetring-a --policy "$WORK/run/policy-a.yaml" --detach -- sleep infinity 2>&1 | tail -1
step "agent-n calls the router, whose certificate only the fleet's authority signs"
ax agent-n "curl -sS -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H 'content-type: application/json' https://$ROUTER_IP:8443/db/agent-a-memory/v3/pipeline -d '{\"requests\":[{\"type\":\"close\"}]}' -w '\nHTTP %{http_code}\n'"
sleep 2; osh openshell logs agent-n --since 2m --source sandbox 2>&1 | grep -E "8443" | tail -4
osh openshell sandbox delete agent-n 2>&1 | tail -1; sleep 3
gw_stop "$WORK/os/gateway-noca.toml"; gw_start "$WORK/os/gateway.toml"
step "with the authority back (A, created under the full configuration)"
sleep 10; ax agent-a "curl -sS -o /dev/null -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H 'content-type: application/json' https://$ROUTER_IP:8443/db/agent-a-memory/v3/pipeline -d '{\"requests\":[{\"type\":\"close\"}]}' -w 'HTTP %{http_code}\n'"
scrub < "$OUT" > "$RES/tls-noca.txt"
