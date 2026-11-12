#!/bin/bash
# Endpoints beside the router, on Docker: where a placeholder goes at an allowed but unbound
# endpoint (L4 and inspected), whether a bound endpoint can reflect the real credential back,
# whether the router reflects it, the agent's view of TLS, a router certificate for the wrong
# name, raw HTTP framing (http-layer.sh), and the host-alias topology: the router on 127.0.0.1
# reached only through host.openshell.internal on one port. Writes results/endpoints.txt.
set -u
. "$(dirname "$0")/lib.sh"
OUT="$WORK/raw/endpoints.txt"
osh openshell gateway select fr108 > /dev/null
exec > "$OUT" 2>&1
ALIAS_IP=192.168.65.254   # what host.openshell.internal resolves to on Docker Desktop
: > "$WORK/raw/echo.log"
for p in 8445 8447 8448; do nohup python3 -I "$HERE/echo.py" $p "$WORK/raw/echo.log" > /dev/null 2>&1 & done
sleep 1
base_policy agent-a "$WORK/run/base-a.yaml"
cat > "$WORK/run/rules-endpoints.yaml" <<EOF
  echo_l4:
    endpoints:
      - host: host.openshell.internal
        port: 8445
        allowed_ips: ["$ALIAS_IP/32"]
    binaries:
      - path: /usr/bin/curl
  echo_rest:
    endpoints:
      - host: host.openshell.internal
        port: 8447
        allowed_ips: ["$ALIAS_IP/32"]
        protocol: rest
        enforcement: enforce
        access: full
    binaries:
      - path: /usr/bin/curl
  echo_bound:
    endpoints:
      - host: host.openshell.internal
        port: 8448
        allowed_ips: ["$ALIAS_IP/32"]
        protocol: rest
        enforcement: enforce
        access: full
        credential_binding:
          provider: fleetring-a
    binaries:
      - path: /usr/bin/curl
  router_via_alias:
    endpoints:
      - host: host.openshell.internal
        port: 8444
        allowed_ips: ["$ALIAS_IP/32"]
        protocol: rest
        enforcement: enforce
        rules:
          - allow: { method: POST, path: "/db/agent-a-*/v3/pipeline" }
        credential_binding:
          provider: fleetring-a
    binaries:
      - path: /usr/bin/curl
  wrong_name:
    endpoints:
      - host: "$ROUTER_IP"
        port: 8446
        allowed_ips: ["$ROUTER_IP/32"]
        protocol: rest
        enforcement: enforce
        access: full
    binaries:
      - path: /usr/bin/curl
EOF
python3 -I "$HERE/policy_edit.py" "$WORK/run/base-a.yaml" "$WORK/run/endpoints-a.yaml" rule "$WORK/run/rules-endpoints.yaml"
python3 -I "$HERE/policy_edit.py" "$WORK/run/endpoints-a.yaml" "$WORK/run/endpoints-a.yaml" bin /usr/bin/openssl
step "A's policy for these experiments: its base policy, openssl added to the router rule, and these rules"
cat "$WORK/run/rules-endpoints.yaml"
set_policy agent-a "$WORK/run/endpoints-a.yaml"

step "S8: A's placeholder at an allowed endpoint that isn't bound: L4 (8445) and inspected REST (8447)"
for port in 8445 8447; do
  ax agent-a "echo \"-- port $port\"; curl -sS -m 5 -H \"Authorization: Bearer \$FLEETRING_TOKEN\" http://host.openshell.internal:$port/bearer -w ' HTTP %{http_code}\n'; curl -sS -m 5 -H \"X-Leak: \$FLEETRING_TOKEN\" http://host.openshell.internal:$port/other-header -w ' HTTP %{http_code}\n'; curl -sS -m 5 -d \"t=\$FLEETRING_TOKEN\" http://host.openshell.internal:$port/body -w ' HTTP %{http_code}\n'"
done
echo "--- what the echo servers received"
cat "$WORK/raw/echo.log"; : > "$WORK/raw/echo.log"

step "reflection: an endpoint bound to A's provider (8448) that echoes the request back"
ax agent-a "curl -sS -m 5 -H \"Authorization: Bearer \$FLEETRING_TOKEN\" http://host.openshell.internal:8448/reflect -w ' HTTP %{http_code}\n'"
echo "--- what the echo server received"; cat "$WORK/raw/echo.log"; : > "$WORK/raw/echo.log"

step "reflection: does any router response carry A's token back? (the token would show as <TOKEN-A>)"
ax agent-a "
P='https://$ROUTER_IP:8443/db/agent-a-memory/v3/pipeline'
A='Authorization: Bearer '\$FLEETRING_TOKEN
for b in 'not json' '{\"requests\":[{\"type\":\"execute\",\"stmt\":{\"sql\":\"SELEC nonsense\"}}]}' '{\"baton\":\"forged\",\"requests\":[]}' '{\"requests\":[{\"type\":\"execute\",\"stmt\":{\"sql\":\"SELECT ?\",\"args\":[{\"type\":\"text\",\"value\":\"x\"}]}}]}'; do
  curl -sS -m 5 -D - -H \"\$A\" -H 'content-type: application/json' \"\$P\" -d \"\$b\"; echo; done
curl -sS -m 5 -D - -H \"\$A\" -H 'content-type: application/json' https://$ROUTER_IP:8443/db/agent-a-missing/v3/pipeline -d '{\"requests\":[{\"type\":\"execute\",\"stmt\":{\"sql\":\"SELECT 1\"}}]}'; echo
curl -sS -m 5 -D - -H \"\$A\" https://$ROUTER_IP:8443/db/agent-a-memory/v3; echo" > "$WORK/raw/reflect.txt" 2>&1
cat "$WORK/raw/reflect.txt"
echo "token occurrences in router responses: $(grep -c -F "$(cat "$DEV/agents/a/token")" "$WORK/raw/reflect.txt")"

step "TLS as the agent sees it, and what it trusts"
ax agent-a "curl -sv -o /dev/null https://$ROUTER_IP:8443/v3 2>&1 | grep -E 'SSL connection|subject:|issuer:|subjectAltName|expire|start date'; openssl x509 -in /run/openshell-supervisor-ca/material/ca.crt -noout -subject -dates; echo \"certificates in the agent's bundle: \$(grep -c 'BEGIN CERT' \$SSL_CERT_FILE)\""
L=$(sed -n 2p "$DEV/tls/ca.crt")
ax agent-a "grep -c '$L' \$SSL_CERT_FILE | sed 's/^/fleet authority in the agent bundle: /'"

step "a router certificate from the fleet's authority for the wrong name (wrong-name.invalid) at $ROUTER_IP:8446"
ax agent-a "curl -sS -m 5 https://$ROUTER_IP:8446/v3 -w '\nHTTP %{http_code}\n'"
osh openshell logs agent-a --since 1m --source sandbox 2>&1 | grep -E "8446" | tail -3
docker run --rm -v "$DEV/tls/ca.crt:/ca.crt:ro" alpine:3 sh -c "apk add -q curl > /dev/null 2>&1; curl -sS --cacert /ca.crt https://$ROUTER_IP:8446/v3 2>&1 | head -2" | sed 's/^/host-side curl with the fleet CA: /'

step "raw HTTP framing on one TLS connection (http-layer.sh, openssl allowed for this run)"
ax agent-a "sh /sandbox/http-layer.sh $ROUTER_IP"
osh openshell logs agent-a --since 2m --source sandbox 2>&1 | grep -E "openssl|Transfer-Encoding|Content-Length" | tail -12

step "the host-alias topology: own-work.sh against the router on 127.0.0.1:8444, through host.openshell.internal"
ax agent-a "sh /sandbox/own-work.sh host.openshell.internal:8444"
ax agent-a "curl -sS -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H 'content-type: application/json' https://host.openshell.internal:8444/db/agent-b-memory/v3/pipeline -d '{\"requests\":[{\"type\":\"close\"}]}' -w '\nHTTP %{http_code}\n'; curl -sS -m 5 https://host.openshell.internal:7410/ -w 'dev node port: HTTP %{http_code}\n'; curl -sS -m 5 https://host.openshell.internal:17670/ -w 'gateway port: HTTP %{http_code}\n'"
echo "listening sockets of the routers on the Mac:"; lsof -nP -iTCP:8443 -iTCP:8444 -sTCP:LISTEN | awk 'NR>1 {print $1, $9}' | sort -u

set_policy agent-a "$WORK/run/base-a.yaml"
pkill -f "$HERE/echo.py" || true
scrub < "$OUT" > "$RES/endpoints.txt"
