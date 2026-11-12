#!/bin/bash
# FR-108 setup on Docker: a dev fleet; routers; an OpenShell 0.1.8 gateway on the Docker driver;
# sandboxes agent-a and agent-b, each with its own work volume and only an OpenShell
# placeholder for its own FleetRing token. The experiment scripts (ex-*.sh) run after it;
# setup-vm.sh adds the microVM gateway. See lib.sh for the variables.
#
#   WORK=... ROUTER_IP=... FLEETRING=... OPENSHELL_BIN=... bash setup.sh
set -eu
. "$(dirname "$0")/lib.sh"
mkdir -p "$WORK/fr/home" "$WORK/fr/router" "$WORK/os/home" "$WORK/os/tls" "$WORK/run"

# 1. The dev fleet, on its own port, with a router principal that relays tokens.
fr dev -d --port 7410
fr cert client router --dir "$DEV/tls" -q
fr cert node router-http --addresses "$ROUTER_IP,host.openshell.internal,127.0.0.1" --dir "$DEV/tls" -q
fr cert node wrong-name --addresses wrong-name.invalid --dir "$DEV/tls" -q
# `fleetring dev` refuses a directory holding files it never writes: keep the router's apart.
mv "$DEV"/tls/node.router-http.* "$DEV"/tls/node.wrong-name.* "$DEV"/tls/client.router.* "$WORK/fr/router/"
printf '\n[[principal]]\nname = "router"\nscopes = []\nforward_tokens = true\n' >> "$DEV/fleetringd.toml"
fr dev stop
fr dev -d

# 2. Routers, each with TLS from the fleet's authority:
#    $ROUTER_IP:8443  the router the sandboxes use (private, non-loopback)
#    127.0.0.1:8444   the same fleet on loopback only, for the host-alias topology
#    $ROUTER_IP:8446  a certificate from the fleet's authority for the wrong name
router() { # name listen cert
  cat > "$WORK/fr/router/$1.toml" <<EOF
listen = "$2"
[http]
cert = "$WORK/fr/router/node.$3.crt"
key = "$WORK/fr/router/node.$3.key"
[tls]
ca = "$DEV/tls/ca.crt"
cert = "$WORK/fr/router/client.router.crt"
key = "$WORK/fr/router/client.router.key"
[control]
address = "127.0.0.1:7410"
EOF
  nohup env HOME="$WORK/fr/home" "$FLEETRING" router --config "$WORK/fr/router/$1.toml" > "$WORK/fr/router/$1.log" 2>&1 &
}
router lan "$ROUTER_IP:8443" router-http
router loopback 127.0.0.1:8444 router-http
router wrongname "$ROUTER_IP:8446" wrong-name

# 3. Agents A and B, with their own databases; B's holds a canary row.
fr agent add a
fr agent add b
fr db create agent-a-memory --agent a
fr db create agent-b-memory --agent b
fr db exec agent-b-memory --agent b "CREATE TABLE secrets (k TEXT PRIMARY KEY, v TEXT)"
fr db exec agent-b-memory --agent b "INSERT INTO secrets VALUES ('plan', 'b-private-plan')"
fr token issue --principal agent-a --scope 'read:agent-a-*' --ttl 2h --dir "$DEV/tls" -q > "$WORK/fr/token-a-ro"

# What transcripts must never show.
{
  printf '%s\t<TOKEN-A>\n' "$(tr -d '\n' < "$DEV/agents/a/token")"
  printf '%s\t<TOKEN-B>\n' "$(tr -d '\n' < "$DEV/agents/b/token")"
  printf '%s\t<TOKEN-A-READ-ONLY>\n' "$(tr -d '\n' < "$WORK/fr/token-a-ro")"
  printf '%s\t192.0.2.10\n' "$ROUTER_IP"
  printf '%s\t<work>\n' "$WORK"
  printf '%s\t<home>\n' "$HOME"
  printf '%s\t<user>\n' "$(id -un)"
  printf '%s\t<host>\n' "$(hostname)"
  printf '%s\t<host>\n' "$(hostname -s)"
} > "$WORK/scrub.map"

# 4. The OpenShell gateway: Docker driver, bridge networking (Docker Desktop's host networking
# is off by default), driver config allowed (for the work volumes; resource admission stays on),
# and the fleet's authority trusted for sandbox egress.
osh openshell-gateway generate-certs --output-dir "$WORK/os/tls" --server-san host.openshell.internal
cp "$DEV/tls/ca.crt" "$WORK/os/fleet-ca.pem"
gw_conf() { # file extra-docker-lines
  cat > "$1" <<EOF
[openshell]
version = 2

[openshell.gateway]
compute_driver = "docker"

[openshell.drivers.docker]
socket_path = "/var/run/docker.sock"
supervisor_network_mode = "bridge"
allow_driver_config = true
$2
[openshell.supervisor.network]
additional_ca_bundle = "$WORK/os/fleet-ca.pem"
EOF
}
gw_conf "$WORK/os/gateway.toml" ""
osh openshell-gateway config preflight --path "$WORK/os/gateway.toml"
gw_start "$WORK/os/gateway.toml"
osh openshell gateway add https://127.0.0.1:17670 --local --name fr108
# Proposals never widen a policy without a person, whatever a sandbox asks for.
osh openshell settings set --global --key proposal_approval_mode --value manual --yes

# 5. The agent image, the profiles, one provider and one work volume per agent.
docker build -q -t fr108-agent:spike "$HERE"
osh openshell profile import -f "$HERE/fleetring-router.profile.yaml"
osh openshell profile import -f "$HERE/fleetring-router-stable.profile.yaml"
for a in a b; do
  FLEETRING_TOKEN="$(cat "$DEV/agents/$a/token")" \
    osh openshell provider create --name "fleetring-$a" --type fleetring-router --credential FLEETRING_TOKEN
  docker volume create --label openshell.ai/sandbox-attachable=true \
    --label openshell.ai/sandbox-attachable-workspace=default "fr108-work-$a"
  # A new volume is root's; the agent's account (10001 in the image) must own it.
  docker run --rm --user 0 -v "fr108-work-$a:/w" fr108-agent:spike chown 10001:10001 /w
  render_policy "$a"
  create_agent "$a"
done

# 6. B's canaries, in its work volume and its home: a holder key, a token, a SQLite file, and a
# listening Unix socket standing in for a multiplexer's control socket.
ax agent-b '
  mkdir -p /sandbox/work/.fleetring/agents/b
  echo B-HOLDER-KEY-CANARY > /sandbox/work/.fleetring/agents/b/client.agent-b.key
  echo B-TOKEN-CANARY > /sandbox/work/.fleetring/agents/b/token
  sqlite3 /sandbox/work/b-local.sqlite "CREATE TABLE t (x); INSERT INTO t VALUES (1)"
  nohup python3 -c "
import socket
s = socket.socket(socket.AF_UNIX); s.bind(\"/tmp/b-mux.sock\"); s.listen(5)
while True:
    c, _ = s.accept(); c.sendall(b\"B-MUX-CANARY\"); c.close()
" > /dev/null 2>&1 &
  sleep 1'
for f in own-work.sh hostile-agent.sh probe-loop.sh http-layer.sh exhaust.sh; do
  osh openshell sandbox upload agent-a "$HERE/$f" /sandbox/
  osh openshell sandbox upload agent-b "$HERE/$f" /sandbox/
done
echo "setup done"
