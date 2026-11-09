#!/usr/bin/env bash
# Starts a three-node fleet and a router on loopback from a `fleetring` binary, for the router's
# Node conformance script (FR-066). Everything it writes goes under the work directory, its home
# included, so it never touches the real ~/.fleetring or ~/.config/fleetring.
#
#   tests/router/start-fleet.sh <fleetring binary> <work directory>
#
# It prints `ROUTER_URL=…` and `FLEETRING_TOKEN=…` for the script, and leaves the processes' IDs in
# <work directory>/pids; tests/router/stop-fleet.sh stops them.
set -euo pipefail

bin=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
mkdir -p "$2"
work=$(cd "$2" && pwd)
export HOME="$work/home"
mkdir -p "$HOME"
for variable in $(env | grep -o '^FLEETRING_[A-Z_]*' || true); do
  unset "$variable"
done

tls="$work/tls"
control=127.0.0.1:17400
router=127.0.0.1:18080

"$bin" -q cert init --fleet conformance --dir "$tls" > /dev/null
for node in control data-1 data-2 data-3; do
  "$bin" -q cert node "$node" --addresses 127.0.0.1 --dir "$tls" > /dev/null
done
"$bin" -q cert client tester --dir "$tls" > /dev/null
"$bin" -q cert client router --dir "$tls" > /dev/null

principals='
[[principal]]
name = "tester"
scopes = ["admin:*", "fleet"]

[[principal]]
name = "router"
scopes = []
forward_tokens = true
'

node_config() {
  local id=$1 role=$2 port=$3
  cat > "$work/$id.toml" <<EOF
data_dir = "$work/$id"
listen = "127.0.0.1:$port"
role = "$role"

[node]
id = "$id"

[control]
address = "$control"

[tls]
ca = "$tls/ca.crt"
cert = "$tls/node.$id.crt"
key = "$tls/node.$id.key"

[admin]
listen = "127.0.0.1:0"
$principals
EOF
}

: > "$work/pids"
node_config control control 17400
"$bin" serve --config "$work/control.toml" > "$work/control.log" 2>&1 &
echo $! >> "$work/pids"
for index in 1 2 3; do
  node_config "data-$index" data "1741$index"
  "$bin" serve --config "$work/data-$index.toml" > "$work/data-$index.log" 2>&1 &
  echo $! >> "$work/pids"
done

client=(--control "$control" --tls-dir "$tls" --as tester)
for _ in $(seq 120); do
  if "$bin" -q db create app --replication-factor 3 "${client[@]}" > /dev/null 2>&1; then
    created=yes
    break
  fi
  sleep 0.5
done
if [ "${created:-}" != yes ]; then
  echo "the fleet didn't come up; see $work/*.log" >&2
  exit 1
fi

cat > "$work/router.toml" <<EOF
listen = "$router"

[tls]
ca = "$tls/ca.crt"
cert = "$tls/client.router.crt"
key = "$tls/client.router.key"

[control]
address = "$control"
EOF
"$bin" router --config "$work/router.toml" > "$work/router.log" 2>&1 &
echo $! >> "$work/pids"
for _ in $(seq 60); do
  if curl -fsS "http://$router/db/app/v3" > /dev/null 2>&1; then
    up=yes
    break
  fi
  sleep 0.5
done
if [ "${up:-}" != yes ]; then
  echo "the router didn't come up; see $work/router.log" >&2
  exit 1
fi

token=$("$bin" -q token issue --principal app --scope write:app --ttl 1h --dir "$tls")
echo "ROUTER_URL=http://$router/db/app/"
echo "FLEETRING_TOKEN=$token"
