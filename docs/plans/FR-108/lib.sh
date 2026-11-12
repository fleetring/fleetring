# Shared by the FR-108 scripts; sourced, not run. Every path is under $WORK, and HOME is
# redirected for both fleetring and OpenShell, so neither writes to the real home directory.
#
#   WORK      an empty scratch directory
#   ROUTER_IP a private, non-loopback address of this machine that containers reach
#   FLEETRING a 0.8 `fleetring` build
#   OPENSHELL_BIN  the directory holding the OpenShell 0.1.8 binaries
: "${WORK:?}" "${ROUTER_IP:?}" "${FLEETRING:?}" "${OPENSHELL_BIN:?}"
HERE=${HERE:-$(cd "$(dirname "$0")" && pwd)}
RES="$HERE/results"
DEV="$WORK/fr/home/.fleetring"
mkdir -p "$WORK/raw" "$RES"

fr() { env HOME="$WORK/fr/home" "$FLEETRING" "$@"; }

# OpenShell's CLI and gateway, telemetry off, state under $WORK/os/home.
osh() {
  env -u XDG_CONFIG_HOME -u XDG_STATE_HOME HOME="$WORK/os/home" PATH="$OPENSHELL_BIN:$PATH" \
    OPENSHELL_TELEMETRY_ENABLED=false OPENSHELL_LOCAL_TLS_DIR="$WORK/os/tls" "$@"
}

# Run a shell command inside a sandbox.
ax() { osh openshell sandbox exec --name "$1" -- sh -c "$2"; }

# Transcript helpers: `step` titles a step, `run` prints the command, its output and exit code.
step() { printf '\n### %s\n' "$*"; }
run() { printf '$ %s\n' "$*"; sh -c "$*" 2>&1; printf '[exit %s]\n' "$?"; }
now() { date -u +%s.%N | cut -c1-14; }

# Replace every literal in $WORK/scrub.map (tab-separated: literal, replacement) and strip
# colour codes, so committed transcripts hold no token, address, path or name of this machine.
scrub() { python3 -I "$HERE/scrub.py" "$WORK/scrub.map"; }

# Wait until a log file holds a line matching a pattern.
wait_for() { i=0; until grep -q "$2" "$1" 2>/dev/null; do i=$((i + 1)); [ $i -gt 120 ] && return 1; sleep 1; done; }

# Start or stop a gateway from a configuration file; its log goes beside the file.
gw_start() {
  : > "$1.log"
  osh nohup "$OPENSHELL_BIN/openshell-gateway" --config "$1" >> "$1.log" 2>&1 &
  wait_for "$1.log" "Gateway listener bound"
}
gw_stop() { pkill -f "openshell-gateway --config $1" || true; sleep 2; }

# Render agent $1's policy from the template into $WORK/run/policy-$1.yaml, with extra
# binaries ($2, space-separated) allowed to reach the router.
render_policy() {
  sed -e "s/@AGENT@/$1/g" -e "s/@ROUTER_IP@/$ROUTER_IP/g" "$HERE/policy.template.yaml" > "$WORK/run/policy-$1.yaml"
  for b in ${2:-}; do printf '      - path: %s\n' "$b" >> "$WORK/run/policy-$1.yaml"; done
}

# Save sandbox $1's current base policy (with the paths OpenShell added at start) to $2.
base_policy() { osh openshell policy get "$1" --base | sed -n '/^version: 1/,$p' > "$2"; }

# Apply policy file $2 to sandbox $1 and wait until it is loaded.
set_policy() { osh openshell policy set "$1" --policy "$2" --wait 2>&1 | tail -1; }

# Create agent $1's sandbox: its provider, its policy, its work volume at /sandbox/work.
create_agent() {
  osh openshell sandbox create --name "agent-$1" --from fr108-agent:spike --provider "fleetring-$1" \
    --policy "${2:-$WORK/run/policy-$1.yaml}" --detach \
    --driver-config-json "{\"docker\":{\"mounts\":[{\"type\":\"volume\",\"source\":\"fr108-work-$1\",\"target\":\"/sandbox/work\",\"read_only\":false}]}}" \
    -- sleep infinity
}
