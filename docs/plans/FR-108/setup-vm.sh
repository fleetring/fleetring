#!/bin/bash
# FR-108 on OpenShell's microVM driver (libkrun; Hypervisor.framework on macOS): a second
# gateway on port 17680 beside setup.sh's, with the same fleet, routers, profiles and policies.
# Needs e2fsprogs (on macOS: `brew install e2fsprogs`) and VM_STATE, a directory with a short
# path: the driver's Unix socket lives under it, and a long path fails with "path must be
# shorter than SUN_LEN". The VM driver takes no Docker volumes, so /sandbox/work is a plain
# directory in each guest.
#
#   VM_STATE=/short/dir WORK=... ROUTER_IP=... FLEETRING=... OPENSHELL_BIN=... bash setup-vm.sh
set -eu
. "$(dirname "$0")/lib.sh"
: "${VM_STATE:?}"
mkdir -p "$VM_STATE" "$WORK/vm/state"
# The formula signs the driver with the hypervisor entitlement; do the same for the tarball.
cat > "$WORK/vm/entitlements.plist" <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict><key>com.apple.security.hypervisor</key><true/></dict></plist>
EOF
codesign --entitlements "$WORK/vm/entitlements.plist" --force -s - "$OPENSHELL_BIN/openshell-driver-vm"
cat > "$WORK/vm/gateway-vm.toml" <<EOF
[openshell]
version = 2

[openshell.gateway]
bind_address = "127.0.0.1:17680"
compute_driver = "vm"

[openshell.supervisor.network]
additional_ca_bundle = "$WORK/os/fleet-ca.pem"

[openshell.drivers.vm]
state_dir = "$VM_STATE"
driver_dir = "$OPENSHELL_BIN"
default_image = "fr108-agent:spike"
bootstrap_image = "nvcr.io/nvidia/base/ubuntu:24.04"
vcpus = 2
mem_mib = 1024
overlay_disk_mib = 2048
EOF
# Its own state directory, so the two gateways' databases stay apart.
vm_osh() { osh env XDG_STATE_HOME="$WORK/vm/state" "$@"; }
vm_osh "$OPENSHELL_BIN/openshell-gateway" config preflight --path "$WORK/vm/gateway-vm.toml"
: > "$WORK/vm/gateway-vm.toml.log"
vm_osh nohup "$OPENSHELL_BIN/openshell-gateway" --config "$WORK/vm/gateway-vm.toml" >> "$WORK/vm/gateway-vm.toml.log" 2>&1 &
wait_for "$WORK/vm/gateway-vm.toml.log" "Gateway listener bound"
osh openshell gateway add https://127.0.0.1:17680 --local --name fr108vm
osh openshell gateway select fr108vm
osh openshell settings set --global --key proposal_approval_mode --value manual --yes
osh openshell profile import -f "$HERE/fleetring-router.profile.yaml"
for a in a b; do
  FLEETRING_TOKEN="$(cat "$DEV/agents/$a/token")" \
    osh openshell provider create --name "fleetring-$a" --type fleetring-router --credential FLEETRING_TOKEN
  osh openshell sandbox create --name "agent-$a" --from fr108-agent:spike --provider "fleetring-$a" \
    --policy "$WORK/run/policy-$a.yaml" --detach -- sleep infinity
done
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
ax agent-a 'mkdir -p /sandbox/work'
echo "setup-vm done"
