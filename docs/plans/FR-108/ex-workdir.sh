#!/bin/bash
# Work directories, on Docker, three ways a launcher could provide them:
#   W1  a labelled Docker volume per agent at /sandbox/work (what setup.sh does; admission stays on)
#   W2  copy in and out with `openshell sandbox upload` and `download`
#   W3  a host directory per agent, bind-mounted (enable_bind_mounts, resource admission off)
# Each time: can A reach B's work directory? Writes results/workdir.txt.
set -u
. "$(dirname "$0")/lib.sh"
OUT="$WORK/raw/workdir.txt"
# Host directories for W3, somewhere Docker Desktop shares reliably (it failed to create mount
# sources under /private/tmp here); removed at the end.
HOSTWORK="${HOSTWORK:-$WORK/hostwork}"
osh openshell gateway select fr108 > /dev/null
exec > "$OUT" 2>&1

step "W1: volumes. Positive control from the host: B's volume holds B's canaries"
docker run --rm -v fr108-work-b:/w:ro alpine:3 find /w -type f
step "W1: from A"
ax agent-a 'ls -la /sandbox/work; cat /sandbox/work/.fleetring/agents/b/client.agent-b.key 2>&1; grep " /sandbox/work " /proc/mounts'
step "W1: a sandbox create naming another agent's volume is the launcher's request; resource admission checks only labels"
docker volume inspect fr108-work-b --format '{{json .Labels}}'

step "W2: upload into A, download out of A"
mkdir -p "$WORK/copy/in" "$WORK/copy/out"; echo "task input for A" > "$WORK/copy/in/task.txt"
osh openshell sandbox upload agent-a "$WORK/copy/in" /sandbox/work/ 2>&1 | tail -1
ax agent-a 'find /sandbox/work/in -type f -exec sh -c "echo {}; cat {}" \;; echo "A wrote this" > /sandbox/work/in/result.txt'
osh openshell sandbox download agent-a /sandbox/work/in "$WORK/copy/out" 2>&1 | tail -1
find "$WORK/copy/out" -type f -exec sh -c 'echo "{}: $(cat {})"' \;
ax agent-b 'ls /sandbox/work/in 2>&1'

step "W3: bind mounts. The gateway restarts with enable_bind_mounts = true and resource admission off"
perl -pe 's/^allow_driver_config = true$/allow_driver_config = true\nenable_bind_mounts = true\n\n[openshell.drivers.docker.resource_admission]\nenabled = false\n/' \
  "$WORK/os/gateway.toml" > "$WORK/os/gateway-bind.toml"
printf 'version: 1\nfilesystem_policy:\n  include_workdir: true\n  read_only: [/usr, /lib, /bin, /etc]\n  read_write: [/tmp]\n' > "$WORK/run/policy-c.yaml"
sed -n '/^\[openshell.drivers.docker\]/,$p' "$WORK/os/gateway-bind.toml"
# The work directory listed explicitly beside the workdir, and /proc for the mount table.
printf 'version: 1\nfilesystem_policy:\n  include_workdir: true\n  read_only: [/usr, /lib, /bin, /etc, /proc]\n  read_write: [/tmp, /sandbox/work]\n' > "$WORK/run/policy-w.yaml"
gw_stop "$WORK/os/gateway.toml"; gw_start "$WORK/os/gateway-bind.toml"
for a in a b; do
  mkdir -p "$HOSTWORK/$a"; chmod 777 "$HOSTWORK/$a"
  osh openshell sandbox create --name "bind-$a" --from fr108-agent:spike --policy "$WORK/run/policy-w.yaml" --detach \
    --driver-config-json "{\"docker\":{\"mounts\":[{\"type\":\"bind\",\"source\":\"$HOSTWORK/$a\",\"target\":\"/sandbox/work\",\"read_only\":false}]}}" \
    -- sleep infinity 2>&1 | tail -1
done
ax bind-b 'id; stat -c "%U:%G %a %n" /sandbox/work; echo B-BIND-CANARY > /sandbox/work/canary; ls -la /sandbox/work'
B_W=$(docker ps --format '{{.Names}}' | grep -E '^openshell-default--bind-b-[0-9a-f-]+$')
echo "-- the same mount, seen with docker exec (no Landlock):"
docker exec "$B_W" sh -c 'grep " /sandbox/work " /proc/mounts; stat -c "%U:%G %a %n" /sandbox/work; ls -la /sandbox/work'
ls -la "$HOSTWORK/b"
step "W3: from A, inside its bind mount"
ax bind-a 'grep " /sandbox/work " /proc/mounts; ls -la /sandbox/work; cat /sandbox/work/../work/canary 2>&1; ls /host_mnt 2>&1; ln -s ../b/canary /sandbox/work/to-b; ln -s / /sandbox/work/to-root; ls -la /sandbox/work; cat /sandbox/work/to-b 2>&1'
step "W3: the same symlinks, followed on the host (as a launcher copying results out would)"
ls -la "$HOSTWORK/a"; echo "host reads $HOSTWORK/a/to-b: $(cat "$HOSTWORK/a/to-b" 2>&1)"
osh openshell sandbox delete bind-a bind-b 2>&1 | tail -2; sleep 3
gw_stop "$WORK/os/gateway-bind.toml"; gw_start "$WORK/os/gateway.toml"
echo "gateway back to its default configuration"; sleep 10; osh openshell sandbox list 2>&1
rm -rf "$HOSTWORK"

scrub < "$OUT" > "$RES/workdir.txt"
