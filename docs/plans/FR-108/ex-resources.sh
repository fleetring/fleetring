#!/bin/bash
# A attacks B's availability, on Docker: memory, processes and disk, with no limits set (the
# default) while B keeps working, then memory with --memory set. Bounded on purpose (1 GiB of
# memory, 2 GiB of disk) so the test can't take the Docker Desktop VM down. Writes
# results/resources.txt.
set -u
. "$(dirname "$0")/lib.sh"
OUT="$WORK/raw/resources.txt"
osh openshell gateway select fr108 > /dev/null
exec > "$OUT" 2>&1
W=$(docker ps --format '{{.Names}}' | grep -E '^openshell-default--agent-a-[0-9a-f-]+$')

step "A's limits as Docker set them, and as A's cgroup shows them"
docker inspect "$W" --format 'memory={{.HostConfig.Memory}} nanocpus={{.HostConfig.NanoCpus}} pids={{.HostConfig.PidsLimit}}'
docker exec "$W" sh -c 'for f in memory.max pids.max cpu.max; do echo "$f: $(cat /sys/fs/cgroup/$f)"; done'
docker run --rm alpine:3 sh -c 'echo "Docker Desktop VM memory: $(grep MemTotal /proc/meminfo)"'

step "memory: A allocates 1024 MiB with no limit, while B works"
ax agent-a 'sh /sandbox/exhaust.sh mem 1024' &
sleep 2; ax agent-b "sh /sandbox/exhaust.sh probe $ROUTER_IP:8443"; wait

step "processes: A asks for 3000, while B works"
ax agent-a 'sh /sandbox/exhaust.sh pids 3000' &
sleep 3; ax agent-b "sh /sandbox/exhaust.sh probe $ROUTER_IP:8443"; wait

step "disk: A writes 2048 MiB to its work volume (the Docker Desktop VM's disk, shared by every sandbox)"
ax agent-a 'sh /sandbox/exhaust.sh disk 2048'

step "memory with --memory 256Mi: sandbox agent-m allocates 512 MiB"
osh openshell sandbox create --name agent-m --from fr108-agent:spike --policy "$WORK/run/policy-c.yaml" --memory 256Mi --cpu 1 --detach -- sleep infinity 2>&1 | tail -1
M=$(docker ps --format '{{.Names}}' | grep -E '^openshell-default--agent-m-[0-9a-f-]+$')
docker inspect "$M" --format 'memory={{.HostConfig.Memory}} nanocpus={{.HostConfig.NanoCpus}}'
osh openshell sandbox upload agent-m "$HERE/exhaust.sh" /sandbox/ > /dev/null 2>&1
ax agent-m 'sh /sandbox/exhaust.sh mem 512'; echo "[exec exit $?]"
osh openshell sandbox get agent-m 2>&1 | grep -i -E "phase|oom|reason" | head -4
osh openshell sandbox delete agent-m 2>&1 | tail -1

scrub < "$OUT" > "$RES/resources.txt"
