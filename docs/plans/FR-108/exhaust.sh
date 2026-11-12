#!/bin/sh
# Run inside a sandbox: try to take a shared resource. Each mode prints what it got.
#
#   sh exhaust.sh mem <MiB>     allocate and touch that much memory
#   sh exhaust.sh pids <n>      start up to n sleeping processes, report how many started
#   sh exhaust.sh disk <MiB>    write that much to the work directory, then remove it
#   sh exhaust.sh probe         what B runs meanwhile: start 50 processes, reach the router
set -u
case "$1" in
  mem)
    python3 -c "
import sys
n = int(sys.argv[1]); blocks = []
for i in range(n):
    blocks.append(bytearray(1024 * 1024))
    if (i + 1) % 256 == 0: print('allocated', i + 1, 'MiB', flush=True)
print('done: allocated', n, 'MiB')
" "$2"; echo "[python exit $?]" ;;
  pids)
    python3 -c "
import os, signal, sys, time
n = int(sys.argv[1]); kids = []
try:
    for _ in range(n):
        pid = os.fork()
        if pid == 0:
            time.sleep(60); os._exit(0)
        kids.append(pid)
    print('started all', n)
except OSError as e:
    print('fork failed after', len(kids), 'children:', e)
time.sleep(2)
for k in kids: os.kill(k, signal.SIGKILL)
for k in kids: os.waitpid(k, 0)
print('killed', len(kids))
" "$2" ;;
  disk)
    df -h /sandbox/work 2>&1 | tail -1
    # /dev/zero isn't in the filesystem policy; write from python instead.
    python3 -c "
import sys
n = int(sys.argv[1]); block = b'\0' * (1024 * 1024)
with open('/sandbox/work/fill', 'wb') as f:
    for i in range(n): f.write(block)
print('wrote', n, 'MiB')
" "$2" 2>&1 | tail -1
    df -h /sandbox/work 2>&1 | tail -1
    rm -f /sandbox/work/fill; echo removed ;;
  probe)
    i=0; while [ $i -lt 50 ]; do sleep 5 & i=$((i + 1)); done; wait; echo "probe: 50 processes ran"
    curl -s -m 5 -o /dev/null -w "probe: router HTTP %{http_code} in %{time_total}s\n" \
      -H "Authorization: Bearer $FLEETRING_TOKEN" -H 'content-type: application/json' \
      "https://$2/db/agent-b-memory/v3/pipeline" -d '{"requests":[{"type":"close"}]}' ;;
esac
