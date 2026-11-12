#!/bin/sh
# Run inside sandbox A in the background: every 0.5 s, one INSERT into A's database through the
# router, logged as "<unix time> <HTTP status> <outcome>". It keeps the placeholder it started
# with, so it shows what a provider update, detach or expiry does to a process already running.
#
#   nohup sh probe-loop.sh <router host:port> > /tmp/loop.log 2>&1 &
U="https://$1/db/${2:-agent-a-memory}/v3/pipeline"
B='{"requests":[{"type":"execute","stmt":{"sql":"INSERT INTO notes (body) VALUES (?)","args":[{"type":"text","value":"probe"}]}},{"type":"close"}]}'
while :; do
  t=$(date +%s.%N | cut -c1-14)
  s=$(curl -s -m 5 -o /tmp/probe.body -w '%{http_code}' -H "Authorization: Bearer $FLEETRING_TOKEN" \
    -H 'content-type: application/json' "$U" -d "$B")
  o=$(grep -o -E '"affected_row_count":1|"code":"[A-Z_]+"|"error":"[a-z_]+"' /tmp/probe.body | head -1)
  echo "$t $s $o"
  sleep 0.5
done
