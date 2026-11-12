#!/bin/bash
# DNS from inside A, on Docker, with tcpdump in A's supervisor's network namespace (the only
# namespace with a network): which names get answers, and whether any query for the agent's
# names leaves. A query sent from that namespace itself is the positive control.
# Writes results/dns.txt.
set -u
. "$(dirname "$0")/lib.sh"
OUT="$WORK/raw/dns.txt"
osh openshell gateway select fr108 > /dev/null
exec > "$OUT" 2>&1
SUP=$(docker ps --format '{{.Names}}' | grep -E '^openshell-default--agent-a-.*-supervisor$')
docker rm -f fr108-tcpdump > /dev/null 2>&1
docker run -d --name fr108-tcpdump --net "container:$SUP" alpine:3 sh -c \
  "apk add -q tcpdump > /dev/null 2>&1 && echo ready && timeout 45 tcpdump -n -l -i any 'not port 17670'" > /dev/null
until docker logs fr108-tcpdump 2>&1 | grep -q ready; do sleep 1; done; sleep 2

step "queries from A (names unique to this run)"
N=$(date +%s)
ax agent-a "for n in a$N-fr108.example.org b$N-fr108.example.net; do printf '%s -> ' \$n; getent hosts \$n || echo 'no answer'; done; echo 'TXT:'; dig +short +time=2 +tries=1 TXT t$N-fr108.example.org; echo 'MX:'; dig +short +time=2 +tries=1 MX m$N-fr108.example.org; dig +time=2 +tries=1 @1.1.1.1 d$N-fr108.example.org 2>&1 | tail -2; dig +time=2 +tries=1 @8.8.8.8 +tcp d$N-fr108.example.org 2>&1 | tail -2"

step "positive control: a query sent from the supervisor's namespace itself"
docker exec fr108-tcpdump nslookup "control$N-fr108.example.org" > /dev/null 2>&1; sleep 3

step "tcpdump in the supervisor's namespace (every packet except the gateway connection)"
docker logs fr108-tcpdump 2>&1 | grep -v -E '^ready|listening on|verbose output|promiscuous'
echo "packets naming the agent's queries: $(docker logs fr108-tcpdump 2>&1 | grep -c -E "[abtmd]$N-fr108")"
echo "packets naming the control query:   $(docker logs fr108-tcpdump 2>&1 | grep -c "control$N-fr108")"
docker rm -f fr108-tcpdump > /dev/null

scrub < "$OUT" > "$RES/dns.txt"
