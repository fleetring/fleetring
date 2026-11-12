#!/bin/sh
# Run inside sandbox A: Hrana reads and writes to A's own database through the router, with
# only the placeholder in A's environment. The supervisor resolves it on the router endpoint.
#
#   sh own-work.sh <router host:port> [database]
set -u
U="https://$1/db/${2:-agent-a-memory}/v3/pipeline"
p() {
  curl -sS -H "Authorization: Bearer $FLEETRING_TOKEN" -H 'content-type: application/json' \
    "$U" -d "$1" -w '\nHTTP %{http_code}\n'
}
echo "placeholder: $FLEETRING_TOKEN"
p '{"requests":[{"type":"execute","stmt":{"sql":"CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)"}},{"type":"close"}]}'
p '{"requests":[{"type":"execute","stmt":{"sql":"INSERT INTO notes (body) VALUES (?)","args":[{"type":"text","value":"written from sandbox A"}]}},{"type":"close"}]}'
p '{"requests":[{"type":"execute","stmt":{"sql":"SELECT id, body FROM notes"}},{"type":"close"}]}'
