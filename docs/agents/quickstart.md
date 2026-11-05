# Agent quickstart

Give an AI agent its own FleetRing databases on a laptop: a dev node, a principal scoped to the agent's names, a first database with a description, labels, and an expiry, and an MCP client that runs `fleetringd mcp`. It takes about five minutes. The [skill](SKILL.md) is what the agent reads afterwards.

## 1. Start a dev node

```sh
fleetringd dev                           # in one terminal; Ctrl-C stops it
. ~/.fleetring/env                       # in another: points the commands at it, as admin
fleetringd status                        # starts with "dev mode: one node on loopback, ..."
```

`~/.fleetring` holds the node's authority, its configuration, and an `admin` certificate with every scope ([dev mode](../../crates/fleetringd/README.md#dev-mode)). Don't give the agent that certificate.

## 2. Give the agent a principal

The agent acts as its own principal, `agent-42`, with one scope: `admin:agent-42-*`, which lets it create, use, describe, label, list, and drop databases whose names start with `agent-42-`, and nothing else. It needs a certificate naming it and a token carrying the scope; a node refuses a token whose principal differs from the certificate's.

```sh
mkdir -m 700 -p ~/.fleetring-agents/agent-42
fleetringd cert client agent-42 --dir ~/.fleetring/tls --ttl 30d
mv ~/.fleetring/tls/client.agent-42.* ~/.fleetring-agents/agent-42/
cp ~/.fleetring/tls/ca.crt ~/.fleetring-agents/agent-42/
fleetringd token issue --dir ~/.fleetring/tls --principal agent-42 --scope 'admin:agent-42-*' --ttl 30d \
  > ~/.fleetring-agents/agent-42/token
chmod 600 ~/.fleetring-agents/agent-42/token
```

The agent's files move out of `~/.fleetring/tls`, because `fleetringd dev` refuses to start from a directory holding files it didn't write. Keep the trailing `-` in the scope: `admin:agent-42*` would also cover `agent-420-memory`, another agent's database, and `token issue` warns about it. Quote the scope so the shell doesn't expand the `*`.

## 3. Try it from the shell, as the agent

```sh
export FLEETRING_TLS_DIR=~/.fleetring-agents/agent-42 FLEETRING_AS=agent-42
export FLEETRING_TOKEN=$(cat ~/.fleetring-agents/agent-42/token)

fleetringd db create agent-42-memory --label purpose=memory --label agent=42 --description "Long-term memory for agent 42.
Schema v1: facts (subject, predicate) upserts."
fleetringd db create agent-42-scratch --label purpose=scratch --label agent=42 --expires-in 2h

fleetringd db exec agent-42-memory "CREATE TABLE IF NOT EXISTS facts (subject TEXT NOT NULL, predicate TEXT NOT NULL, object TEXT NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY (subject, predicate))"
fleetringd db exec agent-42-memory "INSERT INTO facts VALUES (?, ?, ?, ?) ON CONFLICT (subject, predicate) DO UPDATE SET object = excluded.object, updated_at = excluded.updated_at" user:ana prefers "short answers" 1767225600000
fleetringd db query agent-42-memory "SELECT predicate, object FROM facts WHERE subject = ?" user:ana

fleetringd db list --prefix agent-42- --label purpose=memory
fleetringd db show agent-42-memory       # placement, description, labels, creator, expiry, schema
fleetringd db edit agent-42-scratch --expires-in 4h
fleetringd db create agent-43-memory     # refused: outside the agent's scope
fleetringd db drop agent-42-scratch --yes
```

`db show` prints the description as an indented block, marked as written by `agent-42 (you)`, and the schema with each table's row count. `db create agent-43-memory` fails with `PERMISSION_DENIED`, naming the action and the database.

## 4. Register `fleetringd mcp` with an MCP client

`fleetringd mcp` is an MCP server on stdio that acts as one principal, found as every command finds it. Most MCP clients take a JSON configuration like this one; put in your own paths and the token from `~/.fleetring-agents/agent-42/token`:

```json
{"mcpServers": {"fleetring": {"command": "fleetringd", "args": ["mcp"], "env": {"FLEETRING_CONTROL": "127.0.0.1:7400", "FLEETRING_TLS_DIR": "/home/me/.fleetring-agents/agent-42", "FLEETRING_AS": "agent-42", "FLEETRING_TOKEN": "<token>"}}}}
```

`FLEETRING_TOKEN` holds a secret, so keep the configuration file private to you (`chmod 600`), and don't commit it. Once FR-056 adds `--token-file`, the configuration names the token's file instead. The [daemon README](../../crates/fleetringd/README.md#mcp-server) describes the server's flags (`--read-only`, `--max-result-bytes`), its tools, and how it marks untrusted text.

## 5. First tool calls

Ask the agent to do the same through the tools. The calls look like this:

```json
{"tool": "db_create", "arguments": {"name": "agent-42-task-7f3a", "description": "Scratch space for task 7f3a.", "labels": {"purpose": "task", "agent": "42", "task": "7f3a"}, "expires_in": "24h"}}
{"tool": "exec", "arguments": {"database": "agent-42-memory", "sql": "INSERT INTO facts VALUES (?, ?, ?, ?) ON CONFLICT (subject, predicate) DO UPDATE SET object = excluded.object, updated_at = excluded.updated_at", "params": ["user:ana", "tone", "friendly", 1767225700000]}}
{"tool": "query", "arguments": {"database": "agent-42-memory", "sql": "SELECT predicate, object FROM facts WHERE subject = ?", "params": ["user:ana"]}}
{"tool": "db_show", "arguments": {"name": "agent-42-memory"}}
{"tool": "db_drop", "arguments": {"name": "agent-42-task-7f3a", "confirm": "agent-42-task-7f3a"}}
```

Then give the agent the [skill](SKILL.md): load it as a skill if the client supports skills, or put it in the agent's instructions. It covers when to create a database rather than a table, naming, labels, descriptions, expiry, and SQL patterns for memory, task state, and full-text search.

## Dev mode notes

- **One copy of each database.** A dev node holds one replica, its default replication factor; a fleet's default is 3, and the same commands and tools work against it.
- **Expiry still drops.** An expired database is dropped when the node next runs, even if the node was stopped when the time passed. A dev node has no archive unless you configure one, so an expired or dropped database is gone.
- **The agent gets a token, not the admin certificate.** The admin certificate in `~/.fleetring` has every scope. Give each agent its own principal and a prefix scope, and revoke a token you no longer trust with `fleetringd token revoke --token-id <id>`, the ID `token issue` printed.
