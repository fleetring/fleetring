# Agent quickstart

Give an AI agent its own FleetRing databases on a laptop: a dev node, a principal scoped to the agent's names, a first database with a description, labels, and an expiry, and an MCP client that runs `fleetring mcp`. It takes about five minutes. The [skill](SKILL.md) is what the agent reads afterwards.

## 1. Start a dev node

```sh
fleetring dev                           # in one terminal; Ctrl-C stops it
fleetring status                        # in another, through the `dev` context, as admin: "dev mode: one node on loopback, ..."
```

`fleetring dev` writes a context named `dev` that points the commands at the node as `admin`, and makes it current unless another context is; its banner says which, and `fleetring switch dev` selects it. `~/.fleetring` holds the node's authority, its configuration, and an `admin` certificate with every scope ([dev mode](../../crates/fleetringd/README.md#dev-mode)). Don't give the agent that certificate.

## 2. Give the agent a principal

The agent acts as its own principal, `agent-42`, with one scope: `admin:agent-42-*`, which lets it create, use, describe, label, list, and drop databases whose names start with `agent-42-`, and nothing else. It needs a certificate naming it and a token carrying the scope; a node refuses a token whose principal differs from the certificate's.

```sh
mkdir -m 700 -p ~/.fleetring-agents/agent-42
fleetring cert client agent-42 --dir ~/.fleetring/tls --ttl 30d
mv ~/.fleetring/tls/client.agent-42.* ~/.fleetring-agents/agent-42/
cp ~/.fleetring/tls/ca.crt ~/.fleetring-agents/agent-42/
fleetring token issue --dir ~/.fleetring/tls --principal agent-42 --scope 'admin:agent-42-*' --ttl 30d \
  > ~/.fleetring-agents/agent-42/token
chmod 600 ~/.fleetring-agents/agent-42/token
```

The agent's files move out of `~/.fleetring/tls`, because `fleetring dev` refuses to start from a directory holding files it didn't write. Keep the trailing `-` in the scope: `admin:agent-42*` would also cover `agent-420-memory`, another agent's database, and `token issue` warns about it. Quote the scope so the shell doesn't expand the `*`.

## 3. Try it from the shell, as the agent

```sh
fleetring context add agent-42 --control 127.0.0.1:7400 --tls-dir ~/.fleetring-agents/agent-42 \
  --as agent-42 --token-file ~/.fleetring-agents/agent-42/token
fleetring switch agent-42               # the commands below act as agent-42

fleetring db create agent-42-memory --label purpose=memory --label agent=42 --description "Long-term memory for agent 42.
Schema v1: facts (subject, predicate) upserts."
fleetring db create agent-42-scratch --label purpose=scratch --label agent=42 --expires-in 2h

fleetring db exec agent-42-memory "CREATE TABLE IF NOT EXISTS facts (subject TEXT NOT NULL, predicate TEXT NOT NULL, object TEXT NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY (subject, predicate))"
fleetring db exec agent-42-memory "INSERT INTO facts VALUES (?, ?, ?, ?) ON CONFLICT (subject, predicate) DO UPDATE SET object = excluded.object, updated_at = excluded.updated_at" user:ana prefers "short answers" 1767225600000
fleetring db query agent-42-memory "SELECT predicate, object FROM facts WHERE subject = ?" user:ana

fleetring db list --prefix agent-42- --label purpose=memory
fleetring db show agent-42-memory       # placement, description, labels, creator, expiry, schema
fleetring db edit agent-42-scratch --expires-in 4h
fleetring db create agent-43-memory     # refused: outside the agent's scope
fleetring db drop agent-42-scratch --yes
```

`fleetring switch dev` (or `switch -`) goes back to acting as admin. The context stores the token file's path, never the token. `db show` prints the description as an indented block, marked as written by `agent-42 (you)`, and the schema with each table's row count. `db create agent-43-memory` fails with `PERMISSION_DENIED`, naming the action and the database.

## 4. Register `fleetring mcp` with an MCP client

`fleetring mcp` is an MCP server on stdio that acts as one principal, found as every command finds it. Most MCP clients take a JSON configuration like this one; put in your own paths and the token from `~/.fleetring-agents/agent-42/token`:

```json
{"mcpServers": {"fleetring": {"command": "fleetring", "args": ["mcp"], "env": {"FLEETRING_CONTROL": "127.0.0.1:7400", "FLEETRING_TLS_DIR": "/home/me/.fleetring-agents/agent-42", "FLEETRING_AS": "agent-42", "FLEETRING_TOKEN": "<token>"}}}}
```

The agent needs both its own client certificate, which `FLEETRING_TLS_DIR` and `FLEETRING_AS` pick, and the token. `FLEETRING_TOKEN` holds a secret, so keep the configuration file private to you (`chmod 600`), and don't commit it. Add `"--read-only"` to `args` for an agent that only reads: the server then offers only `db_list`, `db_show`, and `query`. `--max-result-bytes <n>` caps each result's JSON (256 KiB by default, from 64 KiB to 4 MiB). The [daemon README](../../crates/fleetringd/README.md#mcp-server) describes the tools, their results and errors, and how the server marks untrusted text.

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
- **The agent gets a token, not the admin certificate.** The admin certificate in `~/.fleetring` has every scope. Give each agent its own principal and a prefix scope, and revoke a token you no longer trust with `fleetring token revoke --token-id <id>`, the ID `token issue` printed.
