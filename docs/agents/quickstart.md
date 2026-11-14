# Agent quickstart

Give an AI agent its own FleetRing databases on a laptop with one command: the agent's MCP client runs `fleetring mcp --agent 42`, which starts a dev node if none runs, gives the agent its own principal scoped to its own databases, and serves. The [skill](SKILL.md) is what the agent reads afterwards.

> **Trusted local development.** Agents on this machine can read each other's credentials. FleetRing tokens restrict FleetRing requests, not an agent's process: agents running as the same OS user can read each other's identity files under `~/.fleetring/agents/` and open SQLite files directly, so separate identities and private directories don't isolate them from each other. Secure separation between agents needs an isolated agent launcher and credential broker, which [ADR-0044](../adr/0044-identity-delegation-and-forking-for-agents.md) ("Runtime isolation and the agent launcher") designs, and which isn't part of 0.8. A [delegated agent](#5-delegate-to-a-child) doesn't change this: it runs with its root agent's certificate, on the same account.

## 1. Register the MCP server

For Claude Code:

```sh
claude mcp add fleetring -- fleetring mcp --agent 42 --context dev
```

For another MCP client, print its configuration and paste it in:

```sh
fleetring agent add 42 --print-config json     # or cursor; `--print-config claude` prints the line above
```

```json
{"mcpServers": {"fleetring": {"command": "/usr/local/bin/fleetring", "args": ["mcp", "--agent", "42", "--context", "dev"]}}}
```

The configuration holds no secret, only the command: `mcp --agent` finds its own credentials. The path is absolute, because GUI MCP clients often lack your shell's `PATH`, and the context is pinned, so that a later `fleetring switch` doesn't move the agent to another fleet. `fleetring agent add 42 --install claude` runs the `claude mcp add` line for you.

`agent add` mints the identity at once, and starts the dev node if none runs; with only the `claude mcp add` line, the first start does both. Add `--read-only` to `agent add` for an agent that only reads: its scopes then read where they would administer (`read:agent-42-*` rather than `admin:agent-42-*`), and the server offers only `db_list`, `db_show`, `query`, and `changes`.

## 2. What the first start does

When the MCP client starts `fleetring mcp --agent 42` and the `dev` context is selected, or none is and nothing names a connection:

1. **It acquires a node.** If no dev node runs from `~/.fleetring`, it starts one in the background, as `fleetring dev -d` does, writes the `dev` context, and makes it current unless another context is. The node outlives the MCP server; `fleetring dev stop` stops it, and `fleetring dev logs` shows its log.
2. **It acquires an identity.** If `~/.fleetring/agents/42/` holds none, it checks that `~/.fleetring` is a dev directory (its configuration says `dev = true` and keeps every address on loopback, and its authority is for the fleet `dev`), then mints one from the dev authority:
   - the principal `agent-42`, with a client certificate valid for a year;
   - a token scoped `admin:agent-42-*` and `stream:agent-42-*`, valid for 30 days, which lets the agent create, use, describe, label, list, follow the changes of, fork, and drop databases whose names start with `agent-42-`, and nothing else. Since 0.8, once every node reports the `delegation` feature, as a 0.8 dev node does, it also holds `admin:agent-42.*`, `stream:agent-42.*`, `grant:agent-42-*`, and `grant:agent-42.*`, so that the agent can [delegate](#5-delegate-to-a-child) to children, whose databases are `agent-42.<segment>-…`;
   - in `~/.fleetring/agents/42/`, a directory only you can read (`0700`): `ca.crt`, `client.agent-42.crt` and `client.agent-42.key`, `token`, and `identity.toml`, which holds no secret. Every file is `0600`.
3. **It serves** MCP on stdin and stdout as `agent-42`, and says on stderr whom it serves as, on which context, and when the certificate and token expire.

Later starts reuse the node and the identity and regenerate nothing. In dev mode only, a start renews the token once fewer than 7 days remain, and the certificate once fewer than 30 do, and revokes the old token. A session that outlives its token gets a tool error that says to restart the MCP server.

Keep the trailing `-` in the scope: `admin:agent-42*` would also cover `agent-420-memory`, another agent's database, which is why agent IDs are lowercase letters, digits, and `_`, never `-` or `.`. `fleetring agent show 42` prints the identity, its scopes and files, and when the token must be renewed by; `fleetring agent list` lists every agent of the context.

An identity minted before 0.8 holds `admin:agent-42-*` alone, without `stream` or `grant`, and a start doesn't change it: it says so on stderr, `changes` is refused, and the agent can't delegate. `fleetring agent renew 42` reissues its credentials with today's defaults. It widens only an identity that holds exactly an older release's default; scopes you chose with `--scope` stay as they are.

## 3. Try it from the shell, as the agent

`--agent 42` acts as the agent's identity on any command, so you see what the agent sees. It only uses an identity that exists; only `mcp --agent`, `agent add`, and `agent renew` mint.

```sh
fleetring db create agent-42-memory --agent 42 --label purpose=memory --label agent=42 --description "Long-term memory for agent 42.
Schema v1: facts (subject, predicate) upserts."
fleetring db create agent-42-scratch --agent 42 --label purpose=scratch --label agent=42 --expires-in 2h

fleetring db exec agent-42-memory --agent 42 "CREATE TABLE IF NOT EXISTS facts (subject TEXT NOT NULL, predicate TEXT NOT NULL, object TEXT NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY (subject, predicate))"
fleetring db exec agent-42-memory --agent 42 "INSERT INTO facts VALUES (?, ?, ?, ?) ON CONFLICT (subject, predicate) DO UPDATE SET object = excluded.object, updated_at = excluded.updated_at" user:ana prefers "short answers" 1767225600000
fleetring db query agent-42-memory --agent 42 "SELECT predicate, object FROM facts WHERE subject = ?" user:ana

fleetring db list --agent 42 --prefix agent-42- --label purpose=memory
fleetring db show agent-42-memory --agent 42      # placement, description, labels, creator, expiry, schema
fleetring db edit agent-42-scratch --agent 42 --expires-in 4h
fleetring db watch agent-42-memory --agent 42 -o jsonl   # each write's rows as it commits, until Ctrl-C
fleetring db fork agent-42-memory agent-42-memory-trial --agent 42 --expires-in 2h   # an independent copy, once it is ready
fleetring db create agent-43-memory --agent 42    # refused: outside the agent's scope
fleetring db drop agent-42-scratch --agent 42 --yes
```

Without `--agent`, the commands act as the `dev` context's `admin`. `FLEETRING_AGENT=42` does the same as `--agent 42` for a shell. `db show` prints the description as an indented block, marked as written by `agent-42 (you)`, and the schema with each table's row count. `db create agent-43-memory` fails with `PERMISSION_DENIED`, naming the action and the database.

## 4. First tool calls

Ask the agent to do the same through the tools. The calls look like this:

```json
{"tool": "db_create", "arguments": {"name": "agent-42-task-7f3a", "description": "Scratch space for task 7f3a.", "labels": {"purpose": "task", "agent": "42", "task": "7f3a"}, "expires_in": "24h"}}
{"tool": "exec", "arguments": {"database": "agent-42-memory", "sql": "INSERT INTO facts VALUES (?, ?, ?, ?) ON CONFLICT (subject, predicate) DO UPDATE SET object = excluded.object, updated_at = excluded.updated_at", "params": ["user:ana", "tone", "friendly", 1767225700000]}}
{"tool": "query", "arguments": {"database": "agent-42-memory", "sql": "SELECT predicate, object FROM facts WHERE subject = ?", "params": ["user:ana"]}}
{"tool": "db_show", "arguments": {"name": "agent-42-memory"}}
{"tool": "db_drop", "arguments": {"name": "agent-42-task-7f3a", "confirm": "agent-42-task-7f3a"}}
```

To follow what is written to a database, the agent calls `changes` once without `after`, which returns `next_after`, a position, then again and again with `after` set to the last `next_after` it got. Each call returns the transactions after that position, at most `limit` (20 by default, at most 100), as soon as it has them, and waits at most 2 seconds when there are none:

```json
{"tool": "changes", "arguments": {"database": "agent-42-memory"}}
{"tool": "changes", "arguments": {"database": "agent-42-memory", "after": "00000000a3f19c0e5b7d21440000000000003068"}}
```

Nothing holds the agent's place between calls, so a dev node releases changes within about 64 writes once its change sink has them; an agent that falls further behind gets `out_of_range` with the oldest position held, and reads the current state with `query` before it continues.

Then give the agent the [skill](SKILL.md): load it as a skill if the client supports skills, or put it in the agent's instructions. It covers when to create a database rather than a table, naming, labels, descriptions, expiry, following changes, and SQL patterns for memory, task state, and full-text search. `--max-result-bytes <n>` after `mcp` caps each result's JSON (256 KiB by default, from 64 KiB to 4 MiB); the [daemon README](../../crates/fleetringd/README.md#mcp-server) describes the tools, their results and errors, and how the server marks untrusted text.

## 5. Delegate to a child

An agent that spawns workers can give each one a credential of its own, narrower and shorter-lived than its own, without the authority's key (ADR-0047). The harness that starts the agents runs it, never the model: no MCP tool delegates, revokes, or returns a token. The child of `42` is `42.c7`, whose principal is `agent-42.c7` and whose databases are `agent-42.c7-…`.

```sh
fleetring agent delegate 42.c7
```

```text
operation 32mf1mmqcq5ext5hhqehsgw2ff: if this command is interrupted, run it again to resend it
agent: 42.c7
principal: agent-42.c7
parent: 42
root: 42
owner: admin
scopes: admin:agent-42.c7-*, stream:agent-42.c7-*
token expires: 2026-11-14T04:06:11Z
delegation: 0EOHGTaQ5NsVd7oh1IPLmg
operation: 32mf1mmqcq5ext5hhqehsgw2ff
trusted local development: agents on this machine can read each other's credentials
```

The control plane checks agent 42's token, records the delegation, and signs the child's token, which `agent delegate` writes to `~/.fleetring/agents/42.c7/` with its `identity.toml`. The child gets no certificate: it connects with agent 42's. Its token lasts 2 hours, or less if the parent's ends sooner; `--ttl` asks for 1 minute to 24 hours. If the command is interrupted, or exits 4 because the control plane stayed unavailable, run it again: it resends under the same operation ID and never records a second delegation.

**Act as the child,** from the shell or as its MCP server:

```sh
fleetring db create agent-42.c7-scratch --agent 42.c7 --expires-in 2h
fleetring db query agent-42-memory --agent 42.c7 "SELECT 1"  # refused: the parent's database
claude mcp add fleetring-c7 -- fleetring mcp --agent 42.c7 --context dev
```

`mcp --agent 42.c7` serves with the child's token and never mints or renews it; at start it names the parent, the owner, and when the token expires.

**What a child can do.** By default, its parent's own actions, `admin` and `stream` (`read` and `stream` under a read-only parent), on `agent-42.c7-*`, and nothing else: not its parent's databases, not its siblings', and no children of its own. Ask for more with `--scope`, repeated, each covered whole by the parent's scopes and its `grant`, and inside agent 42's databases:

```sh
fleetring agent delegate 42.w1 --scope 'admin:agent-42.w1-*' --scope 'fork:agent-42-memory' \
    --scope 'admin:agent-42.w1.*' --scope 'grant:agent-42.w1.*' --ttl 1h
fleetring db fork agent-42-memory agent-42.w1-memory --agent 42.w1   # it may fork the parent's memory
fleetring agent delegate 42.w1.r1                                    # and delegate in turn
```

A request outside the parent's scopes or its `grant`, or outside agent 42's databases, is refused, naming the first scope that doesn't fit (exit 3); so is a `--ttl` past the parent's token or over 24 hours (exit 1). A child is at most 8 levels below its root, and a root's lineage holds at most 256 live delegations.

**See them.** `fleetring agent list` shows each child after its parent, in a `PARENT` column, and `fleetring agent show 42.c7` adds its root, owner, delegation ID, and the ID of the issuer key that signed its token.

**Renew and remove.**

- `fleetring agent renew 42.c7` delegates again as the parent, with the scopes the child records, then revokes the old token. Renewing an agent never renews the agents below it, and revoking its old token ends them: renew each in turn.
- `fleetring agent remove 42.c7` revokes `agent-42.c7` as a principal, as its nearest ancestor with an identity here, which ends it and every agent below it at once, and deletes its directory. Directories of agents below it stay, unusable: remove each with `--no-revoke`.
- `fleetring agent remove 42` removes the root agent: it revokes its own principal and its own token ID, ending every agent below it, without the `fleet` scope. It prints when a new token for agent 42 may be minted, 30 seconds later; a token minted sooner is refused for 30 days, so wait for that time before `agent add 42`, or run `agent renew 42` after it.
- From the shell, as an agent, `fleetring token revoke --principal agent-42.c7 --agent 42` and `--token-id <delegation ID> --agent 42` revoke under the same rule: an agent revokes itself, the token it presented, or what it or its children delegated, and nothing else. A child can't revoke its parent or a sibling.

**Before every node runs 0.8,** `agent delegate` is refused, naming the node that lacks the `delegation` feature, and `agent add` mints a token that can't delegate, saying so. Once every node runs 0.8, `fleetring agent renew 42` reissues an agent's credentials with the defaults that delegate. The first delegation, and the first principal revocation or revocation by an agent, enables the feature for the fleet, and from then on no node can roll back to 0.7.

## Another fleet

Minting is dev-mode-only: it needs the dev authority on this machine, on loopback. Against any other context, `--agent 42` uses an identity that exists, and fails, minting nothing, with the commands that create one. Mint it where the fleet's authority is, with a context for the fleet:

```sh
fleetring agent add 42 --context prod --authority /secure/prod-ca      # the directory with ca.crt and ca.key
```

It writes the identity to `~/.local/share/fleetring/agents/prod/42/` (`$XDG_DATA_HOME/fleetring/agents/<context>/`), with the certificate valid for 30 days (`--ttl` sets it) and the token as long, at most 30 days. Copy that directory to the same path on the agent's machine, keeping it private, and register `fleetring mcp --agent 42 --context prod`. Nothing renews it by itself there: run `fleetring agent renew 42 --context prod --authority <dir>` before the token expires, and copy the directory again. `fleetring agent remove 42 --context prod` revokes the token and deletes the directory.

Delegation needs a delegation issuer on each control node, which dev mode makes for itself. Elsewhere, where the authority is, run `fleetring cert issuer --name <control node ID> --dir /secure/prod-ca`, copy the two files it writes to the control node, and name them in its configuration's `[control] issuer_cert` and `issuer_key`; the node reloads them without a restart. Until then `agent delegate` is refused with the same command as a hint. A child delegated against `prod` runs on the machine that holds its root's directory, since it connects with the root's certificate; `agent renew <child-id>` and `agent remove <child-id>` need no `--authority`, as the parent asks the control plane.

## Dev mode notes

- **One copy of each database.** A dev node holds one replica, its default replication factor; a fleet's default is 3, and the same commands and tools work against it.
- **Expiry still drops.** An expired database is dropped when the node next runs, even if the node was stopped when the time passed. A dev node has no archive unless you configure one, so an expired or dropped database is gone.
- **The agent gets its own identity, never the admin certificate.** The `admin` certificate in `~/.fleetring/tls` has every scope. `fleetring agent remove 42` revokes the agent's token and deletes its directory; `fleetring agent renew 42` reissues both credentials now and revokes the old token.
