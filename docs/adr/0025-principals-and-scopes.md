# ADR-0025: Principals and scopes: mutual TLS between nodes and prefix-scoped credentials for clients

- **Status:** Accepted (2026-10-29)
- **Date:** 2026-10-29
- **Related:** ADR-0017 (membership), ADR-0018 (leases and epochs), ADR-0019 (protocol), ADR-0024 (change capture); tickets FR-046, FR-047, FR-048

## Context

Nothing on the wire is authenticated or encrypted. A process with network access to a node can join the cluster as any node ID, acquire leases, read and write every database, and subscribe to every change stream. Epochs fence stale writers, but fencing assumes every participant is honest about who it is. Before anyone depends on FleetRing, both halves of that assumption have to be enforced: nodes must prove they are members, and clients must prove which keys they may touch.

Comparable systems converge on the same shape:

| System | Node identity | Client identity | Authorization |
|---|---|---|---|
| CockroachDB | per-node certificate from a cluster CA (`cockroach cert`), mutual TLS | client certificate per SQL user, or password | SQL grants |
| etcd | peer certificates from a CA, `--peer-client-cert-auth` | client certificate (the common name is the user) or password | roles with key-prefix permissions |
| rqlite | mutual TLS between nodes | client certificate or Basic auth | per-user permissions (`execute`, `query`, `join`, …) |
| Kafka | mutual TLS or SASL between brokers | mutual TLS or SASL SCRAM | ACLs on principals, literal or prefixed resource patterns, default deny |
| NATS | a chain of Ed25519 keys: operator, account, user | signed user JWTs; the server verifies the chain, never stores users | subject permissions in the JWT |
| Turso / libSQL | not applicable (hosted) | JWTs scoped to a group, a database, or read-only, with expiry | claims in the token |

Two patterns recur. Node identity is a certificate issued by a cluster certificate authority, verified both ways. Client identity is a credential that names a principal, and authorization is a small set of rules on that principal over a resource namespace, with prefixes and default deny. The resource namespace in FleetRing is already there: every database is a key, keys are named with prefixes by convention (`tenant:acme/…`), and cells group nodes. A scope is therefore a key prefix, and a principal's scopes say which keys it may read, write, administer, or stream.

Two constraints are FleetRing's own. First, the control plane must stay pure (ADR-0001, ADR-0020): verification happens at the transport and the request boundary, never inside `apply`, and no key material enters control state. Second, the daemon must stay a single static binary with no build-time toolchain beyond Rust, which rules out OpenSSL and favours `rustls`.

## Decision

1. **Nodes authenticate with mutual TLS, always.** Every connection between nodes, and from a client to a node, is TLS 1.3 through `rustls`. A node presents a certificate issued by the cluster's certificate authority, and both ends verify the other's certificate against that authority. A node's certificate carries its node ID as a URI subject alternative name, `fleetring://<cluster>/node/<id>`, in the SPIFFE style, and a node is refused if the ID it claims in `Join` or `Heartbeat` differs from the one in its certificate. There is no plaintext mode once a cluster has a certificate authority; the only exception is the simulation harness and tests, which use the same code paths with a test authority.

2. **A cluster certificate authority bootstrapped by the operator.** `fleetringd cert init` creates the authority (an Ed25519 key, kept off the nodes), `fleetringd cert node <id> --hosts …` issues a node certificate, and `fleetringd cert client <principal>` issues a client certificate. This is CockroachDB's model, chosen because it works with no external infrastructure and because an operator who already runs a certificate authority (Vault, cert-manager, SPIRE) can hand the daemon certificates from it instead; the daemon only ever needs a certificate, its key, and the trusted authority's certificate. Certificates are short-lived by default (30 days for nodes, 7 for clients) and reloaded from disk without a restart; a changed authority certificate also reloads, so rotation never needs a restart. The authority's private key is the one secret that must never be on a node.

3. **Clients authenticate as principals, two ways.** A client certificate issued by the cluster authority names its principal in the URI subject alternative name, `fleetring://<cluster>/principal/<name>`. Or a bearer token: a signed, expiring token that names the principal and carries its scopes, verified with the authority's public key, so a node checks it without a lookup. Tokens are issued by `fleetringd token issue --principal <name> --scope … --ttl …` and are what applications, the router (E7), and the MCP server use; certificates are for operators and co-located workers. The token format is a Ed25519-signed JWT with registered claims only (issuer, subject, expiry, not-before, a key ID) plus one FleetRing claim for scopes, so standard libraries can mint and inspect them, and later delegated tokens (Authem-style) are a second issuer key the authority trusts.

4. **Scopes are prefix rules on keys, with four actions and default deny.** A scope is `<action>:<pattern>`, where the action is `read`, `write`, `admin`, or `stream`, and the pattern is an exact key or a prefix ending in `*`, the same matcher the directory rules and change sinks use. `write` implies `read`; `admin` (create, drop, change durability, drain) implies both; `stream` is separate so that a change consumer can't read tables. A request is allowed if any scope of the principal allows its action on its key; otherwise it is refused with `PERMISSION_DENIED` before any state is touched. Control-plane operations that have no key (`Join`, `Leave`, `Drain`, membership queries) are reserved for nodes and for principals with the `cluster` scope. Nodes act with implicit full scope towards each other; node-to-node calls are never authorized by key.

5. **Enforcement sits at the request boundary, in one place per service.** An interceptor extracts the principal from the connection (certificate) or the request (token), and each RPC handler checks the scope for its key before calling into the daemon. The pure control crate and the replication layer receive requests that are already authorized; they stay unaware of principals. Decisions are logged with the principal, the action, and the key, never with the credential.

6. **A written threat model, in `SECURITY.md`.** What a compromised client, node, or network can and cannot do under this design, kept current with every security ticket, and release artifacts that can be verified: checksums signed with keyless Sigstore from the release workflow and GitHub build attestations, so an operator can check that a binary came from this repository's release workflow.

## Alternatives considered

- **Plaintext with network-level controls (early rqlite).** Simple, and what most self-hosted SQLite tools ship with. It makes every security property somebody else's firewall rule, and it would have to be replaced before the router, workers, or multi-tenant use. Rejected.
- **Pre-shared cluster secret (gossip keys, as in Serf and Consul's gossip layer).** One symmetric key authenticates members. Cheap, but it gives no per-node identity, so a leaked key is a leaked cluster, and rotation is a coordinated restart. Rejected for nodes; it is roughly what a token is for clients, where the token names a principal and expires.
- **Password authentication for clients (etcd, rqlite Basic auth).** Needs a user store the control plane would have to replicate, and passwords travel on every request. Tokens are verified offline and carry their scopes. Rejected.
- **An external authority only (SPIRE, cert-manager, Vault).** The right answer in many deployments, and the design accepts their certificates unchanged, but requiring one would make the first cluster an infrastructure project. The built-in authority is the floor, not the ceiling.
- **Authorization inside the control plane (`apply` checks the principal).** It would put policy into replicated state and credentials into the command log. Enforcement at the boundary keeps the control crate pure and the audit log free of secrets.
- **A general policy language (OPA, Cedar).** Four actions over prefixes cover every operation the daemon has. A policy engine can be added as an alternative scope source later without changing the enforcement point.

## Consequences

- Every connection carries a certificate, including the CLI's, so the walkthrough in the daemon README gains a `fleetringd cert init` step before `fleetringd` starts, and the single-node quick start issues its own certificates. The simulation harness and the integration tests run with a generated test authority on every run.
- The authority's key is the operator's responsibility. Losing it means no new nodes or clients until the cluster is re-keyed, which the design supports by trusting two authorities during a rotation.
- Node IDs become unforgeable, which the keyed-lease and worker designs (FR-042, FR-043) rely on, and cells plus scopes give tenant isolation without a separate subsystem.
- The router (E7) is one more client of this model: it terminates its own client protocol, then acts towards nodes as a principal whose scopes are the union of what it serves, or forwards the caller's token unchanged. Nothing in this decision depends on which.
- Tokens carry scopes, so revoking one before expiry needs a denylist on nodes (by key ID or token ID), published through the control plane as an additive command. Short lifetimes keep that list small.
- Signed heartbeats or commands are unnecessary: the mutual TLS session authenticates the peer for every message on it.
