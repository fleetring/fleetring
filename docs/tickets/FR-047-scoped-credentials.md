---
id: FR-047
title: Principals and scopes: tokens and prefix rules enforced at every RPC
status: in-progress
priority: P0
size: L
milestone: M10 Security
depends_on: [FR-046]
adrs: [ADR-0025]
---

# FR-047: Principals and scopes: tokens and prefix rules enforced at every RPC

## Context

With FR-046 every caller is identified: a node by its ID, a client by the principal in its certificate. Identity alone still lets any principal read and write every database. ADR-0025 decides that authorization is a set of scopes per principal, `<action>:<pattern>` with the actions `read`, `write`, `admin`, and `stream` over exact keys or prefixes ending in `*`, default deny, checked at the request boundary before any state is touched. It also decides a second credential: a signed, expiring bearer token that names the principal and carries its scopes, verified with the authority's public key with no lookup, for applications, the router, and the MCP server. Kafka's prefixed ACLs with default deny and NATS's signed user JWTs are the precedents; Turso's scoped tokens show the shape clients expect.

## Scope

- **Scopes:** `Scope { action, pattern }` parsed from `read:tenant:acme/*`, `admin:*`, `stream:ledger`; `write` implies `read`, `admin` implies both, `stream` is separate; `cluster` is the scope for key-less control operations. Reuse the matcher the change sinks use. A `Scopes::allows(action, key) -> bool`-shaped check with unit tests over the implication rules; keep the policy pure and in one module.
- **Where scopes come from:** a client certificate's principal maps to scopes through a `[[principal]]` list in the daemon configuration (`name`, `scopes`), identical on every node, validated at start. A token carries its own scopes in one claim. Node certificates get implicit full scope towards nodes and `cluster`.
- **Tokens:** Ed25519-signed JWTs with `iss` (the cluster), `sub` (the principal), `exp`, `nbf`, `kid`, and one claim `fleetring.scopes: ["…"]`. Issued by `fleetringd token issue --principal <name> --scope <scope>… --ttl <duration>` using the authority key (off-node), verified on nodes with the authority's public key; a second issuer key may be trusted for delegated tokens later. Sent as `authorization: Bearer <token>` metadata. A client certificate and a token on the same request must name the same principal.
- **Enforcement:** one interceptor per service that resolves the principal (from FR-046's extension or the token) and a per-RPC check on the request's key with the action the RPC needs; `PERMISSION_DENIED` with a message naming the action and key, never the scopes the caller has. `WatchLeases`, `NodeStatus`, `Verify`, and `Locate` need `read` on the key or `cluster`; `Execute` needs `write` (or `read` for a statement the adapter classifies as read-only); `CreateDatabase`, `DropDatabase`, `ChangeDurability`, `TransferLease` need `admin`; change sink positions in `NodeStatus` need `stream`. Node-to-node RPCs are never authorized by key.
- **Revocation:** a denylist of token IDs or key IDs published through an additive control command (`RevokeToken`), applied on nodes before verification; short lifetimes keep it small.
- **Audit:** one structured log line per denied request and, at debug, per allowed one, with principal, action, key, and RPC, never the credential.
- **Tests:** scope algebra; a principal with `read:tenant:*` can read `tenant:acme` and not write it or read `ledger`; `admin:*` can create and drop; an expired, a tampered, and a revoked token are refused; the CLI with a client certificate acts as that principal; the router-shaped case where a token is forwarded unchanged.
- **Not in scope:** a policy language, per-table permissions, delegated token issuance (Authem).

## Definition of done

- [ ] Every data and control RPC is covered by an explicit scope requirement listed in the proto comments and in `docs/spec/` or the daemon README, and a test enumerates the RPCs against that table so a new RPC without a requirement fails the build.
- [ ] A principal limited to `read:tenant:acme/*` can query that database and is refused on write, on another prefix, and on every admin and cluster operation, through both a client certificate and a token.
- [ ] Token verification needs no network or control round trip, and a revoked token is refused on every node within one heartbeat interval of the revocation command.
- [ ] `deploy/fleetringd.toml` documents `[[principal]]`, the README walkthrough issues a token for the application step, and `SECURITY.md` links the scope table.

## Progress

- 2026-11-02: Started on top of FR-046.
  - **Scope policy (`fleetringd::auth`, pure):** `Action` (`read`, `write`, `admin`, `stream`) with `Action::covers` encoding the implications (write covers read, admin covers both, stream stands alone); `Scope` parsed from `<action>:<pattern>` (split at the first `:`, so keys may hold colons) or the bare word `cluster`, the pattern being the change sinks' `DatabaseMatcher`; `Scopes::allows(Target)` where `Target` is `Key(action, &key)` or `Cluster`; default deny. `admin:*` does not imply `cluster`, and `cluster` grants no key. Unit tests cover every implication, prefix versus exact patterns, parsing and printing, and JSON round trips.
  - **Revocation (`fleetring-control`, additive):** `Action::RevokeToken { revoked: Revoked::Token(TokenId) | Revoked::Key(KeyId), until }` puts an ID on `State::revocations` until `until` (the token's expiry, or `Timestamp::MAX` for a key); revoking again keeps the later time; one whose `until` has passed is rejected as `RevocationPassed`; ticks and revocations forget the entries the time has passed. The state serializes the list only when it isn't empty, so earlier snapshots and logs are unchanged. The replay generator now emits revocations and `the_generated_log_exercises_every_kind_of_change` requires them.
  - **Tokens (`fleetringd::auth::token`):** compact JWS, header `{"alg":"EdDSA","typ":"JWT","kid"}`, claims `iss`, `sub`, `jti`, `iat`, `nbf`, `exp`, and `fleetring.scopes`. `TokenSigner` (from the authority's Ed25519 PKCS#8 key) issues; `TokenVerifier` (the cluster name and one or more trusted public keys, so rotations overlap and a delegated issuer is one more key) verifies with no I/O, in the order size (8 KiB cap), header, trusted and unrevoked key, signature, then claims, issuer, validity window, and the token's own revocation. Only `EdDSA` is accepted, unknown header fields (`crit` included) and unknown claims are refused rather than ignored, `exp` is strict and `nbf` allows 30 s of clock skew. The key ID is the first 12 bytes of the SHA-256 of the raw public key, base64url. `BearerToken`'s `Debug` hides it and errors never quote it. Built on `ring` (already in the tree through rustls in FR-046) and `base64` (already in the tree through tonic) with `serde_json`, so no new crate enters the lock file; `jsonwebtoken` would have added its own crypto and PEM stacks for a format this small.
