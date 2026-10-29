---
id: FR-046
title: Mutual TLS between nodes and clients, with a built-in certificate authority
status: todo
priority: P0
size: L
milestone: M10 Security
depends_on: [FR-029, FR-037]
adrs: [ADR-0017, ADR-0019, ADR-0025]
---

# FR-046: Mutual TLS between nodes and clients, with a built-in certificate authority

## Context

Every gRPC connection in `fleetringd` is plaintext, and a node is whoever says it is in `Join`. ADR-0025 decides that every connection is TLS 1.3 through `rustls`, that nodes and clients present certificates issued by a cluster authority, and that a node's certificate names its node ID as a URI subject alternative name, `fleetring://<cluster>/node/<id>`, so the ID a node claims in `Join` and `Heartbeat` is checked against its certificate. CockroachDB's `cockroach cert` is the model for the operator experience: one command creates the authority, one issues each node's certificate, one issues a client's. etcd and rqlite reach the same shape with external tooling. The daemon only ever needs a certificate, its key, and the trusted authority's certificate, so certificates from Vault, cert-manager, or SPIRE work unchanged.

## Scope

- **`fleetringd cert init --cluster <name>`** creates the authority: an Ed25519 key and a self-signed certificate in a directory the operator chooses; the key never goes to a node. **`cert node <id> --hosts <ip-or-dns>,…`** issues a node certificate with the node URI SAN and the hosts; **`cert client <principal>`** issues a client certificate with the principal URI SAN. Lifetimes default to 30 days for nodes and 7 for clients, settable. Use `rcgen` for issuance and `rustls` with `tokio-rustls` through tonic's `tls-ring` or `tls-aws-lc-rs` feature; check `deny.toml`.
- **Configuration:** a `[tls]` section with `ca`, `cert`, and `key` paths; the daemon refuses to start without them unless `--insecure-plaintext` is given, which logs a warning on every start and exists for the simulation harness and local experiments only. The CLI takes `--ca`, `--cert`, `--key` (and reads `FLEETRING_TLS_DIR`), and a client certificate names the principal the CLI acts as.
- **Verification both ways:** servers require a client certificate chaining to the authority; clients verify the server's certificate against the authority and its hosts. The membership service rejects a `Join`, `Heartbeat`, `Leave`, or `Drain` whose node ID differs from the certificate's URI SAN with `PERMISSION_DENIED`. Node-to-node data calls (replication, promotion, snapshots) accept only node certificates.
- **Reload without restart:** certificate, key, and authority files are re-read when they change (a watcher, or a check on each new connection, bounded to once a second); new connections use the new material, established connections keep theirs. Two authority certificates may be trusted at once so a rotation can overlap.
- **Principal extraction** as a request extension, so FR-047 can authorize on it: node ID for node certificates, principal name for client certificates, nothing for plaintext.
- **Tests:** a cluster over real TCP with issued certificates; a node with a certificate for another ID is refused on join and on heartbeat; a client without a certificate is refused; a rotated node certificate is picked up by the next connection while an open replication stream continues; an expired certificate is refused. The simulation harness runs under TLS with a generated authority so turmoil covers the same code paths.
- **Not in scope:** tokens and scopes (FR-047), the threat model and signed releases (FR-048), revocation.

## Definition of done

- [ ] A cluster started with `cert init`, three `cert node` certificates, and a `cert client` certificate runs the README walkthrough end to end with no plaintext listener, and a plaintext client is refused.
- [ ] A node presenting a certificate for a different ID cannot join, heartbeat, or replicate, and the refusal names the mismatch in the server log without printing key material.
- [ ] Certificate rotation needs no restart, and the test proves an in-flight replication stream survives it.
- [ ] The fixed-seed simulation runs under TLS and stays under its time budget; the ARM, macOS, and Windows jobs pass, including the s390x and wasm emulation jobs where the daemon is built.
- [ ] `deploy/fleetringd.toml`, the daemon README, and `docs/review.md` describe the `[tls]` section, the `cert` commands, and the plaintext escape hatch.
