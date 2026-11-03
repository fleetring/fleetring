# Security policy

## Supported versions

FleetRing is in early development. Security fixes go into the latest release of the `fleetring` crate.

## Authentication and authorization

Every `fleetringd` connection is mutual TLS with certificates from the cluster's authority, and every request is authorized against the caller's scopes before it touches any state (ADR-0025). What each RPC needs, and how principals get scopes from `[[principal]]` entries or bearer tokens, is in the daemon README's [scope table](crates/fleetringd/README.md#principals-and-scopes). A node's certificate acts on that node only (ADR-0026); the threat model below says what that does and doesn't cover.

## Threat model

This section describes `fleetringd` as built today. Every security ticket updates it; a property that is decided but not yet built is marked *planned* with its ticket.

### Assets

- **Database contents:** each replica's SQLite files under its `data_dir`, unencrypted.
- **The replication log and its hash chain:** each replica's log, with one BLAKE3 hash per transaction that commits every byte before it (ADR-0016 point 12).
- **The control log:** the control node's command log and snapshot (one SQLite database): membership, databases, epochs, leases, and token revocations. It holds no key material.
- **The authority key** (`ca.key`): issues every node and client certificate and signs every token. It belongs on no node.
- **Node and client keys:** the private key of each node's and each client's certificate.
- **Tokens:** bearer credentials that name a principal and its scopes; one hour by default, 30 days at most.
- **Release artifacts:** the `fleetringd` archives, `install.sh`, `SHA256SUMS`, and the container image.

### Actors

| Actor | What the design prevents | What it detects | What it doesn't cover |
|---|---|---|---|
| **Network attacker** | Every connection is TLS 1.3 through `rustls`, and both ends verify the other's certificate against the trusted authorities, so the attacker can't read, alter, or inject traffic, or connect at all without a certificate. A certificate must name this cluster, and a node can't claim another node's ID in `Join` or `Heartbeat`. | A failed handshake is logged as a warning, and the connection dropped before any request. | Availability: dropping, delaying, or partitioning traffic stalls writes and failovers. Safety still holds, since epochs fence stale writers and W + Q > R, with no clock deciding anything (ADR-0018). Traffic metadata (who talks to whom, and how much) is visible. **Plaintext mode** (`--insecure-plaintext`) has no protection: nothing is authenticated or encrypted, and any request without a token may do anything. |
| **Compromised client** (its certificate and key, or a token) | It acts only within its scopes; anything else is refused with `PERMISSION_DENIED` before any state is touched. It can't make node-to-node calls. Statements are confined to the database they name: `ATTACH`, `DETACH`, `VACUUM INTO`, `load_extension`, and every pragma but read-only introspection are refused, names beginning with `_fleetring_` are reserved, and SQLite runs in defensive mode (ADR-0026 point 8). `stream` doesn't grant reading tables. A token that names another principal than the certificate it arrives with is refused, unless that certificate's principal forwards tokens. | Every request it makes is an audit line on the `fleetringd::audit` target (debug when allowed, warning when refused), naming the principal, the credential (`certificate` or `token:<id>`), the action, the database, and the RPC, never the token. | Whatever its scopes allow: with `write` it can change or delete rows, and with `admin` it can drop a database, which every replica deletes within seconds. A stolen token works until it expires or `token revoke` reaches each node, within one heartbeat interval; a node whose denylist is more than three heartbeat intervals old refuses every token. Client certificates have no revocation list: one works until it expires (7 days by default), unless its principal's `[[principal]]` entry is removed on every node, which takes a restart. |
| **Compromised data node** (its key, or its host) | Its certificate acts on its own node only (ADR-0026 point 9): it can't create, drop, reconfigure, read, write, list, verify, or stream a database through the client API, revoke a token, drain or remove another member, join or heartbeat as another node, or hand over a lease it doesn't hold. It can't carry a token, or issue certificates or tokens. | Followers recompute each batch's chain hash and re-seed from a snapshot on a mismatch; `db verify` names a replica whose log hashes differently from the others, and `--expect` shows whether a log changed since a recorded hash. Refused calls are logged. | It can read and corrupt every database it replicates, and serve or acknowledge whatever it likes as their primary or follower. The chain isn't signed, so it shows replicas disagree, not which one is honest; a node writing as primary produces a valid chain over what it writes, and the chain doesn't cover pages already checkpointed into the database file (FR-045). Node-to-node calls aren't bound to placement yet, so it can also call `Replicate`, `Fence`, `Fetch`, `Snapshot`, and `Promote` for databases it doesn't host: read any database whole, and open a stream at a higher epoch that fences out its primary, within the epoch and quorum rules (ADR-0031; planned: FR-050). Its own audit lines are on its own host. |
| **Compromised control node** | It can't issue certificates or tokens, since the authority key isn't on it, and so can't impersonate a principal towards other nodes or widen any token's scopes. | The control log is readable (JSON commands in SQLite) and replays deterministically, but it isn't signed, and the node that writes it can rewrite it. | Nearly everything: the control node decides membership, replica sets, epochs, leases, promotions, and the token denylist it publishes. A compromised one can drop any database, give any node a lease, remove members, and publish an empty denylist, re-enabling revoked tokens. It is also a node, with everything a compromised data node can do. |
| **Lost or leaked authority key** | Nothing, while the key is trusted: see [The authority key](#the-authority-key). | Tokens and certificates it signs are indistinguishable from legitimate ones; audit lines name the principal and token ID they claim. | A leaked key is the whole cluster. A lost one means no new certificates or tokens, and existing certificates expire (nodes after 30 days, clients after 7, by default). |
| **Malicious operator** | Through the API, a principal is held to its scopes like any client, and can't make node-to-node calls. | Audit lines name the principal and credential of every request, if they are collected off the node. `db verify --expect <hash>` with a hash recorded elsewhere shows whether a log changed since, while replicas still hold it back to that position. | An operator who holds `ca.key`, a principal with `admin:*` and `cluster`, or root on the hosts can do anything those allow. There is no separation of duties, and audit lines are local and unsigned. Encryption at rest is out of scope; it is left to the disk. |
| **Compromised CI or release pipeline** | Every third-party action is pinned to a commit SHA. Signing is keyless (the workflow's OIDC identity), so there is no long-lived signing key to steal. The release job runs `cargo audit --deny warnings` before it publishes, and CI runs `cargo deny` and a daily `cargo audit`. | `cosign verify-blob` shows `SHA256SUMS` was signed by this repository's `release.yml` from a `fleetringd-v*` tag, with a transparency-log proof; `gh attestation verify` shows an archive, `install.sh`, or the image was built by this repository's workflows. `install.sh` checks the signature when a recent `cosign` is on `PATH`. | A malicious change merged into the repository, or a compromised workflow or maintainer account that pushes a tag: its artifacts are signed and attested like any other, since a signature proves where an artifact was built, not that its source is sound. Vulnerabilities no advisory lists yet. `install.sh` without `cosign` checks only against the release's own `SHA256SUMS`. The `fleetring` crate is published to crates.io by a maintainer, not by this workflow. The verify commands haven't yet run against a published release (FR-048). |

### The authority key

These properties rest on the authority key staying secret: that a node's certificate names its real node ID, that a client certificate names its real principal, and that a token's scopes are the ones an operator issued. Whoever holds the key can issue a certificate for any node ID, which can then join the cluster and make node-to-node calls; a certificate for any principal, which then gets that principal's `[[principal]]` scopes; and a token with any scopes, `cluster` included. Keep it off every node, as `fleetringd cert init` and the daemon README say.

If the key is leaked, or lost:

1. Create a new authority with `fleetringd cert init`, under the same `--cluster` name, since nodes refuse a certificate for another cluster.
2. Put both authority certificates in every node's `ca.crt`. Nodes reload it without a restart and trust both while the rotation overlaps, for certificates and tokens alike.
3. Issue new node and client certificates, and new tokens, from the new authority, and install them.
4. If the key leaked, revoke every token it signed with `fleetringd token revoke --key-id <kid>`, as a principal with `cluster` from the new authority. A key's revocation never expires.
5. Remove the old authority certificate from every node's `ca.crt`. Certificates it issued are refused from then on. A reload applies to new connections, so restart each node to close connections opened with the old certificates.

Until step 5, a leaked key can still issue certificates the cluster trusts. There is no certificate revocation list; a single stolen node or client key is retired the same way, or by letting its certificate expire.

### Verifying a release

The README's [Verify a release](README.md#verify-a-release) section has the `cosign verify-blob` and `gh attestation verify` commands and the expected identity; [RELEASING.md](RELEASING.md#releasing-fleetringd) describes what the release workflows sign and attest.

## Reporting a vulnerability

Report vulnerabilities privately through GitHub: open the repository's **Security** tab and choose **Report a vulnerability**. Please don't open a public issue.

Include what you found, how to reproduce it, and the affected versions. We aim to acknowledge reports within a week.

Input that reaches `fleetring` from outside, such as a ring spec decoded from JSON, must never cause a panic. A crash on such input counts as a security bug.
