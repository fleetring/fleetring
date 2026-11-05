# Security policy

## Supported versions

FleetRing is in early development. Security fixes go into the next release of the `fleetring` crate and of `fleetringd`; older releases don't get backports during 0.x.

## Authentication and authorization

Every `fleetringd` connection is mutual TLS with certificates from the fleet's authority, and every request is authorized against the caller's scopes before it touches any state (ADR-0025). What each RPC needs, and how principals get scopes from `[[principal]]` entries or bearer tokens, is in the daemon README's [scope table](crates/fleetringd/README.md#principals-and-scopes). A node's certificate acts on that node only (ADR-0026); the threat model below says what that does and doesn't cover.

## Threat model

This section describes `fleetringd` as built today. Every security ticket updates it; a property that is decided but not yet built is marked *planned* with its ticket.

### Assets

- **Database contents:** each replica's SQLite files under its `data_dir`, unencrypted.
- **The replication log and its hash chain:** each replica's log, with one BLAKE3 hash per transaction that commits every byte before it (ADR-0016 point 12).
- **Change streams** (FR-039): every change a `[[changes.sink]]` names, as row changes (with values under `capture = "after"` or `"full"`) or opaque batches, delivered to a file or a webhook, and the bearer token and client key a webhook presents.
- **Archives** (FR-049): every row of the databases an `[[archive]]` names, as base snapshots and log segments in an object store, unencrypted, and the static S3 keys that write them.
- **The control log:** the control node's command log and control snapshot (one SQLite database): membership, databases, epochs, leases, and token revocations. It holds no key material.
- **The authority key** (`ca.key`): issues every node and client certificate and signs every token. It belongs on no node.
- **Node and client keys:** the private key of each node's and each client's certificate.
- **Tokens:** bearer credentials that name a principal and its scopes; one hour by default, 30 days at most.
- **Release artifacts:** the `fleetringd` archives, `install.sh`, `SHA256SUMS`, and the container image.

### Actors

| Actor | What the design prevents | What it detects | What it doesn't cover |
|---|---|---|---|
| **Network attacker** | Every connection is TLS 1.3 through `rustls`, and both ends verify the other's certificate against the trusted authorities, so the attacker can't read, alter, or inject traffic, or connect at all without a certificate. A certificate must name this fleet, and a node can't claim another node's ID in `Join` or `Heartbeat`. | A failed handshake is logged as a warning, and the connection dropped before any request. | Availability: dropping, delaying, or partitioning traffic stalls writes and failovers. Safety still holds, since epochs fence stale writers and W + Q > R, with no clock deciding anything (ADR-0018). Traffic metadata (who talks to whom, and how much) is visible. **Plaintext mode** (`--insecure-plaintext`) has no protection: nothing is authenticated or encrypted, and any request without a token may do anything. |
| **Compromised client** (its certificate and key, or a token) | It acts only within its scopes; anything else is refused with `PERMISSION_DENIED` before any state is touched. It can't make node-to-node calls. Statements are confined to the database they name: `ATTACH`, `DETACH`, `VACUUM INTO`, `load_extension`, and every pragma but read-only introspection are refused, names beginning with `_fleetring_` are reserved, and SQLite runs in defensive mode (ADR-0026 point 8). `stream` doesn't grant reading tables. A token that names another principal than the certificate it arrives with is refused, unless that certificate's principal forwards tokens. | Every request it makes is an audit line on the `fleetringd::audit` target (debug when allowed, warning when refused), naming the principal, the credential (`certificate` or `token:<id>`), the action, the database, and the RPC, never the token. | Whatever its scopes allow: with `write` it can change or delete rows, and with `admin` it can drop a database, which every replica deletes within seconds. A stolen token works until it expires or `token revoke` reaches each node, within one heartbeat interval; a node whose denylist is more than three heartbeat intervals old refuses every token. Client certificates have no revocation list: one works until it expires (7 days by default), unless its principal's `[[principal]]` entry is removed on every node, which takes a restart. |
| **Compromised data node** (its key, or its host) | Its certificate acts on its own node only (ADR-0026 point 9): it can't create, drop, reconfigure, read, write, list, verify, or stream a database through the client API, revoke a token, drain or remove another member, join or heartbeat as another node, or hand over a lease it doesn't hold. It can't carry a token, or issue certificates or tokens. | Followers recompute each batch's chain hash and re-seed from a snapshot on a mismatch; `db verify` names a replica whose log hashes differently from the others, and `--expect` shows whether a log changed since a recorded hash. Refused calls, binding refusals included, are logged. | It can read and corrupt every database it replicates, and serve or acknowledge whatever it likes as their primary or follower. The chain isn't signed, so it shows replicas disagree, not which one is honest; a node writing as primary produces a valid chain over what it writes, and the chain doesn't cover pages already checkpointed into the database file (FR-045). It can't reach databases it doesn't host: node-to-node calls (`Replicate`, `Fence`, `Fetch`, `Snapshot`, `Promote`, and the promotion and lease calls to the control node) are bound to placement and checked against control state or the receiver's epoch record before they touch any state, and a node is the one its certificate names (ADR-0031). Its own audit lines are on its own host. |
| **Compromised control node** | It can't issue certificates or tokens, since the authority key isn't on it, and so can't impersonate a principal towards other nodes or widen any token's scopes. | The control log is readable (JSON commands in SQLite) and replays deterministically, but it isn't signed, and the node that writes it can rewrite it. | Nearly everything: the control node decides membership, replica sets, epochs, leases, promotions, and the token denylist it publishes. A compromised one can drop any database, give any node a lease, remove members, and publish an empty denylist, re-enabling revoked tokens. It is also a node, with everything a compromised data node can do. |
| **Lost or leaked authority key** | Nothing, while the key is trusted: see [The authority key](#the-authority-key). | Tokens and certificates it signs are indistinguishable from legitimate ones; audit lines name the principal and token ID they claim. | A leaked key is the whole fleet. A lost one means no new certificates or tokens, and existing certificates expire (nodes after 30 days, clients after 7, by default). |
| **Malicious operator** | Through the API, a principal is held to its scopes like any client, and can't make node-to-node calls. | Audit lines name the principal and credential of every request, if they are collected off the node. `db verify --expect <hash>` with a hash recorded elsewhere shows whether a log changed since, while replicas still hold it back to that position. | An operator who holds `ca.key`, a principal with `admin:*` and `fleet`, or root on the hosts can do anything those allow. There is no separation of duties, and audit lines are local and unsigned. Encryption at rest is out of scope; it is left to the disk. |
| **Compromised CI or release pipeline** | Every third-party action is pinned to a commit SHA. Signing is keyless (the workflow's OIDC identity), so there is no long-lived signing key to steal. The release job runs `cargo audit --deny warnings` before it publishes, and CI runs `cargo deny` and a daily `cargo audit`. | `cosign verify-blob` shows `SHA256SUMS` was signed by this repository's `release.yml` from a `fleetringd-v*` tag, with a transparency-log proof; `gh attestation verify` shows an archive, `install.sh`, or the image was built by this repository's workflows. `install.sh` checks the signature when a recent `cosign` is on `PATH`; `fleetringd upgrade` always checks it, refuses without `cosign` 2.4.2 or newer, and then checks the archive's checksum and the binary's version (ADR-0038). | A malicious change merged into the repository, or a compromised workflow or maintainer account that pushes a tag: its artifacts are signed and attested like any other, since a signature proves where an artifact was built, not that its source is sound. Vulnerabilities no advisory lists yet. `install.sh` without `cosign` checks only against the release's own `SHA256SUMS`. The `fleetring` crate is published to crates.io by a maintainer, not by this workflow. |

### Archives

An archive holds every row of the databases it names, and their history back to its oldest base snapshot, unencrypted, in a store outside the fleet: whoever can read the bucket or directory can read the data, and whoever can write it can delete it. Give the bucket the care the nodes' disks get, with keys that may write only under the archive's prefix, and versioning or object lock if deletes must be recoverable. The S3 keys come from the environment or from a file only its owner may read (the daemon refuses one others can read), are held in memory that is zeroed when dropped, and are never logged or printed. Every segment and snapshot chunk carries a checksum, and a restore recomputes the hash chain over every batch it applies, so an object that was altered or lost fails the restore as data loss, naming it, rather than restoring a wrong copy; like the chain itself, this detects corruption and truncation, not a writer with the keys who rewrites segments and their hashes consistently. Restoring needs `admin` on the source database as well as on the new one, since a restore copies history its caller's scopes may no longer show.

### Change streams

A file sink writes the rows of the databases it names, unencrypted, where its `path` says: give that directory the care the nodes' disks get. A webhook sink sends the same rows to another machine, so it is TLS by default (FR-073): an `https://` `url` is dialled over TLS 1.2 or 1.3, verified against the platform's root certificates and any extra authority in `ca`, and a certificate it doesn't trust is a failed delivery that is retried, never one that falls back to plaintext or drops the change. Plaintext is an explicit opt-in: an `http://` `url` fails configuration validation unless that sink sets `allow_http = true`, and the daemon warns at start if such a sink also sends a token. A receiver that requires client authentication gets a client certificate (`cert` and `key`) and a bearer token (`token_file` or `token_env`, never the configuration file). The token file must be readable by its owner only, which the daemon enforces on Unix (on Windows, restrict it with the file's ACL); the token and the key are held in memory that is zeroed when dropped (rustls keeps its own copy of the key, and each request's `Authorization` header a copy of the token, while they are in use), are kept out of `Debug` output, and are never logged: no span or event carries the `Authorization` header. A token is read once, when the sink starts, so rotating it takes a restart. What the receiver does with a change, and whether it dedups the at-least-once repeats a failover can cause, is the receiver's business.

### The admin listener

The admin listener (`/metrics`, `/healthz`, `/readyz`, ADR-0034) is loopback and plaintext by default, so only processes on the node's machine can reach it. A non-loopback address fails validation unless `[admin] tls = true`, which serves the node's own certificate and limits `/metrics` to the client certificates of the principals `[admin] scrapers` lists (`401` without a certificate, `403` for any other principal or a node, logged on `fleetringd::audit`), or `[admin] allow_plaintext = true`, which opens every endpoint to whoever can connect and warns at every start. The probes never need a certificate: they say only whether the node is up and ready. Metrics carry no database names, principals, token or key IDs, peer nodes, addresses, or SQL; label values come from closed sets in the code or from the node's own configuration (a sink's name), so a scraper learns how many databases a node holds and how they fare, not which ones. A scraper's certificate grants nothing on the gRPC port unless a `[[principal]]` entry also lists it. It serves at most 64 connections at once and closes any beyond them as soon as it accepts them.

Logs are more revealing than metrics. Every request runs in an `rpc` span that carries the caller's principal or node ID and the database the request names, and every line the request causes, in text or JSON, repeats them (ADR-0034, point 8); audit lines name the principal, the credential (`certificate` or `token:<id>`), and the database. Tokens, keys, and certificates are never logged. Database names are tenant data, so treat logs, and wherever they are shipped, with the care the scopes give the databases.

### Dev mode

`fleetringd dev` (ADR-0039) keeps its authority's key (`tls/ca.key`) and the `admin` principal's key, which carries every scope, unencrypted in its directory, `~/.fleetring` by default, beside the databases. Whoever can read that directory owns the dev fleet. That is acceptable only because dev mode is for local development on a single-user machine: with `dev = true` the daemon refuses any address that isn't loopback, any role but `both`, and plaintext, however it is started, and its directories are `0700` and its keys `0600` on Unix. Never put data you care about in a dev node, never copy its directory to a server, and never trust its authority anywhere else.

### The authority key

These properties rest on the authority key staying secret: that a node's certificate names its real node ID, that a client certificate names its real principal, and that a token's scopes are the ones an operator issued. Whoever holds the key can issue a certificate for any node ID, which can then join the fleet and make node-to-node calls; a certificate for any principal, which then gets that principal's `[[principal]]` scopes; and a token with any scopes, `fleet` included. Keep it off every node, as `fleetringd cert init` and the daemon README say.

If the key is leaked, or lost:

1. Create a new authority with `fleetringd cert init`, under the same `--fleet` name, since nodes refuse a certificate for another fleet.
2. Put both authority certificates in every node's `ca.crt`. Nodes reload it without a restart and trust both while the rotation overlaps, for certificates and tokens alike.
3. Issue new node and client certificates, and new tokens, from the new authority, and install them.
4. If the key leaked, revoke every token it signed with `fleetringd token revoke --key-id <kid>`, as a principal with `fleet` from the new authority. A key's revocation never expires.
5. Remove the old authority certificate from every node's `ca.crt`. Certificates it issued are refused from then on. A reload applies to new connections, so restart each node to close connections opened with the old certificates.

Until step 5, a leaked key can still issue certificates the fleet trusts. There is no certificate revocation list; a single stolen node or client key is retired the same way, or by letting its certificate expire.

### Verifying a release

The README's [Verify a release](README.md#verify-a-release) section has the `cosign verify-blob` and `gh attestation verify` commands and the expected identity; [RELEASING.md](RELEASING.md#releasing-fleetringd) describes what the release workflows sign and attest.

## Reporting a vulnerability

Report vulnerabilities privately through GitHub: open the repository's **Security** tab and choose **Report a vulnerability**. Please don't open a public issue.

Include what you found, how to reproduce it, and the affected versions. We aim to acknowledge reports within a week.

Input that reaches `fleetring` from outside, such as a ring spec decoded from JSON, must never cause a panic. A crash on such input counts as a security bug.

## How we handle a report

From report to published advisory, nothing about the flaw is public until the fix is released.

1. **The report arrives as a draft advisory.** GitHub's private vulnerability reporting opens a draft security advisory, visible only to the reporter and the maintainers. Discussion stays in it.
2. **Confirm and assess.** Reproduce the flaw, and settle the affected versions, the severity, and the CWE in the draft. If it isn't a vulnerability, say why in the draft and close it.
3. **Fix privately.** Start the advisory's temporary private fork ("Start a temporary private fork") and open the fix as a pull request there, or work in a private fork. Don't push the fix, a test that reveals it, or a commit message that describes it to the public repository before the release.
   - Actions don't run in a temporary private fork, so run the checks in [`CONTRIBUTING.md`](CONTRIBUTING.md#before-you-push) locally.
   - A fix gets the same independent review as any security-sensitive change.
4. **Release the fix.** Merge the fix and tag the patched release as [`RELEASING.md`](RELEASING.md) describes. During 0.x, fixes go into the next release; there are no backports to older minor versions. The release notes' Security section names the advisory.
5. **Publish the advisory with the release,** as soon as the patched artifacts are verified (see [Publishing an advisory](#publishing-an-advisory)). Credit the reporter unless they ask not to be named.

### Publishing an advisory

The advisory is the record users act on, so it says what is wrong, who is affected, and what to do.

- **The title names the flaw, not the fix,** and starts with the component:
  - Right: "fleetringd: a SQL statement can read and write another database's file on the same node".
  - Wrong: "fleetringd 0.2.0 confines statements to one database".
- **The fields:**

  | Field | What it holds |
  |---|---|
  | Package | Ecosystem `other` and name `fleetringd` for the daemon. Ecosystem `crates.io` and name `fleetring` for the crate. |
  | Affected versions | The exact range, such as `= 0.1.0` or `>= 0.1.0, < 0.2.1`. |
  | Patched versions | The first release with the fix, such as `0.2.0`. |
  | Severity | Low, moderate, high, or critical, with the CVSS vector when one has been worked out. |
  | CWE | The weakness, such as CWE-22 for a path or file reached outside its boundary. |

- **The description has four sections, in this order:**
  - **Impact:** what an attacker can do, with what access, and in which versions. Include what the flaw *doesn't* allow, when that matters to readers' risk.
  - **Patches:** the release that fixes it, and what the fix does.
  - **Workarounds:** what a user who can't upgrade yet can do, or "none".
  - **After upgrading:** how to check whether the flaw was used, and anything to clean up.
- **For the `fleetring` crate,** also file the advisory with [RustSec](https://github.com/rustsec/advisory-db), so `cargo audit` and `cargo deny` report it.

**Example:** [GHSA-353r-2q3f-g5wc](https://github.com/fleetring/fleetring/security/advisories/GHSA-353r-2q3f-g5wc), published with `fleetringd` 0.2.0:

| Field | Value |
|---|---|
| Title | fleetringd: a SQL statement can read and write another database's file on the same node |
| Package | `other` / `fleetringd` |
| Affected versions | `= 0.1.0` |
| Patched versions | `0.2.0` |
| Severity | High |
| CWE | CWE-22 |
| Impact | In 0.1.0, a statement sent through `db exec` or `db query` could attach another database's SQLite file on the same node, then read or write it, bypassing that database's lease and epoch fencing, replication, and change capture, so its replicas could silently diverge. |
| Patches | 0.2.0 confines every statement to the database it names: `ATTACH`, `DETACH`, `VACUUM INTO`, and `load_extension` are refused, only read-only introspection pragmas are allowed, and SQLite's defensive mode is on. |
| Workarounds | None in 0.1.0 short of restricting network access to the data plane to fully trusted clients. |
| After upgrading | Run `fleetringd db verify` on each database to confirm its replicas agree. 0.1.0's logs have no hash chain, so this checks only from the upgrade onward. |
