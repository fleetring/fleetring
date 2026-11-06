# ADR-0044: Principals have kinds, agents record an owner and a parent, holders delegate narrower biscuit tokens without the authority's key, and a fork is a new database that records where it came from

- **Status:** Proposed
- **Date:** 2026-11-05
- **Related:** ADR-0017 (control state), ADR-0020 (simulation), ADR-0021 (engines), ADR-0025 and ADR-0026 (principals, scopes, tokens; this ADR amends 0025 points 1 and 3 and 0026 points 3 and 5 once accepted), ADR-0029 (archives), ADR-0031 (node calls bound to placement), ADR-0037 (router; point 2 amended by point 6.4 below), ADR-0038 (fleet features), ADR-0039 (dev mode), ADR-0041 (agent kit), ADR-0042 (no deletion by absence), ADR-0043 (the `fleetring` CLI, accepted: agent identities in `identity.toml` with `owner` and `parent`, and `fork` and `grant` reserved; point 10 adds two 0.7 requirements to its plan); tickets FR-079 (#114), FR-074 (#99), FR-077 (#105), FR-066 (#69), FR-070 (#76), FR-049 (#52)

This is a design record. It decides the model: who acts, what they may do, how authority is handed down, and what a fork is. It builds nothing. Each part names the release that would build it (point 10), and each later part has its own plan and, where the wire or the control log changes, its own ADR.

## Context

FleetRing is becoming storage for agents: one database per agent, task, or user, created and dropped by the agents themselves (ADR-0041), with an agent that sets itself up in one command (ADR-0043 point 8). Three things agents do aren't expressible yet:

- **A parent spawns children.** A child needs its own databases and a credential that reaches them and nothing else. Today only the authority's key mints a token (ADR-0025 point 2), and that key belongs on no node. In dev mode a process can mint from the local authority (ADR-0043 point 8.6), but against any real fleet a parent agent has no way to hand a child a narrower credential.
- **A child starts from what its parent knows.** The natural shape is a fork: the child's memory database starts as a copy of the parent's at a point in its history, and diverges.
- **Knowledge moves between owners.** A team's agents read the team's databases; an organization publishes read-only databases that other fleets and agents read or fork, and wants a reader to be able to check who published them.

The longer arc constrains the model even where it builds nothing now. A fleet may run on a laptop, in a cloud, or both, with local primaries and cloud followers; it may be shared by several teams or organizations; two fleets may trust each other for some prefixes; people log in through their company's identity provider. The model must not close any of these off.

What exists:

- **Principals and scopes** (ADR-0025, ADR-0026). A principal is a name, 1 to 64 bytes of `[a-z0-9._-]`. A scope is `<action>:<pattern>` with the actions `read`, `write` (implies `read`), `admin` (implies both), and `stream` (alone), or the `fleet` scope. A pattern is an exact name or a byte prefix ending in `*`. Default deny, enforced at the request boundary; `apply` never sees a principal (ADR-0025 point 5).
- **Tokens** are a small, strict JWS: `EdDSA` from a key the authority's certificate bundle holds, registered claims plus `fleetring.scopes`, unknown claims refused, at most 30 days long (ADR-0026 point 5). A token must name the principal of the connection's client certificate, unless that principal forwards tokens, as the router does (ADR-0026 point 3).
- **Revocation** is the denylist in control state: `Revoked::Token(TokenId)` or `Revoked::Key(KeyId)`, sent with every heartbeat reply, failing closed when stale (ADR-0026 point 6 and its amendment).
- **The separator rule.** A pattern is bytes, so `admin:agent-42*` covers `agent-420`. Issuers warn about a prefix that ends in a letter or digit (ADR-0041 point 5), and agent IDs are `[a-z0-9_]{1,32}`, so the `-` after `agent-<id>` always ends the ID (ADR-0043 point 8.5).
- **Agent identities** (ADR-0043 point 8, accepted for 0.7): a directory per agent holding a certificate for `agent-<id>`, a token scoped `admin:agent-<id>-*`, and `identity.toml` (no secrets; `version = 1`, read with unknown fields refused; the principal, scopes, expiries, `issued_by`, and, for delegation, `owner` and `parent`). Agent IDs are `[a-z0-9_]{1,32}`. The certificate alone authorizes nothing, since `agent-<id>` has no `[[principal]]` entry. ADR-0043 point 8.11 already reserves `fork` and `grant` in the scope grammar, refused by every parser and node until delegation exists, and leaves child agents to this ADR.
- **Provenance.** `created_by` and `description_by` record principal names and grant nothing; a restored description is untrusted because an archive isn't signed (ADR-0041 points 1 and 10). FR-074 (#99) proposes that replicas co-sign each archived segment's chain head with their node keys, so a restore can prove a quorum of the fleet held that history.
- **Engine neutrality.** Above the adapter, positions are opaque, byte-ordered values, and every database records its engine (ADR-0021).

Words: the lexicon reserves "checkpoint" for the engine's WAL checkpoint, "role" alone for nothing, and "owner" for nothing about a database (a database has a [creator](../lexicon.md#creator)). This ADR therefore says *signed head* for FR-074's co-signed chain head, *access role* for a named bundle of scopes, and *owner* only for the principal an agent acts for.

## Decision

### 1. Identities: four kinds, three of them principals

| Kind | What it is | Name convention | Authenticated by |
|---|---|---|---|
| `user` | a person | `u-<user>` | a client certificate, or a token issued after a login (point 5) |
| `agent` | a program acting for an owner, possibly spawned by another agent | `agent-<id>` | a certificate or a holder key, plus a token (point 3.4) |
| `service` | any other program: an application, the router, CI, a publisher's signer | `svc-<name>`, or any name today's fleets already use | a client certificate, or a token |
| `node` | a `fleetringd` process | its node ID | its node certificate (ADR-0025 point 1) |

1. **A node is not a principal.** It keeps the separate identity, binding, and calls ADR-0026 point 9 and ADR-0031 give it. It shares only the key types of point 1.6 below.
2. **The principal name stays the identity everything records.** It is what `sub`, `created_by`, `description_by`, and audit lines hold, with today's grammar. Authorization never parses a name: kind, owner, and parent are attributes carried in the identity record and in the token (point 3), and the conventions above only make names readable. A principal with no recorded kind, which is every principal today, is a `service`.
3. **An agent records an owner and a parent.**
   - `owner` is the principal accountable for the agent, as ADR-0043 point 8.3 records it for a root agent: the principal the creating command ran as (`admin` in dev mode), or `authority` when only the authority's key was used. A child inherits its parent's owner, so every agent in a lineage answers to the same principal.
   - `parent` is the agent ID of the agent that spawned it (`42`, whose principal is `agent-42`), absent for a root agent, as ADR-0043 point 8.3 records it.
   - Both are recorded in the agent's `identity.toml` and, from 0.8, in the delegation block that created the agent's token (point 3.2), which names the parent by principal. Like `created_by`, neither grants anything: what an agent may do comes from its token alone.
4. **A child agent's ID extends its parent's with `.`:** agent `42` spawns `42.c7`, whose principal is `agent-42.c7`, and `42.c7` spawns `42.c7.r1`. Each segment is `[a-z0-9_]{1,32}`, as ADR-0043 point 8.5 defines a root ID, and the whole principal name stays within 64 bytes, which bounds the depth. ADR-0043's grammar has no `.`, so 0.7 already refuses `42.c7` as an agent ID and no 0.7 identity can collide with a lineage ID; 0.8 widens the grammar to `.`-separated segments, and only for identities a delegation creates.
5. **An agent's databases nest under its lineage.** A root agent's own databases are `agent-42-<purpose>`, as today. Its descendants' are `agent-42.<…>-<purpose>`, which the pattern `agent-42.*` covers and nothing else does. `.` is the separator here, as `-` is in `agent-42-*`: it isn't a letter or digit, so ADR-0041's warning doesn't fire, and `agent-42.*` can't cover `agent-420…`, whose byte after `42` is `0`. No segment holds `.` or `-`, so a sibling `c70` isn't covered by `agent-42.c7-*` or `agent-42.c7.*` either. A parent's token can therefore cover its whole subtree with one scope, and every scope it hands a child is a narrowing of one it holds (point 2.4).
6. **Keys have a type.** Every public key FleetRing stores or compares (a certificate's, a token root's, a holder's) is a `(key type, bytes)` pair, written `ed25519/<hex>`, and verification goes through one `PublicKey` trait with a `#[non_exhaustive]` `KeyType`. Ed25519 is the only type now, since `ring` already provides it and every key in a fleet is one. P-256 (passkeys), secp256k1 (wallets), and external subjects such as an OIDC issuer and subject are later *credentials* of a principal, mapped to it at the edge (point 5). None of them is ever a principal's name, so adding one changes no recorded name.
7. **One identity file across releases: `identity.toml`, versioned, strict.** ADR-0043 point 8.3 defines it, at `version = 1`, and its reader refuses unknown fields so that a damaged or hand-edited file fails loudly. That strictness stays. A release that adds a field (0.8: the holder key's file, the token's format, a delegated identity's lineage) writes `version = 2` for every identity that uses one, and keeps writing `version = 1`, field for field, for an identity a 0.7 reader can use as it is. Every reader takes `version` before the strict parse and refuses a version it doesn't know with an error that says to upgrade, so a 0.7 CLI meets a delegated identity with a clear refusal, not an unknown-field error and never a half-read credential. Tolerating unknown fields was rejected: a 0.7 client that ignored a holder key would present the wrong certificate and fail far from the cause.
8. **`.` in a principal name is reserved for lineage.** Today's grammar allows `.`, and some fleets already have principals such as `workload.prod`. Those keep working, as services: only an `agent` delegates (point 3.5), so no lineage name can ever be derived from them. But no newly issued name may contain `.`, so that a lineage name only ever comes from a delegation, under its ancestors' revocation:
   - **from 0.7,** `cert client`, `token issue --principal`, and `agent add` refuse a name that starts with `agent-` and contains `.`;
   - **from 0.8,** they, and the issuer later, refuse `.` in every new principal name, with a hint naming `-` or `_` instead;
   - **nodes refuse** a biscuit authority block whose principal contains `.`, or whose kind is `agent` and whose principal isn't `agent-<id>` or `<org>_agent-<id>`, and a JWT whose subject starts with `agent-` and contains `.`. So a root token for `agent-42.c7` can't exist and escape `Revoked::Principal` for `agent-42`, and no service name such as `workload` can be made an agent whose children would be named like `workload.prod`. (`Revoked::Principal` also matches every name below the one it names, point 3.8, as a second guard.)

   Organizations therefore don't use `.` in principal names: an organization's principals are `<org>_<name>` (point 6.2).

### 2. Scopes: `fork` and `grant` join the actions

The actions become a lattice, with two additions:

| Action | Allows | Implies |
|---|---|---|
| `read` | queries, placement, the lease | |
| `write` | changing contents | `read` |
| `fork` | creating a fork *from* the database (point 7) | `read` |
| `admin` | creating, dropping, reconfiguring; creating a fork *into* the name | `write`, `fork`, `read` |
| `stream` | following changes | nothing |
| `grant` | naming a new holder for the token's other scopes, narrowed to this pattern (point 3.5) | nothing |

1. **`fork` is its own action and implies `read`.** It isn't implied by `read` or `write`; `admin` implies it. The split is about metering and about a consistent, provenance-carrying copy made in one operation, never about confidentiality: anyone with `read` can copy every row by querying, so `read` must never be given to a principal the data must be kept from, whether or not it holds `fork`.
2. **`grant` is its own action, implied by nothing, and covers a pattern.** `grant:agent-42.*` lets a holder name a new holder for the scopes it holds, narrowed to names under `agent-42.`. `admin` doesn't imply it: administering a prefix and handing out credentials for it are separate trusts. The `fleet` scope is dropped by every delegation block, whatever the block lists, so it can never be delegated.
3. **The separator rule becomes a refusal for new token forms.** A pattern in a biscuit token (point 3), in an access role (point 2.5), or in a trust anchor's constraint (point 6) must end, before its `*`, in a byte that isn't an ASCII letter or digit; anything else is refused. Today's JWTs and `[[principal]]` entries keep the warning of ADR-0041 point 5, so nothing that works today breaks.
4. **Two operations narrow scopes, and both are exact.** A pattern is either `Exact(n)`, which matches the name `n`, or `Prefix(p)`, written `p*`, which matches every name whose bytes start with `p` (`*` alone is `Prefix("")`).
   - **The meet of two patterns** is the pattern matching exactly the names both match:
     - `Exact(a) ∧ Exact(b)` is `Exact(a)` if `a = b`, else nothing;
     - `Exact(a) ∧ Prefix(p)` is `Exact(a)` if `a` starts with `p`, else nothing;
     - `Prefix(p) ∧ Prefix(q)` is the longer prefix if one starts with the other, else nothing.
   - **The meet of two actions** is the strongest action both imply: `admin ∧ x = x` for `x` in `read`, `write`, `fork`, `admin`; `write ∧ fork = read`; `read ∧ write = read ∧ fork = read`; `stream ∧ stream = stream` and `grant ∧ grant = grant`; every other pair (`stream` or `grant` with anything else) is nothing. `fleet ∧ fleet = fleet`; `fleet` with any database scope is nothing.
   - **Intersecting two sets of scopes** (an attenuation block) is the set of every non-empty pairwise meet, normalized by dropping any scope another scope in the set covers and sorting, so the result is canonical.
   - **The grant step** (a delegation block) first intersects as above, then meets every resulting pattern with each pattern the effective scopes' `grant` scopes cover, keeping the non-empty results and dropping `fleet`. So a holder can name a new holder only for what its `grant` covers.
   - Both are one pure function each in the token module, and `fleetring token inspect` prints their result. The golden vectors, which `docs/spec/tokens-v1.md` carries and extends:

   | Effective | Block | Operation | Result |
   |---|---|---|---|
   | `read:agent-42-*` | `write:agent-42-memory` | intersect | `read:agent-42-memory` |
   | `admin:agent-42-*` | `fork:agent-42-mem*` | intersect | `fork:agent-42-mem*` |
   | `read:agent-42-mem` | `read:agent-42-memory*` | intersect | nothing: the exact name isn't under the prefix |
   | `read:agent-42-memory*` | `read:agent-42-mem` | intersect | nothing |
   | `read:agent-42-memory` | `read:agent-42-*` | intersect | `read:agent-42-memory` |
   | `write:agent-42-*` | `fork:agent-42-*` | intersect | `read:agent-42-*` |
   | `stream:t-*` | `read:t-*` | intersect | nothing |
   | `admin:a-*`, `admin:b-*` | `read:b-x-*`, `read:c-*` | intersect | `read:b-x-*` |
   | `fleet`, `read:a-*` | `fleet` | intersect | `fleet` |
   | `fleet`, `admin:agent-42.*`, `grant:agent-42.*` | `fleet`, `admin:agent-42.c7-*` | grant step | `admin:agent-42.c7-*` |
   | `admin:agent-42-*`, `admin:agent-42.*`, `grant:agent-42.*` | `admin:agent-42.c7-*`, `read:agent-42-memory` | grant step | `admin:agent-42.c7-*`: the memory isn't under the `grant` |
   | `admin:agent-42-*`, `admin:agent-42.*`, `grant:agent-42-*`, `grant:agent-42.*` | `admin:agent-42.c7-*`, `fork:agent-42-memory`, `grant:agent-42.c7.*` | grant step | `admin:agent-42.c7-*`, `fork:agent-42-memory`, `grant:agent-42.c7.*` |
   | `admin:agent-42.*` (no `grant`) | `admin:agent-42.c7-*` | grant step | refused: no `grant` |

5. **Access roles are named bundles of scopes, compiled into tokens when they are issued.**
   - An access role is a name and a list of scope templates with parameters, such as `developer = ["admin:team-{team}-*", "read:org-*", "fork:pub-*"]`.
   - A role is assigned to a principal or a group (point 6.3) with its parameters bound.
   - The issuer (point 5) expands a principal's assignments into plain scopes when it issues a token. Nodes never see an access role and never look one up, so verification stays offline and lookup-free.
   - **Removing a role, or a principal from a group, takes effect at the principal's next token.** Tokens already issued keep the role's scopes until they expire; to end them sooner, revoke the principal (`Revoked::Principal`, point 3.8). The issuer's lifetimes for people (12 hours at most, point 5.2) bound the wait otherwise.
   - An access role can't contain the `fleet` scope or `grant` over a pattern its editor can't grant.
   - Roles, groups, assignments, and login mappings live in the *identity registry*: an ordinary FleetRing database, `_fleetring/identity`, read only by the issuer. Not in control state, because no node needs them, every change would be a new control command and a fleet feature, and control state should stay small. Not in a separate store, because a FleetRing database is replicated, archived, and failed over already, and a standalone auth layer can keep the same schema in a local SQLite file.
   - **Database names starting with `_fleetring` are reserved for FleetRing, and only the `fleet` scope reaches them.** A client is refused creating one, at the request boundary only, so every log replays. No pattern matches a reserved name: not `*` alone, not `_fleetring*`, not `admin:_fleetring/*`; every action on a reserved database, reading and writing `_fleetring/identity` included, needs `fleet`. Otherwise a principal with `admin` on the registry could write itself a role and receive it at its next token. This namespace of whole databases in control state is distinct from the `_fleetring_*` tables, indexes, and triggers that the SQLite adapter reserves *inside* every database (ADR-0041, `engine::sqlite`'s reserved namespace), which doesn't change.
   - Who edits what: principals with the `fleet` scope create organizations and their administrators, and the issuer, acting with `fleet` towards the registry, lets an organization's administrators edit its groups and roles (point 6.3), checking their limits itself.

### 3. Tokens: biscuit's format and signatures, FleetRing's rules

**Requirements.** A holder must be able to narrow its token for a child without holding the authority's key and without a network call. A node must verify the result offline with the authority's public key alone, as it does a JWT today. A child must never hold more than its parent, or for longer. Someone who copies a token's bytes, without the holder's private key, must gain nothing by it. Revoking a parent must revoke what it delegated. And because agents are written in Python and TypeScript as often as in Rust, an agent framework must be able to build and append blocks in its own language.

| | Biscuit (v3) | Macaroons | JWT chains or signed sub-tokens |
|---|---|---|---|
| Narrowing offline, without the authority | yes: append a block, signed with the key the token carries | yes: chained HMAC caveats | yes, if each holder has a key pair and signs the next link |
| Verification | public key: anywhere, offline | the root *secret*: every verifier can mint | public keys of each link |
| A block signed by a named key, not just the token's next key | yes: third-party blocks | no | ours to design |
| Revocation handles | a revocation ID per block, specified | none specified | ours to design |
| Libraries in agents' languages | Rust, Go, Java, Python, Swift, Haskell, and JavaScript through WebAssembly | few and mostly unmaintained | JWT libraries everywhere; chain formats nowhere |
| Cost | a heavier dependency, of which FleetRing uses the format and signatures only | small | small, on `ring`, but a protocol of our own |

**Biscuit's format wins; its authorizer isn't used.** Macaroons lose on their shared secret: every node that verifies could mint, so ADR-0026 point 9's "a stolen node key reaches only its own databases" would no longer hold for credentials. JWT chains would work, but they are a signature protocol of our own, with no library in any agent's language. Biscuit's format is that protocol, specified: a chain of blocks signed with ephemeral keys, third-party blocks signed by a named key, sealing, and revocation IDs, with libraries that build and append blocks in agents' languages. Its Datalog authorizer is a different matter: FleetRing doesn't run it (point 3.1), so what a token allows is decided by FleetRing's own rules, which this ADR and `docs/spec/tokens-v1.md` state.

**The crate.** `biscuit-auth` 6.0.0 (16 July 2025) is the newest release; it is Apache-2.0, which `deny.toml` allows; it declares no `rust-version`, and it builds with Rust 1.88 and 1.89 with default features (checked on aarch64 macOS, 5 November 2026). The project now lives under the Eclipse Foundation (`eclipse-biscuit/biscuit-rust`) and is active, with commits through late October 2026. An independent review found no RustSec advisory for it as of 5 November 2026. Its weight is the main cost: about 85 crates in its tree with default features, of which 34 aren't in the workspace today (among them `ed25519-dalek`, a second Ed25519 implementation beside `ring`, and the `p256`/`ecdsa` stack), plus 20 second versions of crates already here, among them `prost` 0.10 beside the workspace's 0.14, `sha2` 0.9, `base64` 0.13, and `rand` 0.8. Its unreleased main branch already moves to `prost` 0.14, `rand` 0.10, `base64` 0.22, and `ed25519-dalek` 3. So:

- it is pinned to one exact version (`=6.0.0`, or whichever release is newest when 0.8 starts), with default features off except what parsing and signing need, and upgraded only by a reviewed change;
- each duplicate version it brings is listed by name in `deny.toml`'s `[bans] skip`, so a new duplicate still warns;
- its authorizer, which reads `std::time::Instant` for a time limit (forbidden in daemon code by ADR-0020), is never called, so that question doesn't arise;
- every key it would draw from the OS (a block's next key) comes from the injected generator instead, through `append_with_keypair` and the third-party calls that take a key pair.

1. **What FleetRing takes from biscuit, and what it doesn't.** It takes the serialization, the verification of the block signature chain from the root key, the verification of third-party (external) signatures, sealing, and revocation IDs. It doesn't take the Datalog authorizer, at all. Blocks hold *facts only*, in fixed templates; FleetRing validates each block's structure and computes the token's effective scopes, holder, and expiry in one pure function (point 9), with the time from the node's `Clock` (ADR-0020). So FleetRing's verifier, and nothing in Datalog, is what enforces narrowing, `grant`, holder keys, and expiry. A generic biscuit authorizer handed one of these tokens enforces none of it, and nothing may rely on one.
2. **Three block templates; anything else is refused.**

   | Block | Signed by | Facts |
   |---|---|---|
   | **Authority** | an authorization anchor's key (point 6.1): for a fleet's own tokens, the authority's Ed25519 key, the same key that signs JWTs today | `fleet`, `principal`, `kind`, `owner` (agents), `token_id` (as `jti`, so `Revoked::Token` applies), one `scope` per scope, `not_before`, `expires` (at most 30 days after `not_before`, as today), `issued_at`, and optionally `holder` (an Ed25519 public key) |
   | **Attenuation** | the token's next key (an ordinary block) | one `scope` per scope, optionally `expires` |
   | **Delegation** | the token's next key *and* the current holder's key (a third-party block) | `principal` (the child), `parent` (the delegating principal), one `scope` per scope, `issued_at`, `expires`, and `holder` unless it is a bearer delegation (point 3.7) |

   A block with any rule, any check, any `trusting` annotation, any other fact, or a fact of the wrong type is refused; so is a third-party block that isn't a delegation, a delegation that isn't third-party, a token of more than 8 blocks, or one over 8 KiB (`MAX_TOKEN_LEN`, unchanged).
3. **Why stolen bytes are inert.** A biscuit carries the private half of its next key, so anyone holding the bytes can append a block. Against an attenuation block that is harmless: it only narrows, and the token stays bound to the same holder key (point 3.4), which the thief doesn't have. A delegation block is the one that changes who holds the token, so it must also carry an external signature by the *current holder's* private key: biscuit's third-party block, whose external signature covers the block's contents and the previous block's signature, so it can't be moved to another token. The holder key is a separate private key in the holder's identity directory, never inside the token. A thief with the token's bytes has the next key and not the holder key, so it can't name a new holder, and it can't use the token directly either, since the connection must prove the holder key (point 3.6). Mechanically, the delegating agent calls `third_party_request()` on its token, signs the child's block with its holder key (`ThirdPartyRequest::create_block`), and appends it with `append_third_party`, which also signs with the token's next key.

   Setting the token's next key to the child's public key instead was rejected: the proof must then hold the child's private key, which the parent doesn't have, so the token would have to be assembled outside the crate's API.
4. **Holder keys.** A holder key is the public key of the holder's client certificate.
   - A root agent's holder is the key of the certificate the authority issued it, which `agent add` (0.8) names in the authority block's `holder`.
   - A child's holder is a key it generates on its own machine, which its parent's delegation block names. It presents a self-signed certificate for that key (point 3.6). No private key file is ever copied.
   - A token whose authority block names no holder is bound to its principal's certificate, as today (ADR-0026 point 3). It can be attenuated but not delegated: a delegation block needs a current holder key to check its external signature against.
5. **Verification, in order, all or nothing.** One pure function takes the token, the connection's peer, the trust anchors, the denylist, and the node's time:
   1. **Bounds:** at most 8 KiB and 8 blocks.
   2. **Signatures:** parse with `biscuit-auth`, selecting the root key from the authorization anchors by the authority block's key ID; the crate verifies every block signature and every external signature.
   3. **Templates:** every block matches its template (point 3.2).
   4. **Anchor:** the effective scopes start as the authority block's scopes intersected with the anchor's constraint (point 6.1). The principal `P` and the kind are the authority block's; the holder `H` is its `holder`, or none; the expiry `X` is its `expires`. An authority block naming a principal whose name contains `.` is refused (point 1.8).
   5. **Blocks, in order.** For an attenuation block: intersect the scopes (point 2.4) and lower `X` to the block's expiry. For a delegation block:
      - `H` must exist, and the block's external key must be `H`;
      - `P`'s kind must be `agent`, and the block's `parent` must be `P`;
      - the block's `principal` must be `P` followed by `.` and one segment of `[a-z0-9_]{1,32}`, at most 64 bytes in all;
      - the effective scopes must include `grant`; apply the grant step (point 2.4), which drops `fleet`;
      - the block's `issued_at` must be at most the node's time plus 30 seconds (`NOT_BEFORE_LEEWAY_SECS`), and, for a bearer delegation, its `expires` at most 24 hours after its `issued_at`;
      - lower `X` to the block's `expires`; then `P` becomes the child, of kind `agent`, and `H` becomes the block's `holder`, or none for a bearer delegation.
   6. **Time:** the authority block's `not_before` is at most the node's time plus 30 seconds, and the node's time is before `X`.
   7. **Denylist:** the token ID, the root key ID, every block's revocation ID, and every principal in the chain, where `Revoked::Principal` for `Q` matches `Q` and every name starting `Q.` (point 3.8).
   8. **Connection:** if `H` exists, the peer must prove it: a holder-key peer whose key is `H`, or a principal's certificate naming `P` whose key is `H`. If `H` is none after a bearer delegation, only a relay with `forward_tokens` may carry the token. If the token has no holder and no delegation, today's rule applies: the certificate names `P`, or a relay forwards it.
   9. **Result:** the caller is `P` with the effective scopes, its credential the chain of principals and revocation IDs, which the audit line records. Empty effective scopes are a refusal.
6. **Holder-key connections fail closed.** Accepting a client certificate the authority didn't issue is new, and today's code isn't shaped for it: `auth::boundary` maps any peer that is neither a node nor a principal, without a token, to `Caller::Anonymous` (the `_ =>` arm of `resolve_identified`), `Peer::of` maps a connection that carried no identity to `Peer::Plaintext`, and `Caller::authorize` lets `Anonymous` make every call, because only `insecure_plaintext` was meant to produce it. So:
   - A self-signed client certificate becomes a distinct `Peer::HolderKey(PublicKey)`, accepted at the TLS layer only when the node's `[tls] accept_holder_keys` is on (on in dev mode, off by default elsewhere, and needed only where delegated agents connect directly), only for an Ed25519 key, and only with the handshake's proof of possession. Its subject and subject alternative names are never read: a holder-key certificate never names a principal or a node.
   - Without a token that verifies and is bound to that key, the caller is a distinct `Caller::Holder(PublicKey)`, which every RPC refuses with `UNAUTHENTICATED`, node calls included.
   - `Caller::Anonymous` comes only from a listener the configuration makes plaintext, decided from the configuration and never from a missing identity. Every `match` over `Peer` and `Caller` in `auth` is exhaustive, with no wildcard arm, so a new variant can't fall through to a permissive one.
   - A test runs every RPC in `auth::RPCS` from a holder-key peer with no token, a malformed token, a valid token bound to another key, and a valid token without `grant` used to delegate, and requires a refusal each time.
   - **The threat model changes, and `SECURITY.md` says so when this ships.** The network-attacker row today rests on the handshake: a peer without a certificate from the fleet's authority never reaches an RPC. With `accept_holder_keys` on, anyone who can reach the node completes a handshake with a key of its own, so the token check becomes the first filter. Such a peer reaches only the token parser and verifier, before any handler: the parser is bounded (8 KiB, 8 blocks, facts only) and fuzzed in the existing fuzz job, and a connection does nothing without a valid token bound to its key. The section on tokens adds what a thief gets from a copied token file (nothing, without the holder key; a bearer delegation is the exception, usable through a relay for at most its 24-hour window) and from a stolen holder key (that holder's scopes, until its token expires or is revoked).
7. **Bearer delegation, through the router only.** A delegation block may name no holder, so that an unmodified libSQL client can use the token through the router (ADR-0037), which can't see a client's key. Such a token is accepted only from a relay with `forward_tokens`, never on a direct connection, and can't be delegated further (it has no holder key to sign with). Its window lasts at most 24 hours, measured from the block's `issued_at`, which the node bounds by its own time plus 30 seconds. The holder chooses `issued_at`, so it can start the window later than it signs it, but never past the parent's expiry; the cap bounds the window's length, not when it opens.
8. **Revocation.**
   - `Revoked::Block(RevocationId)`: a block's revocation ID is the SHA-256 of its signature, base64url (43 characters, within the denylist's ID grammar). A node refuses a token if *any* of its blocks is on the denylist, so revoking a parent's token, or the block that delegated to a child, revokes every token derived from it.
   - `Revoked::Principal { principal, before }`: refuses every token, JWT or biscuit, that names the principal, or any principal whose name starts with it followed by `.`, anywhere in its chain, and whose authority block was issued before `before`. Revoking `agent-42` therefore ends `agent-42`, `agent-42.c7`, and every descendant's current tokens at once; tokens issued after the revocation work again.
   - Both are kept as `Revoked::Token` is: 30 days and 30 seconds after they are recorded, the longest any token they could refuse may live (ADR-0026 point 5). They travel and fail closed exactly as today's denylist does (ADR-0026 point 6).
   - Delegating to the same child segment twice yields two tokens naming the same principal, say `agent-42.c7`: one principal for scopes, audit lines, and `Revoked::Principal`, two tokens, each revocable alone by its delegation block. Harnesses should pick a fresh segment for every child they start.
9. **Coexistence with today's tokens.**
   - Nodes accept both formats on the same `authorization: Bearer` header, distinguished by shape (a JWS has three dot-separated parts; a biscuit has none). JWTs keep working unchanged, keep `token issue`'s default until 1.0, and stay the format for principals that never delegate. Whether 1.0 changes the default is left to then.
   - **Gating (ADR-0038 point 4).** The new denylist variants are new control state and travel in new control commands, so they are a fleet feature, `delegation`, enabled by the first revocation that uses one. Accepting a biscuit token changes no control state, so it isn't gated, but a node of the previous line refuses one (as ADR-0043 point 8.11 has nodes refuse any token carrying `fork` or `grant`); so every node reports `delegation` in its software features once it can verify them, and `token issue --format biscuit`, `agent add`, and delegation refuse to mint while a member doesn't report it, naming the member, as `upgrade --check` names one.
   - A fleet that enables `delegation` loses rollback to the previous line, as with any feature.
10. **Expiry and renewal.** A child's token can never outlive its parent's token. A renewed parent doesn't renew its children: it delegates to them again. ADR-0043's renew-at-start rule applies to a root agent's token; a child's harness asks its parent.

### 4. Delegation, end to end

A parent agent `42` holds a token whose authority block names its certificate's key as holder and grants `admin:agent-42-*`, `admin:agent-42.*`, `grant:agent-42-*`, and `grant:agent-42.*`. It spawns child `c7`, which may fork the parent's memory:

1. The child's harness creates `<agents_dir>/42.c7/`, generates the child's holder key and a self-signed certificate for it there, and passes the public key to the parent.
2. The parent builds a delegation block: principal `agent-42.c7`, parent `agent-42`, holder the child's key, scopes `admin:agent-42.c7-*` and `fork:agent-42-memory`, `issued_at` now, and `expires` two hours later. It signs the block with its own holder key as a third-party block and appends it. It writes the token and the child's `identity.toml` (`version = 2`, `owner` as the parent's, `parent = "42"`, and the holder key's file) to the child's directory. No authority, no network.
3. The child connects with its self-signed certificate, which proves the holder key, forks `agent-42-memory` into `agent-42.c7-memory` (point 7), uses it, and creates scratch databases under `agent-42.c7-`. It can't write the parent's memory, can't touch a sibling's databases, and can't delegate further, since its token holds no `grant`.
4. Someone who copies the child's token file but not its key can't use it on a node (the connection must prove the key) and can't delegate from it (a delegation needs the key's signature).
5. The parent, whose `admin:agent-42.*` covers the child's databases, reads the child's memory afterwards and copies back what it wants. Copying back is the application's choice, row by row; FleetRing never merges databases.
6. Revoking the parent's delegation block, or `agent-42.c7` as a principal, ends the child; revoking `agent-42` ends both.

In 0.8, `agent add` gives a root agent `admin:agent-<id>-*`, `admin:agent-<id>.*`, `grant:agent-<id>-*`, and `grant:agent-<id>.*` by default, with its certificate's key as holder: the two `grant` scopes let it delegate access to its own databases and its subtree, and nothing else. The commands that spawn and delegate are designed with the 0.8 CLI.

### 5. The edge: OIDC for people, an issuer for tokens

1. **Nodes verify only FleetRing tokens**, offline. They never fetch a JWKS, never accept an OIDC ID token, and never call an identity provider: verification stays a pure function of the token, the trust anchors, the denylist, and the time.
2. **An *issuer* turns a login into a token.** It runs the OIDC authorization-code flow with PKCE (and the device flow for `fleetring login`), verifies the ID token (issuer, audience, signature against the provider's JWKS, expiry, nonce), maps the provider's `(iss, sub)` to a principal through the identity registry, maps configured group claims to groups (only from issuers the registry trusts for groups), expands access roles (point 2.5), and issues a short-lived biscuit, 12 hours at most for a person. Wallet sign-in (EIP-4361) and passkeys are later login methods into the same mapping.
3. **The issuer signs with an *issuer key*, never the authority's.** The authority certifies the issuer key as a constrained trust anchor (point 6.1), naming the patterns and kinds it may issue for, so a stolen issuer key reaches only those. This replaces ADR-0025 point 3's sketch that delegated tokens would come from "a second issuer key": delegation needs no issuer at all (point 3); people logging in do.
4. **Where it runs.** First inside the router process (FR-066, #69), which already terminates HTTPS, verifies tokens, and is a client of the fleet; later as a standalone service. It needs no new protocol towards nodes. The enrollment flow of ADR-0043 point 8.9 builds on the same issuer.
5. **Outgoing JWTs.** Services beside the data that only accept JWTs can get an audience-bound JWT from the issuer, minted from a biscuit the caller presents. Later, if asked for.

### 6. Trust anchors, tenancy, and federation

1. **A fleet trusts a list of trust anchors, each a public key with a constraint and a purpose.**

   | Anchor | Purpose | Constraint |
   |---|---|---|
   | The fleet's authority | authorization: certificates and tokens | none |
   | An issuer key (point 5.3) | authorization: tokens | the patterns and kinds it may issue for |
   | Another fleet's authority (point 6.5) | authorization: that fleet's tokens | the patterns and actions its principals may hold here |
   | A publisher key (point 8) | provenance only: package manifests | the packages it may sign for; grants nothing |

   A token is accepted only if its authority block is signed by an *authorization* anchor and its scopes, intersected with that anchor's constraint, are not empty; the anchor's constraint is intersected into the effective scopes. Anchors are configuration on every node, as the authority's certificate is now, and the CLI keeps a separate trust store for provenance anchors. A publisher key can never authorize a request, and an authorization anchor never vouches for content.
2. **Organizations and tiers are prefixes.** A fleet that serves one organization uses the tiers as prefixes directly; a fleet shared by several gives each organization a namespace, `<org>/`, and puts the same tiers inside it.

   | Tier | One organization | Shared fleet | Who |
   |---|---|---|---|
   | Personal | `u-<user>-*` | `<org>/u-<user>-*` | the user |
   | Agent | `agent-<id>-*`, `agent-<id>.*` | `<org>/agent-<id>-*`, … | the agent and its ancestors |
   | Team | `team-<team>-*` | `<org>/team-<team>-*` | the team's group |
   | Organization | `org-*` | `<org>/org-*` | everyone in the organization |
   | Public | `pub-*` | `pub-<org>-*` | anyone, read only |

   - Organization names are `[a-z0-9-]{1,32}`, so `/` ends one. Public names sit outside any organization's namespace, so one public scope covers them.
   - A one-organization fleet that later becomes shared keeps its names as the *default organization*; only new organizations are namespaced. Shared and hosted fleets start namespaced.
   - Principal names stay fleet-unique; in a shared fleet the registry names them `<org>_u-<user>` and `<org>_agent-<id>`, which today's grammar already allows, never with `.` (point 1.8). Organization names hold no `_`, so the first `_` ends one, and a child of `acme_agent-42` is `acme_agent-42.c7`. An organization's membership is a registry fact carried in tokens, never parsed from the name.
3. **Groups and administration.** A group is a named set of principals in the identity registry. Access roles are assigned to principals and groups; the issuer expands both. A fleet operator (the `fleet` scope) creates organizations and names their administrators. An organization's administrator holds `admin` and `grant` over the organization's namespace and edits its groups, roles, and assignments, with every scope in them inside that namespace; a team's administrator likewise inside `team-<team>-`. The issuer checks these limits when it writes the registry, at its boundary, as nodes check scopes at theirs.
4. **Public read goes through the router, without a client certificate.** The router's configuration names public prefixes. A request without a token, for a database under one, acts as the router's own principal, whose `[[principal]]` entry holds `read` on exactly those prefixes. This amends ADR-0037 point 2's "with no token it can do nothing" for those prefixes only. The router rate-limits anonymous requests and logs them as anonymous on the audit target. Nodes still see a mutually authenticated router. `fork` is never public: forking creates a database, which needs `admin` on a target in some fleet anyway.
5. **Two shapes for more than one site.**
   - **One fleet spanning sites** is the default for one owner: a laptop and cloud nodes are members of one fleet with one authority, sites are zones or cells, and a laptop primary with cloud followers is placement, not trust. Nothing in this ADR changes for it.
   - **Two fleets that trust each other** is for different owners: teams, organizations, a personal fleet beside a company's. Each lists the other's authority as a constrained anchor for exactly the prefixes and actions it accepts, so trust is explicit, one-directional, and narrow: fleet B can accept `read` and `fork` on `team-x-*` from fleet A's principals without A gaining anything else in B. A foreign principal is recorded with its fleet, as a separate field of what records principals, not a new name grammar. Nodes shared across teams are one fleet with several organizations (point 6.2); its operators can read every database on its nodes, which `SECURITY.md`'s threat model must say once such fleets exist, and which per-tier encryption at rest would address later.
6. **Verification of history across fleets** builds on FR-074: a signed head is a database's chain hash at a position, signed by the node keys of a write quorum of its replicas, verifiable offline with the publishing fleet's authority certificate. Light-client proofs of control state (where a database lives, at which epoch) come later and use the same anchors.

### 7. Forking

1. **A fork is a new database** seeded from a source database's snapshot at a position, and independent from then on: its own name, placement, replicas, log, epochs, expiry, and archive. Nothing flows from the source to the fork, or back, after the fork is made. A fork's engine is its source's.
2. **A fork records where it came from**, in a new metadata field, `forked_from`, set only by the fork command:
   - the source's name and, for a source in another fleet or a package, its fleet or publisher;
   - the position, as the engine's opaque bytes;
   - the source's chain hash at that position;
   - the signed head covering that position, once FR-074 exists;
   - for a package, the publisher, the package, its version, and the manifest's digest (point 8);
   - the time.

   Like `created_by`, it grants nothing. It survives an archive and a restore as the archive's claim, verified only where a signed head or a manifest proves it.
3. **Permissions:** `fork` on the source and `admin` on the target name. As point 2.1 says, withholding `fork` while granting `read` meters copies and withholds the provenance-carrying copy; it doesn't keep the data from the reader. A fork from another fleet needs a token that fleet accepts for `fork` on the source (point 6.5), and `admin` here.
4. **How the copy is made is the engine adapter's choice**, behind one operation, so the control plane stays engine-neutral:
   - **File clone** (APFS `clonefile`, `FICLONE` on Btrfs and XFS) of a consistent database snapshot, when a node holds both the source and a replica of the fork. That is always true on a dev node and rarely true in a larger fleet, since a fork's name places it independently.
   - **A database snapshot streamed** from the source's primary to the fork's replicas, with the engine's consistent copy (for SQLite, its backup API or `VACUUM INTO` inside the adapter), which `Replication.Snapshot` already moves. A fork's replica fetching its source's snapshot is a new binding of ADR-0031 ("a replica of a fork being seeded, at the fork's epoch"), checked against control state like the others.
   - **Restore from an archive** at a position (ADR-0029), for a fork of a past position, of another fleet's database, or of a package.

   Page-level copy-on-write between live databases, sharing pages across a fork, is not part of this design.
5. **The command** is a control command, `ForkDatabase`, carrying the target's name, the source, and optionally a position (the source's current position by default), with the target's metadata. It is a fleet feature, `fork`. The target is created as any database is, then seeded, and opens for writes only once seeded; a seeding that fails leaves a database a `db show` reports as unseeded, which `db drop` removes.
6. **Agent memory is a fork.** A child's memory starts as a fork of its parent's (point 4). Whether a child inherits memory, and when it writes back, is the harness's and the agent's decision; FleetRing supplies the fork, the separate prefixes, and the scopes that keep the child off the parent's database.

### 8. Published packages

A *package* is a database a *publisher* releases in versions, for others to read or fork: documentation, decisions, examples, evaluation cases.

1. **A publisher is an Ed25519 key with a name**, held by a principal of the publishing fleet (conventionally a `service`). It is a provenance anchor (point 6.1): signing a package grants it nothing in any fleet, its own included.
2. **A package version is described by a *package manifest*** that the publisher signs: the publisher's name and key ID, the package, the version, the engine, the digest of each file that makes up the version's database snapshot (in the archive format, ADR-0029), the position and chain hash it was taken at, the digest of the previous version's manifest, and optionally a signed head from the publishing fleet together with that fleet's authority certificate.
3. **Verification is offline, anywhere, with the publisher's public key alone:** the manifest's signature, then every file's digest. A signed head adds an independent claim, that the publishing fleet's quorum held that history, checked with the authority certificate the signed manifest carries. Previous-version digests chain the versions, so a reader that holds any later manifest detects a publisher that serves a different manifest for an earlier version.
4. **Rollback and freeze protection, after TUF's snapshot and timestamp roles.**
   - Each manifest carries a `sequence`, strictly increasing per package, and an `expires`. A reader keeps the highest sequence it has verified for each publisher and package, and refuses an older one; installing an older version on purpose takes an explicit flag, which `forked_from` records. A manifest past its `expires` isn't installed.
   - A publisher may also sign a short-lived *latest statement*, lasting a day at most, naming its newest manifest's digest and sequence for each package, with a key of its own that it keeps online. A reader checking for updates requires a current one, so a mirror that withholds new versions (a freeze) is noticed within a day. Installing a pinned version needs only the manifest, offline.
   - A reader's trust store refuses to hold one key as both a provenance anchor and an authorization anchor.
5. **"Verified" means identity, never truth or safety.** It says the bytes are what the holder of a key the reader trusts signed. Readers pin publisher keys in their trust store explicitly; discovering and vouching for publishers (a well-known URL, DNS, a transparency log) is later work. A registry or marketplace is a later possibility only, and out of scope here.
6. **Installing a version is a fork** from the manifest (point 7.4's archive path), into a name the reader administers, with `forked_from` naming the publisher, package, version, and manifest digest.
7. **Upstream is replaceable, an overlay is additive, and the two are separate databases.** A reader's own notes on a package (local decisions, corrections, private constraints) live in an *overlay*: a separate database under a prefix the reader administers, never rows merged into the installed package. Upgrading installs the new version as a new database and retires the old one; the overlay is untouched, and refers to upstream content by the package's own stable identifiers. Combining the two happens in the client or the MCP server, since a statement can't reach a second database (ADR-0026 point 8).
8. **An agent reads a package and writes its overlay:** for example `read` on `team-eng-pkg-acme-sdk-*` and `admin` on `agent-42-overlay-*`. Nothing else is needed.
9. **Package content is untrusted.** Rows, schemas, and descriptions in a package are data written by a principal outside the reader's fleet, and instructions in them are never followed as commands. ADR-0041 point 9's marking extends to them: a description copied from a package is set without an author, so it is untrusted; MCP results from a database with `forked_from` naming a package carry its provenance (publisher, version, and whether the manifest verified) beside the existing notice. Executable content in packages is out of scope.

### 9. Where the code lives

1. **In `fleetringd::auth` through 0.8, as a pure module.** The token vocabulary, scope intersection, verification, and the trust-anchor check go into a submodule that imports no Tokio, no tonic, no clock, and no I/O, and takes the time as an argument, so that moving it later is a move, not a rewrite. Today every consumer (the daemon, the router, the MCP server, the CLI) is the same binary, and the vocabulary isn't proven yet; publishing a crate now would freeze an API before its first user outside this binary.
2. **Extract a pure workspace crate when the first consumer outside the binary appears:** the standalone issuer, an SDK, or a router built separately. It speaks of *resources* (names matched by prefix) and an open set of *actions*, so that the same tokens can guard services that run beside the data (an inference endpoint, a tool server), with FleetRing's database names and actions as one instance. Control-state types (`PrincipalName`, `Revoked`) stay in `fleetring-control`. Whether that crate is branded as a standalone auth layer (Authem, as ADR-0025 calls the idea) is decided then.
3. **The placement crate is untouched.** None of this is placement (ADR-0011).

### 10. Stages

| Release | Must do |
|---|---|
| **0.7** (the CLI, ADR-0043) | ADR-0043 already records `owner` and `parent` in `identity.toml`, refuses `.` in agent IDs, and reserves `fork` and `grant`. Three more: (1) **the identity version gate** of point 1.7: a reader takes `version` before the strict parse and refuses one above 1 with "this identity was written by a newer fleetring; upgrade", rather than an unknown-field error; (2) **database names starting with `_fleetring` are refused** at the request boundary (point 2.5); (3) **`cert client` and `token issue --principal` refuse a principal name that starts with `agent-` and contains `.`** (point 1.8). Tokens stay JWTs. |
| **0.8** (agent integration) | Biscuit tokens in the three templates, verified by FleetRing's own function and never by Datalog, with `token inspect` and the golden vectors; attenuation; delegation as third-party blocks signed by the holder key; holder keys, `Peer::HolderKey` and `Caller::Holder` behind `[tls] accept_holder_keys`, and the `SECURITY.md` changes; bearer delegation through the router; `Revoked::Block` and `Revoked::Principal` behind the `delegation` feature; `fork` and `grant` scopes; `.` refused in every new principal name; `agent add`'s default scopes grow by `admin:agent-<id>.*`, `grant:agent-<id>-*`, and `grant:agent-<id>.*`, with its certificate's key as holder. `db fork` within a fleet behind the `fork` feature, with `forked_from`, file clones on co-located replicas and streamed snapshots otherwise, and an MCP `db_fork` tool. `biscuit-auth` pinned exactly, its duplicates listed in `deny.toml`. This is the minimum for parent and child agents, and what agent-harness integrations build on. |
| **Later**, each when needed | FR-074's signed heads, then verified `forked_from` and trusted restored descriptions. The issuer: OIDC login, the identity registry, groups, access roles, organizations, and enrollment (ADR-0043 point 8.9). Public read through the router. Package manifests, publisher trust, and installing packages from archives. Constrained anchors for other fleets. More key types. Light-client proofs. A registry, only if demand shows. |

Each 0.8 and later item gets its own plan; the format of tokens gets `docs/spec/tokens-v1.md`, and `ForkDatabase` and the new denylist variants each get an ADR or an amendment where they touch the control log.

## Alternatives considered

- **Macaroons.** Attenuation is built in, but verification needs the root secret, so every verifying node could mint any token. Rejected for that alone.
- **JWT chains or signed sub-tokens of our own.** No new dependency, and JWT-shaped. Since FleetRing computes what a token allows itself (point 3.1), this is closer than it looks: what it lacks is a specified chain format with externally signed blocks and revocation IDs, and libraries that build blocks in agents' languages. Biscuit has both.
- **Running biscuit's Datalog authorizer,** with free Datalog or with checks in fixed templates. Free Datalog makes a node's decision depend on programs inside tokens, hard to show and to bound. Even fixed checks would make Datalog, rather than FleetRing's stated rules, the thing that enforces `grant` and holder keys, which it can't (they are about who signed a block and which key a connection proved), and the authorizer reads `std::time::Instant`, which ADR-0020 forbids. Facts only, read by FleetRing's own verifier, is smaller and honest about what enforces what.
- **Binding a delegated token to the holder only through the connection's certificate.** Anyone who copied a token's bytes could append a delegation block naming a key of their own, since a biscuit carries its next private key; signing the delegation block with the holder's key (point 3.3) closes that.
- **Setting the token's next key to the child's public key.** The token's proof would then need the child's private key, which the parent doesn't have, so the token would be assembled by hand outside the crate's API. Third-party blocks do the same job inside it.
- **Accepting holder-key certificates on every node by default.** It would make the token parser every node's first line of defence against the whole network. A setting that is off unless delegated agents connect directly keeps the handshake as the first filter everywhere else.
- **OAuth 2.0 token exchange (RFC 8693) for every child.** Standard, and fine for remote enrollment, but every spawn would need a reachable issuer holding a signing key, which a parent on a laptop with the fleet unreachable doesn't have.
- **UCAN.** Capability chains rooted in decentralized identifiers, wallet-friendly. Its specification moved to a new envelope format recently, its Rust implementations are less mature than biscuit's, and FleetRing's roots are an authority it already has, not decentralized identifiers.
- **Relationship-based authorization (Zanzibar, OpenFGA) or a policy language (Cedar, OPA).** Both need an online lookup or a policy store at every node, against offline verification and lookup-free nodes. Access roles that compile into scopes give people roles without either; a relationship graph could later feed the issuer.
- **Unbound (bearer) delegated tokens everywhere.** Simplest for clients, but `grant` would then be unenforceable, since a holder could hand on its token, and a leaked child token would be as good as the child. Bearer delegation stays, for the router, with a 24-hour window and no further delegation.
- **`.` as the organization separator in principal names** (`acme.agent-42`). It would collide with lineage, whose rule is "the parent's name, `.`, one segment". `_` is free for it, since organization names hold none.
- **Child agents with their own top-level IDs** (`agent-42_7`). Their prefixes wouldn't nest in the parent's, so a parent could never delegate their scopes without already holding them by name, and couldn't cover its subtree with one scope.
- **`read` implies `fork`.** Simpler, but a publisher couldn't allow queries without allowing whole, provenance-carrying copies.
- **Access roles in control state, or evaluated on nodes.** Every edit would be a control command and a fleet feature, and every node would need the registry; compiling at issuance keeps nodes unchanged and verification offline. The cost is that an edited role reaches a principal at its next token, which short lifetimes for people bound, and revocation covers the urgent case.
- **One fleet per organization only.** Simplest isolation, but nodes couldn't be shared across teams, and every small team would run a control plane. Namespaces in one fleet plus constrained anchors between fleets cover both.
- **Merging forks back, or overlays into packages, inside FleetRing.** Row merges need conflict rules, which the one-writer-per-database model rejects. Copying back is the application's explicit act.
- **Extracting a standalone auth crate now.** Clean, but it would freeze an API before the vocabulary has one real consumer outside this binary.

## Consequences

- An agent can give a child a narrower, shorter-lived credential, on its own machine, without the authority and without the network, and revoking any link ends everything below it within the denylist's existing bounds.
- Nodes verify two token formats for a while; the biscuit path adds a dependency of about 85 crates, 34 of them new to the workspace, pinned exactly, and duplicate versions that `deny.toml` lists by name until `biscuit-auth`'s next release.
- Where `[tls] accept_holder_keys` is on, a node's client listener accepts self-signed client certificates for holder keys, an exception to ADR-0025 point 1 that makes the token check, not the handshake, the first filter for such peers. `Peer` and `Caller` gain fail-closed variants, the permissive fall-through to `Caller::Anonymous` goes, and `SECURITY.md`'s network-attacker row and token sections change as point 3.6 says.
- What a token allows is FleetRing's own pure function, specified with golden vectors, so it is testable without biscuit's authorizer and the same on every platform and in the simulation.
- The denylist gains two variants, the scope grammar two actions, metadata one field, the control log one command, and ADR-0031 one binding, each a fleet feature or additive.
- People get roles, groups, and SSO without nodes changing, once the issuer exists; until then, nothing about people changes.
- A fork's provenance, and a package's, are recorded from the first fork; they become verifiable as FR-074 and manifests land, without changing the field.
- `auth::Grant`, the type for a token's contents, collides with the new `grant` action in prose and code; it is renamed (for example to `TokenSpec`) in 0.8.
- ADR-0043's decisions stand; point 10's three 0.7 requirements go into its plan's phase F.
- Principal names with `.` can no longer be issued, from 0.7 for `agent-` names and from 0.8 for all; existing ones keep working.
- The lexicon gains, marked *planned*: access role, delegation (with delegated token and attenuation), fork, group, holder key, identity registry, issuer, organization, overlay, owner (of an agent only), package (with package version, package manifest, and latest statement), parent, principal kind, publisher, revocation ID, signed head, and trust anchor; the entries for action, principal, token, and denylist note what is planned; and "Words we don't use" gains capability, branch and copy (for a fork), checkpoint (for a signed head), release (for a package's version), and role alone (now also access role).

## Open questions for the owner

Each has a recommendation; the body is written as if each recommendation is taken.

1. **The token format.** Biscuit's format and signatures, read by FleetRing's own verifier with no Datalog (point 3), or a JWT chain of our own. *Recommended: biscuit*, for its specified chain with third-party blocks and revocation IDs, and its libraries in agents' languages.
2. **Holder binding.** Delegation blocks signed by the delegating holder's key, children proving their key with a self-signed certificate behind `[tls] accept_holder_keys` (on in dev mode, off by default elsewhere), and bearer delegation only through the router with a 24-hour window (points 3.3 to 3.7). *Recommended: yes.* The alternative, bearer delegation everywhere, makes a copied token file as good as the agent.
3. **`fork` apart from `read`.** A separate action for metering and provenance-carrying copies, not confidentiality (point 2.1). *Recommended: yes.*
4. **Where access roles live.** The `_fleetring/identity` database, reachable with `fleet` alone, compiled into tokens at issue, with `Revoked::Principal` for an immediate removal (point 2.5); or control state. *Recommended: the database.*
5. **The shape of `grant`.** Pattern-scoped, `grant:<pattern>` (point 2.2), or a bare `grant`. *Recommended: pattern-scoped*, so an agent can delegate its subtree without delegating its own databases.
6. **The dependency.** Take `biscuit-auth` in 0.8, pinned exactly to the release current then, with its duplicate versions listed in `deny.toml`, or wait for a release with the dependency updates on its main branch. *Recommended: take it in 0.8*, and move to the newer release by a reviewed change when it ships.
7. **`.` and names.** `.` reserved for lineage in every newly issued principal name (from 0.7 for `agent-` names, from 0.8 for all), and `_` as the organization separator (points 1.8 and 6.2). *Recommended: yes.* The cost is that a new service can't be named `workload.prod`; existing names keep working.
8. **Package freshness.** Manifests with a sequence and an expiry, plus an optional latest statement lasting a day at most (point 8.4). *Recommended: yes*, as the minimum that stops rollback and freeze without a registry.

To unblock: "Accept 0044's recommendations."
