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
| `agent` | a program acting for an owner, possibly spawned by another agent | `agent-<id>` | a certificate or a holder key, plus a token (point 3.5) |
| `service` | any other program: an application, the router, CI, a publisher's signer | `svc-<name>`, or any name today's fleets already use | a client certificate, or a token |
| `node` | a `fleetringd` process | its node ID | its node certificate (ADR-0025 point 1) |

1. **A node is not a principal.** It keeps the separate identity, binding, and calls ADR-0026 point 9 and ADR-0031 give it. It shares only the key types of point 1.6 below.
2. **The principal name stays the identity everything records.** It is what `sub`, `created_by`, `description_by`, and audit lines hold, with today's grammar. Authorization never parses a name: kind, owner, and parent are attributes carried in the identity record and in the token (point 3), and the conventions above only make names readable. A principal with no recorded kind, which is every principal today, is a `service`.
3. **An agent records an owner and a parent.**
   - `owner` is the principal accountable for the agent, as ADR-0043 point 8.3 records it for a root agent: the principal the creating command ran as (`admin` in dev mode), or `authority` when only the authority's key was used. A child inherits its parent's owner, so every agent in a lineage answers to the same principal.
   - `parent` is the agent ID of the agent that spawned it (`42`, whose principal is `agent-42`), absent for a root agent, as ADR-0043 point 8.3 records it.
   - Both are recorded in the agent's `identity.toml` and, from 0.8, in the delegation block that created the agent's token (point 3.4), which names the parent by principal. Like `created_by`, neither grants anything: what an agent may do comes from its token alone.
4. **A child agent's ID extends its parent's with `.`:** agent `42` spawns `42.c7`, whose principal is `agent-42.c7`, and `42.c7` spawns `42.c7.r1`. Each segment is `[a-z0-9_]{1,32}`, as ADR-0043 point 8.5 defines a root ID, and the whole principal name stays within 64 bytes, which bounds the depth. ADR-0043's grammar has no `.`, so 0.7 already refuses `42.c7` as an agent ID and no 0.7 identity can collide with a lineage ID; 0.8 widens the grammar to `.`-separated segments, and only for identities a delegation creates.
5. **An agent's databases nest under its lineage.** A root agent's own databases are `agent-42-<purpose>`, as today. Its descendants' are `agent-42.<…>-<purpose>`, which the pattern `agent-42.*` covers and nothing else does. `.` is the separator here, as `-` is in `agent-42-*`: it isn't a letter or digit, so ADR-0041's warning doesn't fire, and `agent-42.*` can't cover `agent-420…`, whose byte after `42` is `0`. No segment holds `.` or `-`, so a sibling `c70` isn't covered by `agent-42.c7-*` or `agent-42.c7.*` either. A parent's token can therefore cover its whole subtree with one scope, and every scope it hands a child is a narrowing of one it holds (point 3.4).
6. **Keys have a type.** Every public key FleetRing stores or compares (a certificate's, a token root's, a holder's) is a `(key type, bytes)` pair, written `ed25519/<hex>`, and verification goes through one `PublicKey` trait with a `#[non_exhaustive]` `KeyType`. Ed25519 is the only type now, since `ring` already provides it and every key in a fleet is one. P-256 (passkeys), secp256k1 (wallets), and external subjects such as an OIDC issuer and subject are later *credentials* of a principal, mapped to it at the edge (point 5). None of them is ever a principal's name, so adding one changes no recorded name.
7. **One identity file across releases: `identity.toml`, versioned, strict.** ADR-0043 point 8.3 defines it, at `version = 1`, and its reader refuses unknown fields so that a damaged or hand-edited file fails loudly. That strictness stays. A release that adds a field (0.8: the holder key's file, the token's format, a delegated identity's lineage) writes `version = 2` for every identity that uses one, and keeps writing `version = 1`, field for field, for an identity a 0.7 reader can use as it is. Every reader takes `version` before the strict parse and refuses a version it doesn't know with an error that says to upgrade, so a 0.7 CLI meets a delegated identity with a clear refusal, not an unknown-field error and never a half-read credential. Tolerating unknown fields was rejected: a 0.7 client that ignored a holder key would present the wrong certificate and fail far from the cause.

### 2. Scopes: `fork` and `grant` join the actions

The actions become a lattice, with two additions:

| Action | Allows | Implies |
|---|---|---|
| `read` | queries, placement, the lease | |
| `write` | changing contents | `read` |
| `fork` | creating a fork *from* the database (point 7) | `read` |
| `admin` | creating, dropping, reconfiguring; creating a fork *into* the name | `write`, `fork`, `read` |
| `stream` | following changes | nothing |
| `grant` | delegating, to another holder, the token's other scopes narrowed to this pattern (point 3.4) | nothing |

1. **`fork` is its own action and implies `read`.** Forking copies a whole database at a consistent position, with its provenance, as one operation, which a publisher may want to allow or meter separately from queries. Anyone who may fork may read the fork, so `fork` claims nothing beyond `read` about confidentiality; it isn't implied by `read` or `write`. `admin` implies it.
2. **`grant` is its own action, implied by nothing, and covers a pattern.** `grant:agent-42.*` lets a holder delegate any scope it holds to another holder, narrowed to names under `agent-42.`. `admin` doesn't imply it: administering a prefix and handing out credentials for it are separate trusts. The `fleet` scope can never be delegated.
3. **The separator rule becomes a refusal for new token forms.** A pattern in a delegated token (point 3), in an access role (point 2.5), or in a trust anchor's constraint (point 6) must end, before its `*`, in a byte that isn't an ASCII letter or digit; anything else is refused. Today's JWTs and `[[principal]]` entries keep the warning of ADR-0041 point 5, so nothing that works today breaks.
4. **Scopes intersect.** Two patterns intersect to the longer one when one is a prefix of the other, and to nothing otherwise. Two actions intersect to the strongest action both imply. A set of scopes intersects with another pairwise. This is the only arithmetic delegation needs (points 3 and 4), it is computed in one pure function, and `fleetring token inspect` prints its result.
5. **Access roles are named bundles of scopes, compiled into tokens when they are issued.**
   - An access role is a name and a list of scope templates with parameters, such as `developer = ["admin:team-{team}-*", "read:org-*", "fork:pub-*"]`.
   - A role is assigned to a principal or a group (point 6.3) with its parameters bound.
   - The issuer (point 5) expands a principal's assignments into plain scopes when it issues a token. Nodes never see an access role and never look one up, so verification stays offline and lookup-free, and an edited role reaches a principal at its next token.
   - An access role can't contain the `fleet` scope or `grant` over a pattern its editor can't grant.
   - Roles, groups, assignments, and login mappings live in the *identity registry*: an ordinary FleetRing database, `_fleetring/identity`, read only by the issuer. Not in control state, because no node needs them, every change would be a new control command and a fleet feature, and control state should stay small. Not in a separate store, because a FleetRing database is replicated, archived, and failed over already, and a standalone auth layer can keep the same schema in a local SQLite file.
   - **Database names starting with `_fleetring` are reserved for FleetRing.** A client is refused creating one, at the request boundary only, so every log replays. This is a namespace of whole databases in control state, distinct from the `_fleetring_*` tables, indexes, and triggers that the SQLite adapter reserves *inside* every database (ADR-0041, `engine::sqlite`'s reserved namespace), which it keeps unchanged. From the release that creates the first one, a pattern that ends in `*` never matches a reserved name unless the pattern itself starts with `_fleetring`, so `admin:*` doesn't reach the identity registry.
   - Who edits what: principals with the `fleet` scope create organizations and their administrators, and an organization's administrators edit its groups and roles (point 6.3).

### 3. The token format: biscuit, in a fixed vocabulary

**Requirements.** A holder must be able to narrow its token for a child without holding the authority's key and without a network call. A node must verify the result offline with the authority's public key alone, as it does a JWT today. A child must never hold more than its parent, or for longer. Revoking a parent must revoke what it delegated. And because agents are written in Python and TypeScript as often as in Rust, an agent framework must be able to narrow a token in its own language.

| | Biscuit (v3) | Macaroons | JWT chains or signed sub-tokens |
|---|---|---|---|
| Narrowing offline, without the authority | yes: append a block, signed with the key the token carries | yes: chained HMAC caveats | yes, if each holder has a key pair and signs the next link |
| Verification | public key: anywhere, offline | the root *secret*: every verifier can mint | public keys of each link |
| Child ⊆ parent | by construction: every block's checks must pass | by construction | the verifier must recompute it; a format of our own |
| Revocation handles | a revocation ID per block, specified | none specified | ours to design |
| Libraries in agents' languages | Rust, Go, Java, Python, Swift, Haskell, and JavaScript through WebAssembly | few and mostly unmaintained | JWT libraries everywhere; chain verification nowhere |
| Cost | Datalog engine, a heavier dependency | small | small, on `ring`, but a protocol of our own |

**Biscuit wins.** Macaroons lose on their shared secret: every node that verifies could mint, which ADR-0026 point 9's "a stolen node key reaches only its own databases" would no longer hold for credentials. JWT chains would work, but they are a cryptographic protocol of our own, with no standard verifier and no library in any agent's language; biscuit is that protocol, specified, with revocation IDs, sealing, and third-party blocks (with which an issuer or another fleet could later vouch for one statement inside a token), and with implementations an agent framework can use to narrow a token itself. Its cost, the Datalog engine, is contained by the vocabulary below.

**The crate.** `biscuit-auth` 6.0.0 (16 July 2025) is the newest release; it is Apache-2.0, which `deny.toml` allows; it declares no `rust-version`, and it builds with Rust 1.88 and 1.89 with default features (checked on aarch64 macOS, 5 November 2026). The project now lives under the Eclipse Foundation (`eclipse-biscuit/biscuit-rust`) and is active, with commits through late October 2026. Its weight is the main cost: about 85 crates in its tree with default features, of which 34 aren't in the workspace today (among them `ed25519-dalek`, a second Ed25519 implementation beside `ring`, and the `p256`/`ecdsa` stack), plus 20 second versions of crates already here, among them `prost` 0.10 beside the workspace's 0.14, `sha2` 0.9, `base64` 0.13, and `rand` 0.8. Its unreleased main branch already moves to `prost` 0.14, `rand` 0.10, `base64` 0.22, and `ed25519-dalek` 3. Its authorizer reads `std::time::Instant` to enforce a time limit, which ADR-0020 forbids in daemon code; FleetRing sets that limit high enough never to trip and bounds evaluation through the vocabulary instead, so the outcome never depends on a clock. Appending a block draws an ephemeral key from the OS; FleetRing calls `append_with_keypair` with a key from the injected generator.

1. **A fixed vocabulary, refused otherwise.** FleetRing tokens use biscuit's format and signatures, and Datalog only in a small set of block *templates*, specified with golden vectors in a normative `docs/spec/tokens-v1.md` (as `placement-v1.md` is for placement, ADR-0008). A node recognizes each block as an instance of one template and refuses a token with any other fact, rule, check, or scope annotation, as ADR-0026 point 5 refuses unknown JWT claims. Any biscuit authorizer that supplies the operation, the database, and the time still enforces every restriction correctly, so outside verifiers stay possible, but FleetRing never evaluates arbitrary Datalog.
2. **The authority block** is signed by a trust anchor (point 6.1); for a fleet's own tokens that is the authority's Ed25519 key, the same key that signs JWTs today. It states the fleet, the principal, its kind, an agent's owner, the token ID (as `jti` today, so `Revoked::Token` still applies), the scopes, `not_before`, `expires` (at most 30 days later, as today), the issue time, and optionally a holder key (point 3.5).
3. **An attenuation block** may be appended by any holder. It states scopes and an expiry; the token's effective scopes become the intersection (point 2.4) and its expiry the earlier one. A block can only narrow: a child's scopes are a subset of its parent's and its expiry no later, by construction, whatever the child's tooling does. The CLI refuses to append a block that would narrow to nothing.
4. **A delegation block** is an attenuation block that also names a new holder: the child's principal, its parent (the holder before it), and its holder key (point 3.5). A node accepts it only if the effective scopes just before it include `grant`, and then also intersects everything after it with the patterns those `grant` scopes cover, so a holder delegates only what it was allowed to delegate; `grant` survives into the child only if the block lists it again, narrowed. Only an agent delegates to a new holder, and the child's principal must be the delegating agent's principal followed by `.` and one segment (`agent-42` names `agent-42.c7`), so a holder can't name a child as anyone else, and every principal in a chain is trustworthy in audit lines and in `created_by`. A user or service starts a root agent with `agent add` (ADR-0043) or, later, through the issuer, and narrows its own token for an application by attenuation. The chain of delegation blocks *is* the lineage: every request's audit line records the principals from the root to the holder, with each block's revocation ID, and `created_by` records the holder.
5. **Delegated tokens are bound to the holder's key.** Biscuit tokens are bearer tokens unless bound, and `grant` would mean nothing if a holder could simply hand its token on. So:
   - A child generates its own Ed25519 key, on its own machine, and the parent's delegation block names its public key. No key file is ever copied.
   - On a direct connection to a node, the child presents a self-signed client certificate for that key. This amends ADR-0025 point 1 for one case: a node's client listener accepts a client certificate the authority didn't issue, and the connection then has no principal and can carry only a token whose current holder key is that certificate's key. Every request on it without such a token is refused. Node certificates and principals' certificates are verified against the authority, as before.
   - This generalizes ADR-0026 point 3: a token is accepted on a connection whose client certificate names the token's holder principal (today's rule, for authority-issued certificates) or carries the token's holder key, or from a relay with `forward_tokens`.
   - **Bearer delegation through the router only.** A delegation block may name no holder key, so that an unmodified libSQL client can use it through the router (ADR-0037), which can't see a client key. Such a token lasts at most 24 hours from its last delegation block, and nodes accept it only from a relay with `forward_tokens`, never on a direct connection.
6. **Limits.** A token holds at most 8 blocks and at most 8 KiB (`MAX_TOKEN_LEN`, unchanged).
7. **Revocation.**
   - `Revoked::Block(RevocationId)`: a block's revocation ID is the SHA-256 of its signature, base64url (43 characters, within the denylist's ID grammar). A node refuses a token if *any* of its blocks is on the denylist, so revoking a parent's token, or the block that delegated to a child, revokes every token derived from it.
   - `Revoked::Principal { principal, before }`: refuses every token, JWT or biscuit, whose chain names the principal at any position and whose authority block was issued before `before`. Revoking `agent-42` therefore ends `agent-42`, `agent-42.c7`, and every descendant's current tokens at once, and new tokens issued after the revocation work again.
   - Both are kept as `Revoked::Token` is: 30 days and 30 seconds after they are recorded, the longest any token they could refuse may live (ADR-0026 point 5). They travel and fail closed exactly as today's denylist does (ADR-0026 point 6).
8. **Coexistence with today's tokens.**
   - Nodes accept both formats on the same `authorization: Bearer` header, distinguished by shape (a JWS has three dot-separated parts; a biscuit has none). JWTs keep working unchanged, keep `token issue`'s default until 1.0, and stay the format for principals that never delegate. Whether 1.0 changes the default is left to then.
   - **Gating (ADR-0038 point 4).** The new denylist variants are new control state and travel in new control commands, so they are a fleet feature, `delegation`, enabled by the first revocation that uses one. Accepting a biscuit token changes no control state, so it isn't gated, but a node of the previous line refuses one; so every node reports `delegation` in its software features once it can verify them, and `token issue --format biscuit`, `agent add`, and delegation refuse to mint while a member doesn't report it, naming the member, as `upgrade --check` names one.
   - A fleet that enables `delegation` loses rollback to the previous line, as with any feature.
9. **Expiry and renewal.** A child's token can never outlive its parent's token. A renewed parent doesn't renew its children: it delegates to them again. ADR-0043's renew-at-start rule applies to a root agent's token; a child's harness asks its parent.

### 4. Delegation, end to end

A parent agent `42`, holding `admin:agent-42-*`, `admin:agent-42.*`, `grant:agent-42-*`, and `grant:agent-42.*`, spawns child `c7`, which may fork the parent's memory:

1. The child's harness creates `<agents_dir>/42.c7/`, generates the child's holder key there, and passes the public key to the parent.
2. The parent appends a delegation block to its own token: holder `agent-42.c7`, parent `agent-42`, the child's key, scopes `admin:agent-42.c7-*`, `fork:agent-42-memory`, and an expiry of two hours. It writes the token and the child's `identity.toml` (`version = 2`, `owner` as the parent's, `parent = "42"`, and the holder key's file) to the child's directory. No authority, no network.
3. The child forks `agent-42-memory` into `agent-42.c7-memory` (point 7), uses it, and creates scratch databases under `agent-42.c7-`. It can't write the parent's memory, can't touch a sibling's databases, and can't delegate further, since its token holds no `grant`.
4. The parent, whose `admin:agent-42.*` covers the child's databases, reads the child's memory afterwards and copies back what it wants. Copying back is the application's choice, row by row; FleetRing never merges databases.
5. Revoking the parent's delegation block, or `agent-42.c7` as a principal, ends the child; revoking `agent-42` ends both.

In 0.8, `agent add` gives a root agent `admin:agent-<id>-*`, `admin:agent-<id>.*`, `grant:agent-<id>-*`, and `grant:agent-<id>.*` by default: the second pair lets it delegate access to its own databases and its subtree, and nothing else. The commands that spawn and delegate are designed with the 0.8 CLI.

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
   - Principal names stay fleet-unique; in a shared fleet the registry names them `<org>.u-<user>` and `<org>.agent-<id>`, which today's grammar already allows. An organization's membership is a registry fact carried in tokens, never parsed from the name.
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
3. **Permissions:** `fork` on the source and `admin` on the target name. A fork from another fleet needs a token that fleet accepts for `fork` on the source (point 6.5), and `admin` here.
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
4. **"Verified" means identity, never truth or safety.** It says the bytes are what the holder of a key the reader trusts signed. Readers pin publisher keys in their trust store explicitly; discovering and vouching for publishers (a well-known URL, DNS, a transparency log) is later work. A registry or marketplace is a later possibility only, and out of scope here.
5. **Installing a version is a fork** from the manifest (point 7.4's archive path), into a name the reader administers, with `forked_from` naming the publisher, package, version, and manifest digest.
6. **Upstream is replaceable, an overlay is additive, and the two are separate databases.** A reader's own notes on a package (local decisions, corrections, private constraints) live in an *overlay*: a separate database under a prefix the reader administers, never rows merged into the installed package. Upgrading installs the new version as a new database and retires the old one; the overlay is untouched, and refers to upstream content by the package's own stable identifiers. Combining the two happens in the client or the MCP server, since a statement can't reach a second database (ADR-0026 point 8).
7. **An agent reads a package and writes its overlay:** for example `read` on `team-eng-pkg-acme-sdk-*` and `admin` on `agent-42-overlay-*`. Nothing else is needed.
8. **Package content is untrusted.** Rows, schemas, and descriptions in a package are data written by a principal outside the reader's fleet, and instructions in them are never followed as commands. ADR-0041 point 9's marking extends to them: a description copied from a package is set without an author, so it is untrusted; MCP results from a database with `forked_from` naming a package carry its provenance (publisher, version, and whether the manifest verified) beside the existing notice. Executable content in packages is out of scope.

### 9. Where the code lives

1. **In `fleetringd::auth` through 0.8, as a pure module.** The token vocabulary, scope intersection, verification, and the trust-anchor check go into a submodule that imports no Tokio, no tonic, no clock, and no I/O, and takes the time as an argument, so that moving it later is a move, not a rewrite. Today every consumer (the daemon, the router, the MCP server, the CLI) is the same binary, and the vocabulary isn't proven yet; publishing a crate now would freeze an API before its first user outside this binary.
2. **Extract a pure workspace crate when the first consumer outside the binary appears:** the standalone issuer, an SDK, or a router built separately. It speaks of *resources* (names matched by prefix) and an open set of *actions*, so that the same tokens can guard services that run beside the data (an inference endpoint, a tool server), with FleetRing's database names and actions as one instance. Control-state types (`PrincipalName`, `Revoked`) stay in `fleetring-control`. Whether that crate is branded as a standalone auth layer (Authem, as ADR-0025 calls the idea) is decided then.
3. **The placement crate is untouched.** None of this is placement (ADR-0011).

### 10. Stages

| Release | Must do |
|---|---|
| **0.7** (the CLI, ADR-0043) | ADR-0043 already records `owner` and `parent` in `identity.toml`, refuses `.` in agent IDs, and reserves `fork` and `grant`. Two more: (1) **the identity version gate** of point 1.7: a reader takes `version` before the strict parse and refuses one above 1 with "this identity was written by a newer fleetring; upgrade", rather than an unknown-field error; (2) **database names starting with `_fleetring` are refused** at the request boundary (point 2.5). Tokens stay JWTs. |
| **0.8** (agent integration) | Biscuit tokens in the fixed vocabulary, with `token inspect`; attenuation and delegation, holder keys and self-signed holder certificates, bearer delegation through the router; `Revoked::Block` and `Revoked::Principal` behind the `delegation` feature; `fork` and `grant` scopes; `agent add`'s default scopes grow by `admin:agent-<id>.*`, `grant:agent-<id>-*`, and `grant:agent-<id>.*`. `db fork` within a fleet behind the `fork` feature, with `forked_from`, file clones on co-located replicas and streamed snapshots otherwise, and an MCP `db_fork` tool. This is the minimum for parent and child agents, and what agent-harness integrations build on. |
| **Later**, each when needed | FR-074's signed heads, then verified `forked_from` and trusted restored descriptions. The issuer: OIDC login, the identity registry, groups, access roles, organizations, and enrollment (ADR-0043 point 8.9). Public read through the router. Package manifests, publisher trust, and installing packages from archives. Constrained anchors for other fleets. More key types. Light-client proofs. A registry, only if demand shows. |

Each 0.8 and later item gets its own plan; the format of tokens gets `docs/spec/tokens-v1.md`, and `ForkDatabase` and the new denylist variants each get an ADR or an amendment where they touch the control log.

## Alternatives considered

- **Macaroons.** Attenuation is built in, but verification needs the root secret, so every verifying node could mint any token. Rejected for that alone.
- **JWT chains or signed sub-tokens of our own.** No new dependency, and JWT-shaped. But it is a cryptographic protocol to design, specify, and review ourselves, with no verifier or library outside this repository, and agents in other languages couldn't narrow a token without calling our binary. Biscuit is this protocol, already specified and implemented in agents' languages.
- **Biscuit with free Datalog.** Maximum expressiveness, but a node's decision would depend on arbitrary programs inside tokens, which makes what a token allows hard to show and evaluation hard to bound. The fixed vocabulary keeps the format and drops the open-endedness; widening it later is additive.
- **OAuth 2.0 token exchange (RFC 8693) for every child.** Standard, and fine for remote enrollment, but every spawn would need a reachable issuer holding a signing key, which a parent on a laptop with the fleet unreachable doesn't have.
- **UCAN.** Capability chains rooted in decentralized identifiers, wallet-friendly. Its specification moved to a new envelope format recently, its Rust implementations are less mature than biscuit's, and FleetRing's roots are an authority it already has, not decentralized identifiers.
- **Relationship-based authorization (Zanzibar, OpenFGA) or a policy language (Cedar, OPA).** Both need an online lookup or a policy store at every node, against offline verification and lookup-free nodes. Access roles that compile into scopes give people roles without either; a relationship graph could later feed the issuer.
- **Unbound (bearer) delegated tokens everywhere.** Simplest for clients, but `grant` would then be unenforceable, since a holder could hand on its token, and a leaked child token would be as good as the child. Bearer delegation stays, for the router, short-lived.
- **Child agents with their own top-level IDs** (`agent-42_7`). Their prefixes wouldn't nest in the parent's, so a parent could never delegate their scopes without already holding them by name, and couldn't cover its subtree with one scope.
- **`read` implies `fork`.** Simpler, but a publisher couldn't allow queries without allowing whole, provenance-carrying copies.
- **Access roles in control state, or evaluated on nodes.** Every edit would be a control command and a fleet feature, and every node would need the registry; compiling at issuance keeps nodes unchanged and verification offline. The cost is that an edited role reaches a principal at its next token, which short lifetimes for people bound, and revocation covers the urgent case.
- **One fleet per organization only.** Simplest isolation, but nodes couldn't be shared across teams, and every small team would run a control plane. Namespaces in one fleet plus constrained anchors between fleets cover both.
- **Merging forks back, or overlays into packages, inside FleetRing.** Row merges need conflict rules, which the one-writer-per-database model rejects. Copying back is the application's explicit act.
- **Extracting a standalone auth crate now.** Clean, but it would freeze an API before the vocabulary has one real consumer outside this binary.

## Consequences

- An agent can give a child a narrower, shorter-lived credential, on its own machine, without the authority and without the network, and revoking any link ends everything below it within the denylist's existing bounds.
- Nodes verify two token formats for a while; the biscuit path adds a dependency of about 85 crates, 34 of them new to the workspace, and duplicate versions that cargo-deny reports as warnings until `biscuit-auth`'s next release.
- A node's client listener accepts self-signed client certificates for holder keys, a narrow, tested exception to ADR-0025 point 1, which `SECURITY.md`'s threat model must describe: such a connection can do exactly what its holder-bound token allows, and nothing without one.
- The denylist gains two variants, the scope grammar two actions, metadata one field, the control log one command, and ADR-0031 one binding, each a fleet feature or additive.
- People get roles, groups, and SSO without nodes changing, once the issuer exists; until then, nothing about people changes.
- A fork's provenance, and a package's, are recorded from the first fork; they become verifiable as FR-074 and manifests land, without changing the field.
- `auth::Grant`, the type for a token's contents, collides with the new `grant` action in prose and code; it is renamed (for example to `TokenSpec`) in 0.8.
- ADR-0043's decisions stand; point 10's two 0.7 requirements go into its plan's phase F.
- The lexicon gains the words below, marked *planned*.
