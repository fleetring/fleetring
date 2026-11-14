# FleetRing token spec, version 1

- **Status:** Accepted with [ADR-0047](../adr/0047-online-delegation.md), for 0.8.0 (FR-090, FR-091). Its results never change: a change to what any vector expects needs a new spec version, and, where `apply` decides with it, a fleet feature (§9).
- **Normative:** yes. The key words MUST, MUST NOT, SHOULD, and MAY are used as in RFC 2119.
- **Conformance:** an implementation conforms if it reproduces every case in [`crates/fleetring-control/tests/vectors/tokens-v1.json`](../../crates/fleetring-control/tests/vectors/tokens-v1.json). The vector file's `layout` field describes its format. The reference implementation is `fleetring_control::{scope, matcher, delegation}` and `Revocations` for §2 to §5, §7, and §8, and `fleetringd::auth` for the signing of §6. The signed tokens of §6 were produced by a second, independent implementation and match the reference byte for byte.
- **Decisions behind this spec:** [ADR-0025](../adr/0025-principals-and-scopes.md) and [ADR-0026](../adr/0026-scope-enforcement-details.md) (principals, scopes, tokens), [ADR-0044](../adr/0044-identity-delegation-and-forking-for-agents.md) §2 (the scope lattice), [ADR-0047](../adr/0047-online-delegation.md) (online delegation), and [ADR-0038](../adr/0038-version-compatibility-and-upgrades.md) point 4 (fleet features).

A FleetRing token is a JWT that names a principal and carries its scopes. The fleet's authority signs root agents', services', and people's tokens. A control node's delegation issuer signs a delegated agent's token, after the control plane has recorded the delegation and its lineage. Every node verifies both kinds with the authority's public key alone: an issuer-signed token through the issuer certificate it carries.

This spec fixes what every node, every control replica, and every signer must compute identically: the scope grammar and lattice, agent names, token and delegation IDs, the token's encoding, the checks of an issuer-signed token, and how the denylist judges a token by its lineage. `apply` decides with §2, §3, §4, and §7, so two control replicas that disagree on any of them would apply the same command two ways (ADR-0047 point 6.5).

Words follow the [lexicon](../lexicon.md): *root agent*, *delegated agent*, *delegation issuer*, *issuer certificate*, *delegation ID*, and *lineage*. An *authority-signed* token carries no `x5c`; an *issuer-signed* token does.

## 1. Conventions

- Text is UTF-8. Comparisons, prefixes, and sorting are byte for byte, with no Unicode normalization or case folding.
- "base64url" is RFC 4648 §5 without padding; "base64" is RFC 4648 §4, the standard alphabet, with padding.
- Times inside a token are whole seconds since the Unix epoch. Times in control state, and in this spec's vectors, are milliseconds; a token's time `t` is `t × 1000` milliseconds there.
- Floating point MUST NOT be used anywhere.

## 2. Scopes

### 2.1 Grammar

A scope is the word `fleet`, or `<action>:<pattern>`:

- **The action** is one of `read`, `write`, `fork`, `admin`, `stream`, and `grant`, exactly; case matters.
- **The `:`** is the first `:` of the text. Everything after it is the pattern, so the pattern may hold further `:`s.
- **The pattern** is any non-empty text. If it ends in `*`, it is `Prefix(p)`, where `p` is the text before that last `*`; otherwise it is `Exact(n)`, where `n` is the whole pattern. A `*` anywhere else is an ordinary byte. `*` alone is `Prefix("")`, and `**` is `Prefix("*")`.

Text that is neither `fleet` nor contains `:` is refused (*shape*); an action outside the list is refused (*unknown action*), `fleet:x` included; an empty pattern is refused (*empty pattern*).

**Canonical text.** A scope's canonical text is `fleet`, `<action>:<n>` for `Exact(n)`, or `<action>:<p>*` for `Prefix(p)`. Every text the grammar accepts is the canonical text of the scope it parses to, and no two texts parse to the same scope. ADR-0047 point 6.5 has `apply` refuse scope text that isn't the canonical form of what it parses to; for a single scope that always holds, so what `apply` can refuse is a non-canonical *set* (§3.3).

| Scope text | Result |
|---|---|
| `read:tenant:acme/*` | accepted: `read` over the prefix `tenant:acme/` |
| `write:ledger` | accepted: `write` over the exact name `ledger` |
| `admin:*` | accepted: `admin` over every name |
| `fleet` | accepted: the fleet scope |
| `read:a:b:c` | accepted: `read` over the exact name `a:b:c` |
| `admin:agent-*-x` | accepted: `admin` over the exact name `agent-*-x` |
| `admin:agent-**` | accepted: `admin` over the prefix `agent-*` |
| `admin:agent-42.c7_*` | accepted: `admin` over the prefix `agent-42.c7_` |
| `read` | refused: `shape` |
| `cluster` | refused: `shape` |
| `fleet:x` | refused: `unknown_action` |
| `Read:x` | refused: `unknown_action` |
| `read:` | refused: `empty_pattern` |

### 2.2 Matching

`Exact(n)` matches the database name `n`; `Prefix(p)` matches every name whose bytes start with `p`. A pattern's text is not checked against the database name grammar.

Names reserved for FleetRing (`_fleetring…`, ADR-0044 point 2.5) are matched as bytes like any other in version 1, so `*` matches them. When the exclusion ADR-0044 point 2.5 asks for ships, it changes `covers`, `meets`, and the grant step for those names, which is a new spec version and a fleet feature.

### 2.3 What a scope allows

`fleet` allows the control operations that name no database. A database scope allows a request to take an action on a database its pattern matches when the scope's action covers the request's action (§3.1), except that `grant` allows no request at all: it only bounds what a token may delegate (ADR-0044 point 2.2). Everything not allowed is denied.

## 3. The lattice

### 3.1 Actions

An action *covers* the actions it implies and itself:

| Action | Covers |
|---|---|
| `admin` | `admin`, `write`, `fork`, `read` |
| `write` | `write`, `read` |
| `fork` | `fork`, `read` |
| `read` | `read` |
| `stream` | `stream` |
| `grant` | `grant` |

The *meet* of two actions is the strongest action both cover: `admin ∧ x = x` for `x` in `read`, `write`, `fork`, and `admin`; `write ∧ fork = read`; `read ∧ write = read ∧ fork = read`; `stream ∧ stream = stream`; `grant ∧ grant = grant`; every other pair has no meet.

### 3.2 Covering and meeting scopes

A pattern covers another when it matches every name the other matches: `Prefix(p)` covers `Prefix(q)` and `Exact(q)` when `q` starts with `p`; `Exact(n)` covers only `Exact(n)`.

A database scope covers another when its action covers the other's action and its pattern covers the other's pattern. `fleet` covers only `fleet`, and no database scope covers `fleet`. So `admin:a-*` covers `read:a-x` and `fork:a-x-*`, and neither `-` nor `.` stands for the other: `admin:agent-42-*` doesn't cover `admin:agent-42.c1-*`.

The *meet* of two patterns is the one of them the other covers, or nothing when neither covers the other: the exact name under the prefix, or the longer of two prefixes one of which starts with the other. The meet of two database scopes is the meet of their actions over the meet of their patterns, or nothing if either is nothing. `fleet ∧ fleet = fleet`; `fleet` meets no database scope.

### 3.3 The canonical form of a set

The canonical form of a set of scopes keeps every scope no other scope in the set covers, without repeats, sorted by canonical text byte for byte. It allows exactly what the set allows. A set is *canonical* when it equals its canonical form.

The scopes of an issuer-signed token are in canonical form (ADR-0047 point 2.2), and `apply` refuses a recorded delegation whose scopes aren't (point 5.9). Nodes don't require canonical form of any token.

### 3.4 Intersection

The intersection of two sets is the canonical form of every non-empty meet of a scope of one with a scope of the other. Both sides are first put in canonical form. It is commutative, idempotent, and never wider than either side.

### 3.5 The grant step

The grant step is what a holder with *effective* scopes may hand a new holder that asks for *requested* scopes:

1. If the effective scopes hold no `grant` scope, the grant step is refused (*no grant*).
2. Intersect the effective and requested scopes (§3.4).
3. Meet the pattern of each resulting database scope with each pattern the effective `grant` scopes hold, keeping the action; drop `fleet`.
4. The result is the canonical form of the non-empty results. If it is empty, the grant step is refused (*empty*).

The grant step narrows silently. The *strict* grant step never narrows: it refuses with *no grant* as above, with *empty* when the requested set is empty, and otherwise returns the requested set in canonical form if every requested scope, in canonical order, is covered by a scope of the grant step's result; if not, it refuses with *not covered*, naming the first requested scope, in canonical order, that isn't. `fleet` is never kept, so asking for it is refused. The control plane mints with the strict grant step (ADR-0047 point 5.3).

ADR-0044 §2.4's golden table, as written. The vectors carry each row, with the strict grant step's result beside each grant step's:

| Effective | Block | Operation | Result |
|---|---|---|---|
| `read:agent-42-*` | `write:agent-42-memory` | intersect | `read:agent-42-memory` |
| `admin:agent-42-*` | `fork:agent-42-mem-*` | intersect | `fork:agent-42-mem-*` |
| `read:agent-42-mem` | `read:agent-42-memory-*` | intersect | nothing: the exact name isn't under the prefix |
| `read:agent-42-memory-*` | `read:agent-42-mem` | intersect | nothing |
| `read:agent-42-memory` | `read:agent-42-*` | intersect | `read:agent-42-memory` |
| `write:agent-42-*` | `fork:agent-42-*` | intersect | `read:agent-42-*` |
| `stream:t-*` | `read:t-*` | intersect | nothing |
| `admin:a-*`, `admin:b-*` | `read:b-x-*`, `read:c-*` | intersect | `read:b-x-*` |
| `fleet`, `read:a-*` | `fleet` | intersect | `fleet` |
| `fleet`, `admin:agent-42.*`, `grant:agent-42.*` | `fleet`, `admin:agent-42.c7-*` | grant step | `admin:agent-42.c7-*` |
| `admin:agent-42-*`, `admin:agent-42.*`, `grant:agent-42.*` | `admin:agent-42.c7-*`, `read:agent-42-memory` | grant step | `admin:agent-42.c7-*`: the memory isn't under the `grant` |
| `admin:agent-42-*`, `admin:agent-42.*`, `grant:agent-42-*`, `grant:agent-42.*` | `admin:agent-42.c7-*`, `fork:agent-42-memory`, `grant:agent-42.c7.*` | grant step | `admin:agent-42.c7-*`, `fork:agent-42-memory`, `grant:agent-42.c7.*` |
| `admin:agent-42.*` (no `grant`) | `admin:agent-42.c7-*` | grant step | refused: no `grant` |

### 3.6 Bounds

The intersection and the grant step refuse a side of more than 256 scopes (`MAX_SCOPES`), and no signer signs a token with more than 256 scopes or with none. Version 1 gives no vectors for the bound.

## 4. Agent names

### 4.1 Grammar and depth

An agent name is a principal name (1 to 64 bytes of `a-z`, `0-9`, `-`, `_`, and `.`) of the form `agent-<id>(.<segment>){0,8}`, where the ID and every segment are 1 to 32 bytes of `a-z`, `0-9`, and `_`. With no segment it is a *root agent name*; with one to eight it is a *delegated agent name*. Neither the ID nor a segment holds `-` or `.`, so in `agent-<id>-` and `agent-<id>.` the ID ends before that byte.

A name's *depth* is its number of `.`s, 0 to 8 (`MAX_DEPTH`). Its *root* is `agent-<id>`; its *parent*, for a delegated name, is the name less its last `.<segment>`. A name is refused, in this order, if it isn't a principal name (*principal*), doesn't start with `agent-` (*not agent*), has an ID or segment outside the grammar (*segment*), or is deeper than 8 (*too deep*). The depth bound holds apart from the 64-byte bound.

| Name | Result |
|---|---|
| `agent-42` | depth 0 (a root agent name) |
| `agent-42.c7` | depth 1 |
| `agent-42.c7.r1` | depth 2 |
| `agent-42.a.b.c.d.e.f.g.h` | depth 8 |
| `agent-42.c7_x` | depth 1 |
| `agent-42.a.b.c.d.e.f.g.h.i` | refused: `too_deep` |
| `agent-42.` | refused: `segment` |
| `agent-42.c-7` | refused: `segment` |
| `agent-42-x` | refused: `segment` |
| `agent-42.C7` | refused: `principal` |
| `workload.prod` | refused: `not_agent` |

A name that starts with `agent-` and holds `.` is a *reserved dotted agent name* (ADR-0044 point 1.8). Every delegated agent name is one; a reserved dotted agent name such as `agent-42.c-7` that isn't an agent name is neither signed by an issuer nor accepted from the authority.

### 4.2 At or below

A principal name is *at or below* `Q` when it is `Q`, or starts with `Q` followed by `.`. So `agent-42.c7.r1` is at or below `agent-42` and `agent-42.c7`, while `agent-420` and `agent-42-x` aren't at or below `agent-42`, and neither `agent-42.c70` nor `agent-42.c7_x` is at or below `agent-42.c7`. A dotted service name such as `workload.prod` is at or below `workload`. A principal revocation (§8.2) and the connection rule for issuer-signed tokens (ADR-0047 point 4.1) compare names this way.

## 5. IDs

### 5.1 Token IDs and key IDs

A token ID (`jti`) and a key ID (`kid`) are each 1 to 64 bytes of `A-Z`, `a-z`, `0-9`, `-`, and `_`. They are separate namespaces: a denylist entry for a token ID never matches a key ID, and the reverse.

The key ID of an Ed25519 key is the base64url of the first 12 bytes of the SHA-256 digest of its raw 32-byte public key: 16 characters. Every signer derives it the same way, for the authority's keys and for issuer keys alike.

### 5.2 Delegation IDs

A delegation ID, a delegated token's `jti`, is the base64url of 16 bytes drawn by the proposing control node: exactly 22 characters. Only the canonical encoding is accepted, so each ID has one spelling: the last character encodes two data bits followed by four zero bits, so it MUST be `A`, `Q`, `g`, or `w`. `apply` keys delegation records by this ID, and a second spelling would name a second record. Every delegation ID is a token ID; most token IDs are not delegation IDs.

| Delegation ID | Result |
|---|---|
| `AAAAAAAAAAAAAAAAAAAAAA` | accepted: 16 zero bytes |
| `AAAAAAAAAAAAAAAAAAAAAQ` | accepted |
| `AAAAAAAAAAAAAAAAAAAAAg` | accepted |
| `AAAAAAAAAAAAAAAAAAAAAw` | accepted |
| `AAECAwQFBgcICQoLDA0ODw` | accepted: bytes `00` to `0f` |
| `AAAAAAAAAAAAAAAAAAAAAB` | refused: a valid token ID, but not canonical |
| `AAAAAAAAAAAAAAAAAAAA+A` | refused: `+` isn't base64url |
| `AAAAAAAAAAAAAAAAAAAAA` | refused: 21 characters |
| `root-jti` | refused: a valid token ID, but not 22 characters |

## 6. The token

### 6.1 Encoding

A token is a JWS in compact serialization (RFC 7515 §7.1): `<header>.<claims>.<signature>`, each part the base64url of its bytes. The header and claims are UTF-8 JSON objects; the signature is the 64-byte Ed25519 signature (RFC 8032) of the ASCII bytes `<header>.<claims>`, as they appear in the token. A token is at most 8,192 bytes (`MAX_TOKEN_LEN`); a node refuses a longer one before parsing it, and a signer refuses to produce one.

Unknown header fields and unknown claims are refused, never ignored. A verifier MUST NOT depend on the order of fields. Ed25519 signatures are deterministic, so the vectors pin the reference signers' exact bytes: fields in the order of the tables below, no whitespace, and absent fields omitted.

### 6.2 Header

| Field | Value | Authority-signed | Issuer-signed |
|---|---|---|---|
| `alg` | `"EdDSA"`, exactly | required | required |
| `typ` | `"JWT"`, exactly | optional; written by FleetRing's signers | optional; always written |
| `kid` | the signing key's key ID (§5.1) | required: a trusted authority key's | required: the issuer certificate's key |
| `x5c` | an array of exactly one string, the base64 of the issuer certificate's DER | absent | required |

`x5c` is standard base64 with padding, as RFC 7515 §4.1.6 says; base64url is refused. The DER MUST be the certificate's canonical DER, and the certificate MUST pass §7.1's checks. A token carries `x5c` exactly when an issuer signed it, so a node tells the two kinds apart by its presence.

An optional field that is absent is omitted. A field whose value is `null`, `"typ": null` or `"x5c": null` among them, is malformed and refused, never read as absent (§6.3).

### 6.3 Claims

| Claim | JSON type | Authority-signed | Issuer-signed |
|---|---|---|---|
| `iss` | string: the fleet's name | required | required |
| `sub` | string: a principal name | required; never a reserved dotted agent name | required: a delegated agent name |
| `jti` | string: a token ID | required | required: a delegation ID |
| `iat` | integer: seconds | optional; written by FleetRing's signers | required |
| `nbf` | integer: seconds | required | required |
| `exp` | integer: seconds | required | required |
| `fleetring.scopes` | array of strings: canonical scope texts | required | required, in canonical form (§3.3) |
| `fleetring.kind` | string: `"agent"` | optional; only with a root agent name as `sub` | required |
| `fleetring.owner` | string: a principal name | optional | the parent's owner: `apply` records no delegation from a token without one, though nodes don't check it |
| `fleetring.parent` | string: a principal name | absent | required |
| `fleetring.chain` | array of `{"id": <token ID>, "kid": <key ID>}` | absent | required: one entry per level of depth |

- **The issuer-signed column is what the control plane signs.** §7.1 lists what nodes check, which is less: a node checks `jti` against the token ID grammar only, not the delegation ID's, and checks neither that the scopes are in canonical form nor `fleetring.owner`. `apply` checks each of them before it records the delegation (§7.3).
- **Absent claims are omitted.** A signer MUST NOT write `null` for an absent claim, and a verifier MUST refuse a token in which any header field or claim is `null` as malformed, rather than read it as absent. So `"fleetring.chain": null` is refused, where reading it as absent would let an authority-signed token pass as one without a lineage. FleetRing's signers have always omitted absent fields, 0.7's included. An *empty* `fleetring.chain`, `[]`, is present: an authority-signed token carrying it is refused.
- **`fleetring.kind`** has one value in version 1. Without `kind = "agent"`, a token can't delegate.
- **`fleetring.chain`** is the lineage, root first, ending with the parent. Each entry's `id` is the ancestor's token ID: the root token's `jti`, or a delegated ancestor's delegation ID. Its `kid` is the key that signed the copy of that ancestor's token from which this token was minted: the authority's key for the root, an issuer's key for a delegated ancestor. A child's chain is its caller's chain followed by `{caller's jti, caller's kid}`.
- **`iat`** is the signing time. An issuer-signed token's `iat` is the effective time of the command that recorded the delegation, rounded down to the second, and its `nbf` is at least its `iat`.
- **Times** are whole seconds. The authority's signer drops milliseconds; the issuer's signer refuses claims whose times aren't whole seconds.

### 6.4 Claims in control state

Control state records a delegation's claims, every claim but `kid` and `x5c`, as JSON with these field names, in this order: `fleet` (`iss`), `principal` (`sub`), `id` (`jti`), `issued_at` (`iat`), `not_before` (`nbf`), `expires_at` (`exp`), `scopes`, `kind`, `owner`, `parent`, and `chain`. Times are milliseconds. `issued_at`, `kind`, `owner`, and `parent` are omitted when absent, and `chain` when empty; unknown fields are refused. The log and every snapshot carry this JSON, so its bytes are pinned: a change to it changes what replicas store.

## 7. What nodes, `apply`, and signers check

### 7.1 An issuer-signed token

A node, data or control, refuses an issuer-signed token unless all of the following hold, checking the certificate before it trusts anything else in the token (ADR-0047 points 1.4 and 3):

1. **The certificate** (`IssuerCertificate::check`, then `valid_at`):
   1. `x5c` is an array of exactly one base64 string, and the bytes are a certificate's canonical DER, its extensions included;
   2. the certificate was issued directly by a trusted authority in the node's `[tls] ca` bundle, with an Ed25519 signature (OID 1.3.101.112), and certifies an Ed25519 key that is no trusted authority's key;
   3. its only name is the subject alternative name URI `fleetring://<this fleet>/issuer/<name>`;
   4. it isn't an authority; it has no extended key usage extension; its key usage, if present, is digital signature alone; and it marks no extension critical other than basic constraints, key usage, and subject alternative name;
   5. it lasts at most 90 days plus one day of slack, and it is valid at the node's time, with 30 seconds of leeway on its start;
   6. `kid` is the key ID of the certificate's key;
   7. the JWS signature verifies with that key;
   8. neither `kid` nor the key ID of the authority key that signed the certificate is listed as `Revoked::Key`.
2. **The claims** (`TokenClaims::check_issuer_signed`, in this order, the first failure named):
   1. `sub` is a delegated agent name (*not delegated*; point 3.1);
   2. `fleetring.kind` is `"agent"` (*not agent*), and `fleetring.parent` is `sub` less its last segment (*parent*; point 3.2);
   3. `fleetring.chain` has exactly as many entries as `sub` has levels of depth, so at most 8, each with a valid token ID and key ID (*chain length*; point 3.3);
   4. the scopes are in the agent tier for `sub` (§7.2; points 3.4 to 3.7);
   5. `iat` is present (*no issued at*), `iat ≤ nbf` (*issued after start*), `nbf < exp` (*ends before start*), and `exp − nbf` is at most 24 hours (*too long*; point 3.8);
   6. `iss` is the node's fleet (point 3.9). The verifier checks this against the fleet it serves; `check_issuer_signed` doesn't, since it knows no fleet.
3. **The window and the denylist:** `nbf` is at most 30 seconds after the node's time, `exp` is after it, `iat` is at most 30 seconds after it, and §8 doesn't refuse the token.
4. **The certificate and the authorities, against the claims:** `exp` is no later than the certificate's end, so no token outlives its issuer certificate, on a request or on an open stream, which a node ends at `exp` (*outlives certificate*); and the first chain entry's `kid`, the key that signed the root token, is a trusted authority key in the node's `[tls] ca` bundle (*untrusted root*), so removing an authority from the bundle ends its root tokens' descendants as well as the root tokens. Both go beyond ADR-0047 points 1.4 and 3, and both only refuse.

The vectors' `refused_tokens` section gives a signed token for each of `x5c` in base64url, a `kid` that isn't the certificate key's, an expired certificate, an `exp` after the certificate's end, a root `kid` no trusted authority holds, and `null` header fields and claims.

### 7.2 The agent tier

The agent tier is the only set of scopes a delegation issuer may sign, fixed in code. `tier::check(child, scopes)` refuses a root agent name outright (*not delegated*). Otherwise it takes each scope in the order given, names the first that breaks a rule, and names the first rule it breaks, in this order:

1. it isn't `fleet` (*fleet*; point 3.4);
2. it lies within the root's namespace: with `<root>` the root of `child`, `agent-<id>`, the scope `<action>:<root>-*` or `<action>:<root>.*`, for the scope's own action, covers it (§3.2) (*outside root*; point 3.5);
3. if it is a `grant` scope, `grant:<child>.*` covers it (*grant outside child*; point 3.6);
4. if its pattern is a prefix with at least one byte before the `*`, the last of those bytes isn't an ASCII letter or digit (*no separator*; point 3.7). An exact name passes.

An empty set passes; signers refuse a token with no scopes (§3.6). The checks are coverage in the lattice, not text comparisons, so they can't disagree with the grant step.

**The `_` separator.** `_` is a segment character (§4.1) and, not being an ASCII letter or digit, a separator for rule 4. So `admin:agent-42.c7_*` passes the tier for `agent-42.c7`, yet it covers the sibling agent `agent-42.c7_x`'s databases (`agent-42.c7_x-*`) and its children's (`agent-42.c7_x.r1-*`), not only names `agent-42.c7` would mean. It stays inside the root's namespace, so no lineage escapes its root, and a `grant` scope must still lie within `<child>.` (rule 3), so `grant:agent-42.c7_*` is refused. The separator rule guards against names a careless prefix covers by accident; it doesn't confine a scope to one subtree, and a scope ending in `_*` should be read that way.

| Agent | Scopes | Result |
|---|---|---|
| `agent-42.c7` | `fork:agent-42-memory` | accepted |
| `agent-42.c7` | `admin:agent-42.c7-*` | accepted |
| `agent-42.c7` | `read:agent-42.*` | accepted |
| `agent-42.c7` | `grant:agent-42.c7.*` | accepted |
| `agent-42.c7` | `admin:agent-42.c7_*` | accepted: covers the sibling `agent-42.c7_x` |
| `agent-42.c7` | `admin:agent-43-*` | refused: `outside_root` |
| `agent-42.c7` | `admin:agent-420-*` | refused: `outside_root` |
| `agent-42.c7` | `admin:agent-42*` | refused: `outside_root`, before the separator rule |
| `agent-42.c7` | `grant:agent-42.d1.*` | refused: `grant_outside_child` |
| `agent-42.c7` | `grant:agent-42.c7*` | refused: `grant_outside_child` |
| `agent-42.c7` | `grant:agent-42.c7_*` | refused: `grant_outside_child` |
| `agent-42.c7` | `admin:agent-42.c7x*` | refused: `no_separator` |
| `agent-42.c7` | `fleet` | refused: `fleet` |
| `agent-42` | `admin:agent-42-*` | refused: `not_delegated` |

### 7.3 What `apply` and the signer add

Before `apply` records a delegation, and before an issuer signs one, `TokenClaims::check_recordable` runs §7.1's claim checks and then requires `nbf` to be at most 24 hours after `iat` (*starts too late*; ADR-0047 point 5.9). Nodes don't check that lead.

An issuer's signer (`DelegationSigner::sign`) also refuses claims for another fleet, with no scopes or more than 256, with a time that isn't a whole second, that expire after its certificate, or that would make a token longer than 8,192 bytes. `apply` also refuses, among the other checks of ADR-0047 point 5.9, scopes that aren't in canonical form (§3.3), more than 256 of them, and times that aren't whole seconds.

### 7.4 An authority-signed token

A node refuses an authority-signed token whose `sub` is a reserved dotted agent name (§4.1), that carries `fleetring.parent` or `fleetring.chain`, or whose `fleetring.kind` is `"agent"` with a `sub` that isn't a root agent name. It also refuses one that lasts more than 30 days from `nbf` to `exp`, whose `nbf` is more than 30 seconds after the node's time, whose `exp` isn't after it, or whose `iat`, if present, is more than 30 seconds after it (ADR-0026 point 5, ADR-0047 point 2.7).

## 8. Revocation by lineage

### 8.1 Token and key entries

A node refuses a token if the denylist holds any of:

- `Revoked::Token` of its `jti`, or `Revoked::Key` of its `kid`;
- `Revoked::Token` of any chain entry's `id`;
- `Revoked::Key` of any chain entry's `kid`.

So revoking a root token, any ancestor's delegation, or any key that signed an ancestor's copy ends the whole subtree below it, whichever control node signed what. A root token has no chain and is judged by its own `jti` and `kid`. An issuer-signed token is also refused when the authority key that certified its issuer is listed as `Revoked::Key` (§7.1, check 1.8). That entry refuses the tokens those issuers signed, but not a token descended from one of their copies whose own issuer another authority certified: a chain names the issuer key that signed each ancestor's copy, not the authority that certified that issuer. So after revoking an authority key, revoke the issuer keys it certified as well. The reference implementation (`Revocations::refuses_token`) names the first entry it finds, for the audit line, in this order: the token's `jti`, its `kid`, then each chain entry's `id` and `kid`, root first.

### 8.2 Principal entries

A principal revocation of principal `P` at control time `before` (milliseconds) refuses a token when its `sub` is at or below `P` (§4.2) and:

- for an issuer-signed token, `iat × 1000 ≤ before`;
- for an authority-signed token, `iat × 1000 ≤ before + 30,000`. The 30 seconds of leeway cover a root token's signer whose clock ran ahead of control time; an issuer-signed token needs none, since its `iat` is control time;
- for a token without `iat`, always: it counts as issued before every revocation, so it fails closed.

An ancestor named in an issuer-signed token's chain is always at or above its `sub`, so testing `sub` covers every ancestor name derived from it.

## 9. Versioning

- This is token spec version 1. Any change to the results of §2 to §8 for the same inputs requires a new version.
- **A change that alters whether `apply` accepts a command is also a fleet feature** (ADR-0038 point 4, ADR-0047 point 6.5), so that a mixed control group never applies the same command two ways. That covers the grammar, canonical text, the lattice, agent names, delegation IDs, the tier, the structural checks, and the control-state JSON of §6.4.
- The vectors file is append-only. New cases MAY be added at the end of a section, but existing cases MUST NOT change. `tokens-v1.lock` freezes every case, and the conformance test checks that every row of this spec's tables of scope texts, narrowing, agent names, delegation IDs, and the tier appears in the vectors.
