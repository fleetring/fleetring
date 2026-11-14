# FR-090 to FR-093 (#140 to #143): online delegation

This plan builds ADR-0047 (online delegation) across four tickets:

- FR-090: the delegation issuer, and nodes that verify delegated JWTs;
- FR-091: `Control.DelegateToken`, lineage in control state, `Control.ListDelegations`, and moving the scope grammar and lattice into `fleetring-control`;
- FR-092: the CLI, and root agents that can delegate;
- FR-093: the ancestor rule, `Revoked::Principal`, entries that record how they were added, the unreadable-denylist rule, `iat` as signing time, and reporting `delegation`.

The ADR wins wherever it and a ticket differ. Section 3 lists the differences. A difference between this plan and the ADR is a bug in this plan.

**Status of the inputs (2026-11-12).**

- ADR-0047 is Proposed, at `27f3910` on `fr-089-adr-0047`. That commit settles the parts the first draft of this plan marked pending (rates, pruning, tombstones, the entry shape, `recorded_by`, the connection rule by signer, and I5's freshness hook). Nothing in this plan is pending.
- This plan follows the ADR's recommended answers 1 to 6. Section 18 lists what would change under another answer.
- This revision answers an adversarial review of the first draft. The review's findings are folded in; the stage table is new.

**Status (2026-11-13): built.** Every stage, S1a to S10, has landed on `release/0.8.0`, with reviews R1 to R3 folded in below and R4's documentation items in S10. S9 deleted the `unreleased-delegation` preview feature (P8): `software::running_in` and `Reporting` are gone, every node of the release reports `delegation` from `SUPPORTED`, and `git grep unreleased-delegation` finds only this plan. P8, O4, and the tests this plan gates on the preview describe how the stages were built, and are kept as the record; no build has the feature now. See "S9 and S10, as built" after section 13.

## 1. What exists today

Checked against `fr-089-adr-0047` (`27f3910`), which is `main` plus the ADR.

| Area | Where | What it does today |
|---|---|---|
| JWT format and verification | `crates/fleetringd/src/auth/token.rs` | Strict `Header {alg, typ?, kid}` and `Claims {iss, sub, jti, iat?, nbf, exp, fleetring.scopes}`, both `deny_unknown_fields`. `TokenSigner::issue` writes `iat = nbf`. `TokenVerifier { issuer, keys }` finds `kid` among the authority keys. It checks `Revoked::Key`, `iss`, the 30-day maximum, `nbf` with 30 s leeway, `exp`, `ReservedPrincipal` (dotted `agent-` names), then `revoked.refuses(jti, kid)`. It never looks at `iat`. `MAX_TOKEN_LEN` is 8 KiB. |
| Connection rule and denylist source | `auth/boundary.rs` | `Authorizer::resolve_identified` accepts a token whose `sub` differs from the certificate's principal only when that principal sets `forward_tokens`. On `Peer::Plaintext` it skips that check (`certificate = None`) and verifies the token whenever the node has a TLS identity. `Source::Denylist {list, max_age}` refuses tokens while the list is missing or stale. `Source::Control(handle)` treats control state as always current. `Authorized::guard` rechecks `token_holds(id, key, expires_at)` every `STREAM_RECHECK` (1 s). `identity.token_verifier()` builds a new `TokenVerifier` for every request. |
| Caller types | `auth/policy.rs` | `Credential::Token {id, key, expires_at}`. `RPCS` must equal the proto's `Scope:` lines (a test keeps them equal). `RevokeToken` needs `Fleet`. |
| Scope grammar and lattice | `auth/scope.rs`, `auth/scope/lattice.rs`, `changes/matcher.rs` | Pure code. `auth::scope::Action` collides by name with `fleetring_control::Action`. `DatabaseMatcher::pattern()`, `Pattern`, and `Scopes::normalized()` are `pub(crate)`. `changes/matcher.rs` also holds `CaptureRules`, which imports `crate::engine::CaptureMode`. Two lattice cost tests read `std::time::Instant` under `#[expect(clippy::disallowed_methods)]`, which only `fleetringd`'s `clippy.toml` disallows. ADR-0044 §2.4's golden vectors live in the lattice's unit tests; `docs/spec/tokens-v1.md` doesn't exist. |
| Denylist on the wire | `data/convert.rs`, `auth/denylist.rs` | `revocations_from_proto` **skips** an entry it can't read, with a warning. |
| Certificate names | `tls/names.rs`, `tls/conn.rs`, `observe/admin.rs` | `SubjectUri::{Node, Principal, Authority}`. `peer_of` has an `other =>` arm that refuses the authority. The gRPC listener requires a client certificate; the admin listener allows none (`allow_unauthenticated`) but runs `peer_of` on one it gets. A client dialing a node checks only the server name (rustls's WebPKI verifier), never the subject kind. rustls-webpki accepts a certificate with no extended key usage for any use. `tls/` doesn't deny wildcard arms. |
| Control log | `fleetring-control/src/command.rs`, `fleetringd/src/control/{node,log}.rs` | `Command {at, action}`, without `deny_unknown_fields`. The single writer stamps `at` in `commit_group` (`Command::new(self.clock.now(), action)`). Log format 1 stores each command as JSON. |
| Control state | `crates/fleetring-control/src/{state,apply,revocation,software}.rs` | `Revocations` is a `BTreeMap<Revoked, Timestamp>`, serialized as `Vec<(Revoked, Timestamp)>`. `Action::RevokeToken {revoked, until}`; its `apply` calls `forget_through`. `Action::features()` ends in `_ => &[]`. The crate's `clippy.toml` disallows only hash types. |
| Ticks | `control/failover.rs`, `fleetring-control/src/fork/{apply,driver}.rs` | The timer submits `Tick` only after a tick interval with no commands, and the fork driver proposes one when `fork::apply::tick_due` says a deadline or retention passed. |
| Operation IDs | `fleetring-control/src/fork/operation.rs` | `fork::OperationId` is 16 bytes. Resends are requester-scoped and intent-compared. |
| Control service | `control/service.rs` | `ControlService` has `clock`, `entropy`, `submit`, and `submit_drawing`. `fork_database` resolves against `self.clock.now().max(state.now())`. |
| The control plane | `control/node.rs` | **One writer.** The Raft group (FR-064, #67) is paused, and ADR-0040 is Proposed. In 0.8 "a leader change" can only be a control node restart, and "two issuer keys" can only be a key rotation (a reload, or a restart with new files). |
| Software | `fleetringd/src/software.rs` | `running()` folds `SUPPORTED = [Undrain, Metadata, Expiry, Fork]`. `membership.rs`, `sim/client.rs`, and `upgrade` call it. |
| Test features | `fleetringd/Cargo.toml`, `lib.rs` | `fault-hooks` gates `tests/fork.rs` and `tests/fork_cli.rs` with `required-features`, and `lib.rs` refuses it with `not(debug_assertions)`, which `CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true` (set in `simulation-sweep.yml`) defeats. CI runs `cargo test --workspace --all-features`, so every integration test's binary is built with every feature. |
| Integration tests | `fleetringd/tests/` | One binary per file, plus `tests/sim/main.rs` for the simulation. Many files (`daemon.rs`, `control.rs`, `membership.rs`, `observe.rs`) start non-dev daemons through the binary. |
| Fuzzing | `fuzz/` | A separate workspace with targets for `fleetring` only, run nightly by `fuzz.yml`. |

## 2. What each ticket adds

- **FR-090:**
  - `SubjectUri::Issuer`, `Authority::issue_issuer`, `cert issuer`, `[control] issuer_cert` and `issuer_key` with reload, the expiry warning, and the dev-mode pair.
  - `x5c` and the four claims, `DelegationSigner`, the issuer path, point 3's constraints, and the `iat` rules in the verifier, with an `x5c` cache in the loaded identity.
  - The connection rule by signer, the refusal of a token without a client certificate, lineage revocation checks on requests and stream rechecks, and audit lines.
  - I5's freshness bound as a typed hook on `Source::Control`.
  - `SECURITY.md`.
- **FR-091:**
  - The scope grammar, the lattice, and the pure matcher move into `fleetring-control` (ADR §6.5), with `docs/spec/tokens-v1.md` and its golden vectors.
  - `State::{delegations, delegation_operations, rate_windows}`, `Action::RecordDelegation`, `recorded_by` stamped by the log layer, pruning in `apply` and a timer-proposed `Tick`, and `Feature::Delegation` (in `ALL`, not reported until FR-093).
  - `DelegateToken` and `ListDelegations`, their handlers, `ControlClient::delegate_token`, and simulation issuer rotation.
- **FR-092:**
  - Root identities minted with `kind`, `owner`, and `grant` once every member reports `delegation`.
  - `agent delegate`, `--agent <child>`, `mcp --agent <child>`, lineage in `agent list` and `agent show`, `agent list --fleet`, and `identity.toml` version 2 for children.
- **FR-093:**
  - `Revoked::Principal`, entries `{until, added}`, the ancestor rule, replacement, pruning, its cap and rates.
  - The unreadable-denylist rule.
  - `iat` as the signing time.
  - The CLI's revocations for children and roots.
  - Reporting `delegation`.
  - The release-blocker tests.

## 3. Ticket versus ADR differences

The ADR wins in every case. Each ticket should be edited to match before its stage starts. Lines marked † are differences from the code, not from the ADR.

### FR-090 (#140)

1. **When `delegation` is reported.** The ticket has nodes report it from FR-090. ADR §6.6 and §12.3 move reporting to FR-093, and the `RELEASING.md` line becomes "0.8.0 isn't cut between FR-090 and FR-093".
2. **The issuer certificate's limits.** The ticket omits the 90-day maximum with one day of slack on nodes and backdating counted (§1.2, §1.4.5); no extended key usage, key usage limited to digital signature, and no critical extension but the three named (§1.4.4); exactly one `x5c` entry (§1.4.1); a subject URI naming nothing else (§1.4.3); and the refusal of a node or client certificate in `x5c` (§1.5).
3. **New node-side refusals the ticket lacks:**
   - every token whose `iat` is later than the node's time plus 30 s (§2.7);
   - an issuer-signed token without `iat`, or without `iat ≤ nbf < exp` (§3.8);
   - the separator refusal for issuer-signed patterns (§3.7);
   - an authority-signed token carrying `fleetring.parent` or `fleetring.chain` (§3, the converse);
   - an authority-signed `kind = "agent"` token whose `sub` isn't a root agent name (§2.6).
4. **The connection rule depends on the signer** (§4.1). The ancestor match applies only to issuer-signed tokens; an authority-signed token keeps today's equality rule. A token on a connection with no client certificate is refused. The boundary says whether a token is relay-carried, for FR-091 and FR-093 (§4.5).
5. **The I5 freshness bound** is a typed hook on `Source::Control` that the single writer always reports as current (§13, I5). The ticket doesn't mention it.
6. **Configuration.** Both `issuer_cert` and `issuer_key` or neither, and either one is a configuration error on a node without the control role (§1.3). Load and reload check the token-independent rules of §1.4 and the key match; a reload failure keeps the old pair.
7. **Dev mode.** 90 days' validity, replacement under 30 days, on expiry, or on a mismatch, through temporary names, and defaults for a 0.7 dev directory (§1.7).
8. **Tests.** "`agent-42.c70`'s certificate" can't exist (§4.3). The plan tests `agent-420`'s and `agent-4`'s certificates.

### FR-091 (#141)

1. **The command carries the caller's presented claims**, every claim but `kid` and `x5c`, and `apply` compares them with the record (§5.9). The ticket's `caller_token: {id, kid, chain}` is too weak.
2. **Records and operations.** The ticket prunes records 30 s after expiry and forgets operations after 7 days. The ADR has `kept_until`, operations kept until it plus 1 hour, a "revoked" tombstone kept 1 hour from the revocation, and pruning in `RecordDelegation`'s and `RevokeToken`'s own `apply` as well as `Tick` (§6.1, §6.2).
3. **Limits.** The ADR: 256 live per root lineage, revoked ones not counted; 30 a minute per caller; 60 a minute from a lineage's delegated agents, never counting the root; counted in `State::rate_windows` by command; and a backstop of 8,192 unpruned records per lineage (§5.10, §6.2, §6.3). The ticket has 256 and 30 per parent.
4. **The proto.** The response adds the issuer key's ID (§5.1). `lifetime_ms` and `not_before_ms` are `optional`, since the canonical request tells absent from given (§5.7).
5. **Times.** The ADR adds: `now = max(clock, State::now)`, refused with `UNAVAILABLE` when `State::now` leads the clock by more than 30 s; an explicit `not_before` more than 30 s past, more than 24 h ahead, or at or after `P.exp` is refused; a minimum explicit lifetime of 60 s; and `iat` from the command's effective time with `nbf` raised to it (§5.4, §5.6).
6. **The result must be in the agent tier** for the child's name (§5.3). The ticket checks only the separator.
7. **What the caller must be.** A direct connection, never relay-carried (§4.5); an agent name (§5.2); and the issuer preconditions (§1.6).
8. **Retries.** A retry of a revoked delegation is refused, and so is one of an expired delegation (§5.7).
9. **The audit line** also names the delegation ID and the issuer key ID (§5.12).
10. **The default-scope error** is "your token holds no grant over agent-42.c7-" (§5.3).
11. † **The client.** `fleetring-client` makes no RPC calls, so the call goes on `fleetringd::control::ControlClient`, and the output types go in `fleetring_client::output`.
12. **What `apply` checks of a delegated caller.** Its whole lineage for revocations, and the child against the parent's record: owner, chain, the strict grant step on scopes, and `exp` (§5.9).
13. **The scope grammar and lattice move into `fleetring-control`** (§6.5), with `docs/spec/tokens-v1.md`. The ticket doesn't mention it.
14. **`recorded_by`** is stamped by the log layer (§6.1), and **`Control.ListDelegations`** lists records for incident response (§6.4).
15. **A same-second re-delegation of a revoked name** is refused with `UNAVAILABLE`, retryable (§5.9, §11.5). It needs `Revoked::Principal`, so it lands with FR-093's stage S8a.

### FR-092 (#142)

1. **`agent renew` upgrades** an identity that holds exactly 0.7's default or FR-081's defaults (§12.4). The code's `widened` handles only 0.7's.
2. **The child's record.** `identity.toml` version 2 records `root`, the delegation ID, `issuer_key_id`, and `operation`, and no certificate (§10, §4.2). † `AgentRecord::cert_expires_at` is required today.
3. **`agent list --fleet [--recorded-by] [--since]`** is in scope (§6.4, §10). The first draft dropped it.
4. **Dev mode** in the ticket "always" reports `delegation`. A dev node reports it only from FR-093's build.
5. **`agent delegate` details** from `docs/cli.md` §16: up to 5 resends within 60 s, the `UNAVAILABLE` retry of a name revoked less than a second ago, exit codes 3 and 4, and the trusted-local-development line.
6. **`agent renew <child>` and `agent remove <child>`** need FR-093's ancestor rule, so S9 builds them.

### FR-093 (#143)

1. **`before`** is the revoking command's effective time, stored as it is. Nodes add 30 s only for authority-signed tokens (§3, §11.5). The ticket says "the command's time" and has no leeway rule.
2. **`until`** depends on the name and on what the entry covers, and a `fleet` revocation's `until` is never below the longest pruned `kept_until` plus 30 s (§11.5). The ticket keeps every principal entry 30 days and 30 s.
3. **Who may revoke what.** Agents only; targets are `P` itself, the token `P` presented, or recorded delegations below `P` and their principals; `apply` checks a delegated caller's record; relay-carried tokens are refused except for their own `jti` (§4.5, §11.3). The ticket lets any principal revoke any name at or below it.
4. **Bounds.** 1,024 counted `Ancestor` entries per root lineage; 30 a minute per caller and 60 from a lineage's delegated agents, root not counted; one entry per `(fleet, principal)` (§11.2, §11.6). The ticket has none.
5. **Entries record how they were added,** only a principal entry replaces others, and every token or principal revocation prunes the records it ends (§11.2, §11.4). The ticket has neither.
6. **The unreadable-denylist rule** (§3). The ticket omits it.
7. **`iat` is the signing time** in `TokenSigner` (§2.7). The ticket asks only that `iat` be present.
8. **FR-093's build reports `delegation`** (§6.6).
9. **`agent remove <root-id>`** revokes the root's own token ID as well as its principal (§11.7).

## 4. Decisions this plan makes inside the ADR

Each closes a gap the ADR leaves to the implementation. Reviewers should attack these first.

- **P1. One implementation of each rule, in `fleetring-control`.** The node verifier, the stream recheck, `apply`, and the handler call the same pure functions:
  - the scope grammar, the lattice, and the matcher, moved from `fleetringd` (ADR §6.5);
  - `AgentName`, the agent name grammar;
  - `tier::check`, point 3.4 to 3.7, written over the lattice's own `Scope` and `DatabaseMatcher`;
  - `TokenClaims::check_issuer_signed`, point 3.1 to 3.8 with `tier::check` inside it, and `TokenClaims::check_recordable`, which adds §5.9's `nbf ≤ iat + 24 h`: the verifier (S4b), the signer (S4a), and `apply` (S5) call them, so the structural rules have one implementation;
  - `Revocations::refuses_token`, which covers the token's own ID and key, every chain entry, and (from S8a) principals with the leeway chosen by the signer.
- **P2. Withdrawn.** The first draft's second, text-level scope checker and its `tier_vectors.json` are gone: ADR §6.5 moves the one implementation instead. The golden vectors are the spec's (S1c).
- **P3. The command carries the caller's presented claims in full**, never a digest. They are at most 8 KiB, the control crate has no hash dependency, and the record compares field by field, which gives a readable rejection and nothing to collide.
- **P4. Recorded times are whole seconds.** The handler floors `not_before` and `exp`. `apply` refuses times that aren't multiples of 1,000 ms, sets `iat = floor_to_second(effective time)`, and raises `nbf` to it. The recorded claims are exactly the claims signed.
- **P5. The command fixes the child's structure.** `RecordDelegation` carries a `segment`, and `apply` derives `sub`, `parent`, `chain`, and `owner` from the caller (I3 by construction).
- **P6. The ancestor rule and principal revocations are their own actions.** `RevokeToken {revoked, until}` stays the operator's `Token` and `Key` action, and `apply` refuses it with a `Principal` target. `Action::RevokePrincipal` (with `fleet`) and `Action::RevokeAsAncestor` carry the rest. Old logs and the operator path stay untouched.
- **P7. The denylist's entries.** `Revocations` is keyed internally by `EntryKey::{Token(TokenId), Key(KeyId), Principal {fleet, principal}}`, so `before` is a value and a principal has one entry. Each entry is `Entry { until, added: Added::{Fleet, Ancestor {root, by}}, before: Option<Timestamp>, of: Option<AgentName> }`: `before` for principal entries, `of` for an ancestor-rule token entry of a delegation (its principal, ADR §11.4). An entry serializes as `[revoked, until]` when it is `Fleet` with nothing more, which is 0.7's form byte for byte, and as `[revoked, until, {added, before?, of?}]` otherwise. Only `apply` with `delegation` enabled can write the longer form.
- **P8. A preview build reports `delegation` early, but never to a real fleet.** FR-090 to FR-092 can't be tested end to end while no member reports `delegation`.
  - A cargo feature, `unreleased-delegation`, makes `software::running_in(Reporting::Preview)` include `Feature::Delegation`. The daemon asks for `Preview` only in dev mode or in the simulation's environment. A non-dev node in the production environment reports the release set, so no real fleet can enable the feature from a preview build, whatever its debug-assertion setting.
  - The daemon logs a warning at start whenever the feature is compiled in.
  - **Why not refuse to start outside dev mode,** as the review asked: CI's `cargo test --workspace --all-features` builds every integration test's binary with the feature, and many of them start non-dev daemons (`tests/daemon.rs`, `control.rs`, `membership.rs`, `observe.rs`). A refusal would fail them all. Not reporting has the same effect on a real fleet: `DelegateToken` and the ancestor rule answer `FeatureUnsupported`.
  - The `lib.rs` guard against builds without debug assertions stays, as a second line only, since `CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true` defeats it.
  - Tests that need the feature use `#![cfg(feature = "unreleased-delegation")]` at the top of a new file, or `#[cfg(feature = "unreleased-delegation")]` on a test function, never `required-features`.
  - Only S5 (which adds the feature) and S9 (which deletes it) edit `fleetringd/Cargo.toml`. S9 and R4 grep that `unreleased-delegation` is gone.
- **P9. Single-writer freshness, with a typed hook.** `Source::Control` gains `freshness: ControlFreshness::{SoleWriter, Acknowledged {last, max_age}}`. Production builds `SoleWriter`, whose state is current by definition. `Acknowledged` refuses every token with `UNAVAILABLE` once `last`, the time a quorum or the current leader last acknowledged this node, is older than `max_age`. A test drives it. FR-064 constructs `Acknowledged`; no `TODO` is left in the code.
- **P10. `x5c` is standard base64 with padding** (RFC 7515 §4.1.6). base64url is refused. The signing input stays base64url, as JWS requires.
- **P11. The `x5c` cache lives in the loaded identity generation.** `Loaded` (`tls/identity.rs`) holds a cache of issuer certificates that passed the token-independent checks, keyed by the certificate's TBS bytes (the signed part, as x509-parser returns it) and bounded to 16 entries, in a `Mutex<BTreeMap>`. The TBS is what the authority signed, so a hit means its content was verified before, whatever wrapper bytes carry it; `check` also refuses any wrapper but the canonical DER (see the re-check pins). `token_verifier()` hands the verifier an `Arc` of it. A reload builds a new `Loaded`, so a changed authority bundle starts with an empty cache. The validity window and the `kid` match are checked on every token; the cache skips only the parse and the certificate signature check.
- **P12. `recorded_by` is a log-layer stamp.** `Command` gains `recorded_by: Option<NodeId>`, `#[serde(default, skip_serializing_if = "Option::is_none")]`, so every 0.7 command reads and writes byte for byte. The single writer stamps its own node ID beside `at` on every command it logs (`Command::stamped(at, by, action)`; `ControlOptions::recorder(NodeId)`). Replay reads it from the log, so it is deterministic. `apply` refuses `RecordDelegation` without it. `Command` has no `deny_unknown_fields`, so a 0.7 binary reading a 0.8 log before `delegation` is enabled ignores the field. Under FR-064, the state machine sets it from the entry's leader ID before `apply`, never from the payload; S10 adds that line to FR-064's plan.
- **P13. A token too large to use is refused before recording.** The handler signs a trial of the resolved claims with the current pair, and refuses with `INVALID_ARGUMENT`, "the child's token would exceed 8 KiB; ask for fewer scopes", when it exceeds `MAX_TOKEN_LEN`. `DelegationSigner::sign` refuses the same, so no caller returns a token every node refuses. `apply` refuses more than `MAX_SCOPES` scopes.
- **P14. `apply` also bounds `not_before`.** ADR §5.4 has the handler refuse a `not_before` more than 24 h ahead, and §6.3's bounds rely on it "whoever proposes", but §5.9's structure list doesn't repeat it. `apply` refuses `nbf > iat + 24 h` too, so a proposer can't stretch `kept_until`. Flagged to the ADR's author as a one-line addition to §5.9.

## 5. Stages

| Stage | Ticket | What | Depends on | Owns these files | Size | Model, effort | Fable review |
|---|---|---|---|---|---|---|---|
| **S1a** | 091 | Move the scope grammar, the lattice, and the pure matcher into `fleetring-control`, re-exported so importers don't change. No behaviour change. | the owner accepts ADR-0047 | `fleetring-control/src/{scope.rs, scope/lattice.rs, matcher.rs}` (new), `fleetring-control/src/lib.rs`; `fleetringd/src/auth/{scope.rs, scope/lattice.rs}` (removed), `auth/mod.rs`, `auth/scope_cost.rs` (new, the two cost tests), `changes/matcher.rs`, `changes/mod.rs` | M | Sonnet, with this plan as its spec | R1 |
| **S2** | 090 | The issuer certificate: `SubjectUri::Issuer`, `Authority::issue_issuer`, `IssuerCertificate` with its token-independent and time checks split, `peer_of` with explicit arms, and the wildcard lint in `tls/` | the owner accepts ADR-0047 | `tls/{names,authority,conn,issuer (new),mod,identity}.rs` (`identity.rs` for lint fixes only), `fleetringd/src/lib.rs` (Send/Sync asserts), `tests/tls.rs` | M | Opus, high | R1 |
| **S1b** | 090, 091 | The lineage vocabulary: `AgentName`, `ChainLink`, `DelegationId`, `TokenClaims` (with `scopes: Scopes`), `SignerKind`, `tier::check` over the lattice, and `Revocations::refuses_token` with `TokenRef` | S1a | `fleetring-control/src/delegation/{mod,name,claims,tier}.rs` (new), `revocation.rs`, `lib.rs`, `tests/it/{lineage.rs (new), main.rs}` | M | Opus, high | R1 |
| **S3** | 090 | The issuer files: `[control] issuer_*` with load checks, reload that keeps the old pair, and the 7-day warning; `cert issuer`; dev mode's pair and its replacement; the dev-directory check; `serve --check` | S2 | `config.rs` (`ControlSettings`), `tls/issuer_files.rs` (new: `IssuerPair`), `tls/mod.rs`, `main.rs` (`CertCommand::Issuer`), `dev.rs`, `serve_check.rs`, `upgrade/config_check.rs`, `tests/dev/*` (issuer cases), `tests/tls.rs` (CLI section) | M | Opus, high | R1 |
| **S4a** | 090 | The token format: `x5c` and the four claims, `TokenSpec::new` and its call sites, `TokenSpec::agent`, and `DelegationSigner` | S1b, S2, S3 (for `main.rs`) | `auth/token.rs` (format, `TokenSpec`, `DelegationSigner`), every `TokenSpec` constructor call site (`agent.rs`, `main.rs`, `dev.rs`, `sim/fleet.rs`, tests; mechanical) | M | Opus, high | R1 |
| **S5** | 091 | Delegation in control state: the three maps, `RecordDelegation`, `recorded_by`, pruning in `apply`, the timer's `Tick`, the limits and rate windows, the backstop, `features()` without a wildcard, the model-based replay generator, rollback, and the preview feature | S1b | `fleetring-control/src/delegation/{record,apply,limits,prune}.rs` (new), `state.rs`, `command.rs`, `apply.rs`, `software.rs`, `lib.rs`, `tests/it/{delegation.rs (new), delegation_replay.rs (new), replay.rs, rollback.rs, software.rs, common/mod.rs, main.rs, fixtures/*}`; `fleetringd/Cargo.toml` (the feature), `fleetringd/src/software.rs`, `fleetringd/src/control/{node.rs, failover.rs}` | L | Opus, high | R2 |
| **S4b** | 090 | The verifier: the issuer path, point 3, the converse, the `iat` rules, the `x5c` cache in `Loaded`, and a fuzz target | S4a | `auth/token.rs` (verification), `tls/identity.rs` (cache), `tests/delegated_tokens.rs` (new), `fuzz/{Cargo.toml, fuzz_targets/token.rs (new)}`, `.github/workflows/fuzz.yml` | M | Opus, high | R1 |
| **S1c** | 091 | `docs/spec/tokens-v1.md`: the canonical scope text, the lattice, the tier, and the golden vectors, with a conformance test | S1b, S5 (for `tests/it/main.rs`) | `docs/spec/tokens-v1.md` (new), `fleetring-control/tests/vectors/tokens-v1.json` (new), `tests/it/{tokens_vectors.rs (new), main.rs}` | S | Opus, medium | R1 |
| **S4c** | 090 | The connection rule: `Carriage`, the signer in `Credential`, the ancestor match for issuer-signed tokens only, the refusal without a client certificate, the recheck through `refuses_token`, audit lines, and the freshness hook | S4b | `auth/{boundary.rs, policy.rs, mod.rs}`, `router/auth.rs` (tests), `fleetringd/src/lib.rs`, `tests/delegation.rs` (new) | M | Opus, high | R1 |
| **R1** | 090 | Fable reviews S1a to S4c (the credential path and the moved lattice), **before W6** | S4c | — | — | Fable, high | — |
| **S6** | 091 | `Control.DelegateToken` and `Control.ListDelegations`: the proto, the handlers, signing, retries, the clock-lead refusal, `ControlClient`, daemon wiring of the pair, the recorder, and `Reporting`, and simulation issuer rotation | R1, S3, S4c, S5 | `fleetring-proto/proto/fleetring/v1/fleetring.proto` (the two RPCs), `control/service.rs` (new handlers), `control/delegation.rs` (new), `auth/policy.rs`, `auth/boundary.rs` (credential access for handlers), `control/client.rs`, `daemon.rs`, `membership.rs` (`Reporting`), `sim/{fleet,client,host}.rs`, `crates/fleetringd/README.md`, `tests/delegate_token.rs` (new), `tests/sim/{main.rs, delegation.rs (new)}` | L | Opus, high | R2 |
| **R2** | 091 | Fable reviews S5 and S6 (minting) | S6 | — | — | Fable, high | — |
| **S7a** | 092 | Root agents that can delegate: the probe, defaults with `grant`, `kind`, and `owner`, and `agent renew` widening | S6 | `agent.rs`, `agent_cmd.rs` (`add`, `renew` for roots), `tests/agents.rs` (root cases) | M | Opus, high | R3 |
| **S7b** | 092 | Children: `agent delegate`, `--agent <child>`, `mcp --agent <child>`, `agent list` and `agent show`, `agent list --fleet`, version 2 records, and the MCP tool-name test | S7a | `agent.rs`, `agent_cmd.rs`, `resolve.rs`, `main.rs` (`agent delegate`, `agent list --fleet`), `mcp/server.rs`, `mcp/tools.rs` (test only), `fleetring-client/src/output.rs`, `tests/agents.rs`, `tests/mcp.rs` | M | Opus, high | R3 |
| **R3** | 092 | Fable reviews S7a and S7b | S7b | — | — | Fable, high | — |
| **S8a** | 093 | Control state: `Revoked::Principal`, P7's entries, `RevokePrincipal` and `RevokeAsAncestor`, the ancestor rule, replacement, pruning with tombstones, `until` and the `fleet` floor, the cap and rates, the same-second refusal, and the wire case | S6 | `fleetring-control/src/{revocation.rs, delegation/revoke.rs (new), delegation/apply.rs, apply.rs, command.rs, state.rs, lib.rs}`, `tests/it/{revocation.rs (new), delegation.rs, delegation_replay.rs, replay.rs, rollback.rs, common/mod.rs, main.rs, fixtures/*}`; `fleetring.proto` (the `Revocation` case and `RevokeTokenRequest.principal`), `data/convert.rs` | L | Opus, high | R4 |
| **S8b** | 093 | Nodes and the handler: the unreadable-denylist rule; principal checks with the leeway by signer; `RevokeToken` with `Requirement::FleetOrAncestor`; the relay's own-`jti` exception; the same-second `UNAVAILABLE` | S8a | `data/convert.rs`, `auth/denylist.rs`, `auth/boundary.rs`, `auth/policy.rs`, `control/service.rs` (`revoke_token` only), `data/node.rs`, `crates/fleetringd/README.md`, `tests/delegation.rs`, `tests/control.rs` | M | Opus, high | R4 |
| **S9** | 093 | `iat` as the signing time; the CLI's revocations; reporting `delegation` for real and deleting the preview feature; the release-blocker tests | S7b, S8b | `auth/token.rs` (`TokenSigner` only), `agent.rs`, `agent_cmd.rs`, `main.rs` (`token revoke`), `sim/fleet.rs` (token minting), `fleetringd/src/software.rs`, `fleetringd/Cargo.toml`, `fleetringd/src/lib.rs` (the guard), every file with `cfg(feature = "unreleased-delegation")`, `tests/release_blocker.rs` (new), `tests/sim/delegation.rs`, `tests/agents.rs` | M | Opus, high | R4 |
| **S10** | all | Docs: `SECURITY.md`; the lexicon's terms marked current; `docs/cli.md` "Where `fleetring` stands"; `RELEASING.md`; the dated ADR notes if FR-089 didn't add them; FR-064's checklist lines (the freshness hook and `recorded_by` from the entry) | S6 (facts settle there); final pass after S9 | `SECURITY.md`, `docs/lexicon.md`, `docs/cli.md`, `RELEASING.md`, `docs/adr/0025,0026,0039,0043,0044` (notes only), `docs/plans/FR-064-raft-control-plane.md` | S/M | Opus, medium | in R4 (`SECURITY.md` only) |
| **R4** | 093 | Fable reviews S8a to S10, and the whole branch against I1 to I11 and the release blocker | S9, S10 | — | — | Fable, high | — |

### Order and parallel worktrees

Every stage is a branch off the integration branch `fr-090-093-delegation`, cut from `main` once FR-089 lands. It is rebased onto the integration branch and passes every gate in section 17 alone before it rolls up. Within a wave, file ownership doesn't overlap.

| Wave | Runs in parallel | Why there is no overlap |
|---|---|---|
| W1 | S1a and S2 | S1a is `fleetring-control/src/{scope,matcher}`, `auth/scope*`, `auth/mod.rs`, and `changes/`; S2 is `tls/` and `lib.rs`. |
| W2 | S1b and S3 | S1b is `fleetring-control` (`delegation/`, `revocation.rs`, its tests); S3 is config, CLI, dev, `serve_check.rs`, and `tls/{mod,issuer_files}.rs`. |
| W3 | S4a and S5 | S4a is `auth/token.rs` and the `TokenSpec` call sites (`agent.rs`, `main.rs`, `dev.rs`, `sim/fleet.rs`). S5 is `fleetring-control` and `fleetringd/{Cargo.toml, software.rs, control/node.rs, control/failover.rs}`. S4a starts after S3 rolls up, since both touch `main.rs`. |
| W4 | S4b and S1c | S4b is `auth/token.rs`, `tls/identity.rs`, `fuzz/`, and a new test file. S1c is `docs/spec/` and `fleetring-control/tests/`, after S5 rolled up. |
| W5 | S4c alone | It needs S4b's `Verified`. |
| — | R1 | Runs on S1a to S4c before W6, so its findings reach S5's follow-ups and S6 before minting is built. |
| W6 | S6 alone | It needs R1, S3, S4c, and S5. |
| W7 | S7a, then S7b, in parallel with S8a, then S8b | S7 is `agent*`, `resolve.rs`, `mcp/`, `main.rs`, and `fleetring-client`. S8 is `fleetring-control`, the proto, `convert.rs`, `denylist.rs`, `boundary.rs`, `policy.rs`, `service.rs` (`revoke_token`), `data/node.rs`, and the README. |
| W8 | S9 and S10 | S10 owns `RELEASING.md` and `docs/`; S9 touches neither. |

**One PR.** The stages roll up into the integration branch, which runs every gate locally and is pushed once. The PR is titled `feat: online delegation (ADR-0047)` and carries `Fixes #140`, `Fixes #141`, `Fixes #142`, and `Fixes #143`. Reviews R1 to R4 run on the integration branch's diff ranges. After the rollup, every stage worktree, its branch, and its `target/` are removed, and `git worktree prune` runs.

## 6. S1a: moving the scope grammar and lattice (`fleetring-control`)

The move changes no behaviour and no bytes. Its only test changes are paths.

- **New modules.**
  - `fleetring_control::scope`, a public module: `Action`, `Target`, `Scope`, `Scopes`, `ParseScopeError`, and `lattice`'s `GrantStepError`, `MAX_SCOPES`, and `TooManyScopes`. They are **not** re-exported at the crate root, since `fleetring_control::Action` is the control action.
  - `fleetring_control::matcher`, a public module: `DatabaseMatcher`, `DatabaseMatchers`, and `ParseMatcherError`, with their serde impls. `Pattern` and `DatabaseMatcher::pattern()` stay `pub(crate)`; their only users move with them.
- **What stays in `fleetringd`.**
  - `changes/matcher.rs` keeps `CaptureRules`, which needs `crate::engine::CaptureMode`, and imports `DatabaseMatchers` from the control crate.
  - `changes/mod.rs` re-exports `pub use fleetring_control::matcher::{DatabaseMatcher, DatabaseMatchers, ParseMatcherError}` and drops `pub(crate) use self::matcher::Pattern`.
  - `auth/mod.rs` re-exports `pub use fleetring_control::scope::{Action, GrantStepError, MAX_SCOPES, ParseScopeError, Scope, Scopes, Target, TooManyScopes}`. No importer of `crate::auth::{Action, …}` or `fleetringd::changes::DatabaseMatcher` changes.
  - `lib.rs`'s `assert_debug_send_sync::<auth::Action>()` keeps compiling through the re-export. The control crate asserts its own.
- **Visibility.** `Scopes::normalized()` becomes `pub`, documented as the canonical form, since S1b's `apply` compares against it and `fleetringd`'s cost tests call it. Nothing else widens.
- **Doctests** that name `fleetringd::auth::{…}` or `fleetringd::changes::DatabaseMatcher` move with their items and name `fleetring_control::scope` and `fleetring_control::matcher`.
- **The two cost tests** (`an_oversized_side_is_refused_before_any_work`, `narrowing_at_the_limit_stays_cheap`) stay in `fleetringd`, in `auth/scope_cost.rs`, with their `#[expect(clippy::disallowed_methods)]`. Moved, the expectation would be unfulfilled (the control crate doesn't disallow `Instant::now`) and the crate would read a clock. Their helpers (`distinct`, `scope`) are copied with them.
- **ADR-0044 §2.4's golden-vector test** moves with the lattice unchanged; S1c makes the spec its home.
- **Lints.** The moved code passes the control crate's `clippy.toml` and its 1.88 MSRV check; `fleetring-control` gains no dependency.

**Tests:** every moved unit test passes in its new crate; `cargo test -p fleetringd` passes untouched apart from the cost tests' path; `cargo doc` with `-D warnings` resolves every moved link.

## 7. S1b and S1c: the lineage vocabulary, and the spec

### S1b (`fleetring-control`)

```rust
// delegation/name.rs
pub struct AgentName(PrincipalName); // `agent-<id>` or `agent-<id>(.<seg>){1,8}`; each part [a-z0-9_]{1,32}; ≤ 64 bytes
impl AgentName {
    pub fn root(&self) -> AgentName;
    pub fn depth(&self) -> usize;                       // the number of '.'s, 0..=8
    pub fn parent(&self) -> Option<AgentName>;          // None for a root
    pub fn child(&self, segment: &str) -> Result<AgentName, ParseAgentNameError>;
    pub fn is_delegated(&self) -> bool;
    pub fn as_principal(&self) -> &PrincipalName;
}
impl PrincipalName { pub fn is_at_or_below(&self, ancestor: &PrincipalName) -> bool } // == or starts with "<ancestor>."
pub const MAX_DEPTH: usize = 8;

// delegation/claims.rs
pub struct DelegationId(TokenId);                       // 22 base64url characters; the proposer encodes 16 drawn bytes
pub struct ChainLink { pub id: TokenId, pub kid: KeyId }
pub enum TokenKind { Agent }
pub enum SignerKind { Authority, Issuer }               // who signed a copy; fixes the principal leeway (ADR §3)
pub struct TokenClaims {                                // every claim but `kid` and `x5c`
    pub fleet: Box<str>, pub principal: PrincipalName, pub id: TokenId,
    pub issued_at: Option<Timestamp>, pub not_before: Timestamp, pub expires_at: Timestamp,
    pub scopes: Scopes, pub kind: Option<TokenKind>, pub owner: Option<PrincipalName>,
    pub parent: Option<PrincipalName>, pub chain: Vec<ChainLink>,
}

impl TokenClaims {
    pub fn check_issuer_signed(&self) -> Result<AgentName, ClaimsViolation>; // point 3.1 to 3.8 (3.9, iss, is the caller's)
    pub fn check_recordable(&self) -> Result<AgentName, ClaimsViolation>;    // the above, then nbf ≤ iat + 24 h (§5.9)
}
pub const MAX_DELEGATED_LIFETIME: Millis = 24 h; pub const MAX_NOT_BEFORE_LEAD: Millis = 24 h;
#[non_exhaustive] pub enum ClaimsViolation { NotDelegated, NotAgent, Parent, ChainLength { links, depth }, Tier(TierViolation),
    NoIssuedAt, IssuedAfterStart, EndsBeforeStart, TooLong, StartsTooLate }

// delegation/tier.rs: ADR point 3.4 to 3.7, over the lattice's own types
pub fn check(child: &AgentName, scopes: &Scopes) -> Result<(), TierViolation>;            // NotDelegated for a root name
#[non_exhaustive] pub enum TierViolation { NotDelegated, Fleet, OutsideRoot { scope: Scope }, GrantOutsideChild { scope: Scope }, NoSeparator { scope: Scope } }

// revocation.rs
pub struct TokenRef<'a> { pub id: &'a TokenId, pub kid: &'a KeyId, pub chain: &'a [ChainLink],
                          pub principal: &'a PrincipalName, pub issued_at: Option<Timestamp>,
                          pub signer: SignerKind }
impl Revocations { pub fn refuses_token(&self, token: &TokenRef<'_>) -> Option<Revoked> }
```

- `tier::check` decides "within `agent-<root>-` or `agent-<root>.`" as coverage by `Prefix("agent-<root>-")` or `Prefix("agent-<root>.")` in the lattice, and "every `grant` within `<sub>.`" likewise, so it can't disagree with the grant step. The separator rule reads the pattern's last byte before `*`.
- In S1b, `refuses_token` checks the token's own ID and key, every chain `id` (`Revoked::Token`), and every chain `kid` (`Revoked::Key`), and returns the first match, for the audit line. S8a adds principals, using `signer` for the leeway. `signer` is in S1b so that every caller passes it from the start.
- `apply` derives a caller's `signer` from its name: an issuer-signed token always names a delegated agent, and an authority-signed one never does (ADR §2.6, §3).
- The old `refuses(id, kid)` stays as a wrapper until S4c moves its callers.

**Tests** (`tests/it/lineage.rs`):

- `agent_names_parse_root_and_delegated_and_refuse_the_rest`: `agent-42`, `agent-42.c7`, and depth 8 parse; depth 9, 65 bytes, `agent-42.`, `agent-42..x`, `agent-42.C7`, `agent-42.c-7`, and `workload.prod` are refused.
- `at_or_below_matches_agent_42_and_its_children_never_agent_420`.
- `tier_refusals_name_the_scope`: `admin:agent-43-*` under root 42; `read:pub-*`; `admin:agent-420-*`; `grant:agent-42.d1.*` for `agent-42.c7`; `grant:agent-42.c7*`; `admin:agent-42.c7x*`; `fleet`. `fork:agent-42-memory` is accepted.
- `prop_tier_check_agrees_with_coverage`: for random patterns, `check` accepts exactly the scopes the two root prefixes cover (the lattice's own `covers`).
- `refuses_token_matches_own_id_key_every_chain_id_and_every_chain_kid`.
- `prop_refuses_token_is_monotonic_in_the_list`.
- `issuer_signed_claims_need_a_delegated_subject_its_parent_and_a_link_per_level` and `issuer_signed_claims_times_hold_at_each_boundary`: `check_issuer_signed` and `check_recordable` at every boundary of point 3.1 to 3.8 and §5.9, the first broken rule named in point order (added after the S1a, S1b and S2 review).

### S1c (`docs/spec/tokens-v1.md`)

- The spec is normative, as `placement-v1.md` is: the canonical text of a scope (`<action>:<pattern>` or `fleet`), the canonical form of a set (sorted, nothing covered or repeated: `Scopes::normalized()`), the meet, intersection, the grant step, the separator refusal, and the agent tier.
- ADR-0044 §2.4's table is carried as written, plus the tier cases of S1b.
- **Conformance:** `crates/fleetring-control/tests/vectors/tokens-v1.json`, append-only, with a `layout` field. `tests/it/tokens_vectors.rs` runs every case through the moved lattice and `tier::check`, and checks that every row of the spec's tables appears in the JSON, so the two never drift.
- Versioning: a change to any vector's result is a new spec version and a fleet feature (ADR §6.5).
- **The `_` separator.** `_` is a segment character and, not being a letter or digit, a separator for the separator refusal. The spec says so, with a vector: `admin:agent-42.c7_*` passes the tier for `agent-42.c7` and covers the sibling `agent-42.c7_x` (ADR §3's dated note). The separator refusal guards against accidental coverage; it doesn't confine a scope to one subtree.
- **Canonical delegation IDs.** A delegation ID is exactly the unpadded base64url of 16 bytes, 22 characters whose last is `A`, `Q`, `g` or `w` (its low four bits zero), so each ID has one spelling. The spec states the rule with vectors for the four accepted last characters and a refused one (`DelegationId`'s parser already enforces it), since `apply` keys records by it and a second spelling would name a second record.

## 8. S2: the issuer certificate (`fleetringd/src/tls`)

```rust
// names.rs
SubjectUri::Issuer { fleet: FleetName, name: NodeId }  // fleetring://<fleet>/issuer/<name>
// authority.rs
pub const MAX_ISSUER_LIFETIME: Duration = 90 days;    // from start to end, backdating counted
pub const ISSUER_LIFETIME_SLACK: Duration = 1 day;    // nodes only, for other authority tools (§1.2)
impl Authority { pub fn issue_issuer(&self, name: &NodeId, validity: Validity) -> Result<Issued, AuthorityError> }
//   Ed25519; IsCa::ExplicitNoCa; key usage digitalSignature only; no extended key usage; one URI SAN; refuses over 90 days
// issuer.rs
pub struct IssuerCertificate { der: Box<[u8]>, name: NodeId, key: VerifyingKey, key_id: KeyId, not_before: Timestamp, not_after: Timestamp }
impl IssuerCertificate {
    /// §1.4.2 to §1.4.4 and the length cap of §1.4.5: everything that depends on neither a token nor the time.
    pub fn check(der: &[u8], fleet: &FleetName, authorities: &[VerifyingKey]) -> Result<Self, IssuerCertError>;
    /// §1.4.5's window at `now`, with 30 s of leeway on the start.
    pub fn valid_at(&self, now: Timestamp) -> Result<(), IssuerCertError>;
    pub fn key_id(&self) -> &KeyId;
}
#[non_exhaustive] pub enum IssuerCertError { Parse, NotEd25519, AuthorityKey, UnknownAuthority, BadSignature, Subject, ExtraName,
    Authority, ExtendedKeyUsage, KeyUsage, CriticalExtension, TooLong, NotYetValid, Expired }  // lowercase messages
// conn.rs: peer_of lists Node, Principal, Authority, and Issuer explicitly:
//   Issuer => "an issuer certificate can't open a connection"
// mod.rs: #![deny(clippy::wildcard_enum_match_arm)], with tls/'s existing wildcard arms made explicit
```

- The certificate signature check uses `ring`'s `ED25519` over `tbs_certificate`, against each authority key; the algorithm OID must be 1.3.101.112.
- S3's load and reload call `check` and the key match; S4b's verifier calls `check` (through the cache), then `valid_at` on every token.

**Tests** (`tests/tls.rs`, unit tests):

- `issuer_certificate_from_this_authority_checks`.
- `issuer_certificate_refusals`, one case each: another fleet's authority; self-signed; a node certificate; a client certificate; an extra DNS subject alternative name (§1.4.3); expired; not yet valid beyond 30 s; with an extended key usage; with `keyCertSign`; marked as an authority; an unknown critical extension; a non-Ed25519 signature.
- `issuer_lifetime_is_ninety_days_plus_one_day_of_slack_on_nodes`: `issue_issuer` accepts 90 days and refuses 90 days and 1 s, counting a start set in the past; `check` accepts a certificate exactly 91 days long from another tool and refuses 91 days and 1 s.
- `issuer_certificate_cannot_open_a_connection`, on real configurations:
  - as a client certificate to the gRPC listener, refused by `peer_of`;
  - as a client certificate to the admin listener, refused and logged;
  - as a node's client certificate dialing another node, refused by `peer_of`;
  - as a server certificate dialed by a node's client configuration, refused. rustls-webpki accepts a certificate with no extended key usage for server authentication, so what refuses it is the server-name check against its URI-only subject alternative name. The test pins that, so a later change to how clients name servers can't silently admit it.

## 9. S3: the issuer files (config, CLI, dev mode)

- **Configuration.** `ControlSettings` gains `issuer_cert` and `issuer_key`. Both or neither; either one on a node without the control role is an error naming the field.
- **Load.** The pair must pass `IssuerCertificate::check` against `[tls] ca` and the key must match, or the node refuses to start, naming both files. An expired certificate isn't refused at load (§1.3 lists only token- and time-independent checks); `DelegateToken` refuses it (§1.6), and the node warns. `serve --check` runs the same checks.
- **Reload.** `IssuerPair` (`tls/issuer_files.rs`) mirrors `Identity`'s reload: a stamp check at most every second. A reload that fails `check`, or finds a key that doesn't match, keeps the pair it had and logs why. A warning from 7 days before expiry, once a day.
  - `IssuerPair::current()` returns the loaded material. S3 lands it against a `pub(crate) struct LoadedIssuer {cert, key}`; S6 builds S4a's `DelegationSigner` from it.
- **`fleetring cert issuer --name <n> [--ttl <d>] --dir <authority dir> [--out <dir>]`** writes `issuer.<n>.crt` and `issuer.<n>.key` (`0600`), `--out` created `0700`, and refuses `--ttl` over 90 days. Its result names the two files and the two `[control]` lines, and never prints the key.
- **Dev mode.**
  - At start, `fleetring dev` writes `tls/issuer.dev.crt` and `.key` (90 days) when missing; the generated configuration names them; with `dev = true` and no `[control] issuer_*`, the two files are the defaults, so a 0.7 dev directory works and its `fleetringd.toml` is never rewritten.
  - **Replacement** (§1.7): at start and at each reload check, a dev node replaces the pair when fewer than 30 days remain, when it has expired, or when the key doesn't match. It writes both to temporary names in `tls/`, syncs them, and renames the key, then the certificate. A reload between the two renames finds a mismatch and keeps the old pair until its next check.
  - `check_dev_dir` allows both files and their temporary names.
- **Tests:**
  - `cert_issuer_writes_0600_key_and_refuses_over_90_days`;
  - `control_node_with_mismatched_issuer_pair_refuses_to_start_naming_both_files`;
  - `issuer_on_a_data_only_node_is_a_config_error`;
  - `issuer_cert_without_key_is_a_config_error`;
  - `issuer_pair_reloads_without_restart`;
  - `reload_of_a_mismatched_or_invalid_pair_keeps_the_old_pair`;
  - `issuer_expiry_warns_from_seven_days`;
  - `dev_writes_issuer_pair_and_replaces_it_under_30_days_expired_or_mismatched`;
  - `dev_replaces_key_then_certificate_through_temporary_names`;
  - `dev_dir_from_0_7_uses_default_issuer_paths_without_rewriting_config`;
  - `serve_check_reports_issuer_problems`.

## 10. S4a, S4b, and S4c: delegated JWTs on every node (`auth/`)

### S4a: the format

- `Header` gains `x5c: Option<Vec<String>>`. `Claims` gains `fleetring.kind`, `fleetring.owner`, `fleetring.parent`, and `fleetring.chain: Vec<ChainEntry {id, kid}>`. All stay `deny_unknown_fields`, so a 0.7 node refuses them (§2.8).
- `TokenSpec` gains `issued_at: Timestamp`, `kind`, `owner`, and `lineage: Option<Lineage {parent, chain}>`.
  - `TokenSpec::new(principal, scopes, id, not_before, expires_at)` sets `issued_at = not_before` and no extras. Every call site moves to it, which keeps 0.7's bytes until S9.
  - `TokenSpec::agent(owner)` sets `kind` and `owner`.
- `pub struct DelegationSigner` holds the key, the `IssuerCertificate`, and the fleet. `Debug` shows the key ID and the issuer name only.

```rust
impl DelegationSigner {
    pub fn new(cert_der: &[u8], key_pkcs8: &[u8], fleet: &FleetName, authorities: &[VerifyingKey]) -> Result<Self, IssuerError>;
    pub fn key_id(&self) -> &KeyId;
    pub fn expires_at(&self) -> Timestamp;
    pub fn sign(&self, claims: &TokenClaims) -> Result<BearerToken, IssueError>; // header {alg, typ, kid, x5c: [base64(DER)]}
}
```

`sign` writes exactly the given claims, and refuses claims that fail `TokenClaims::check_recordable` (point 3.1 to 3.8 and §5.9's lead, the one implementation `apply` also runs), or whose token would exceed `MAX_TOKEN_LEN` (P13).

**Tests:** `a_depth_eight_chain_with_the_certificate_fits_in_8_kib`, with the default scopes plus a `grant`, asserting the margin left; `a_token_with_many_scopes_over_8_kib_is_refused_by_sign`; `token_spec_new_keeps_0_7_bytes` (`a_token_round_trips_and_names_its_key` stays green); `delegation_signer_debug_never_shows_the_key`.

### S4b: verification

`verify_identified` keeps its cheap-first order:

1. Size and split.
2. The header JSON.
3. `alg` and `typ`.
4. `kid` parses.
5. **With `x5c`:** exactly one entry, standard base64 (base64url is refused); `IssuerCertificate::check` through the cache (P11); `valid_at(now)`; `kid` equals the certificate key's ID. **Without `x5c`:** `kid` is an authority key.
6. The JWS signature.
7. The claims JSON.
8. `Revoked::Key(kid)`, and, with `x5c`, `Revoked::Key(cert.authority_key_id())`, so revoking an authority key ends the issuer certificates it signed (ADR §1.4.8's dated note).
9. `iss`.
10. Lifetime: 30 days for the authority, 24 h for an issuer.
11. **`iat`, for every token:** if present, at most the node's time plus 30 s (§2.7). An authority-signed token may have `iat > nbf` (a backdated `nbf`) or no `iat`, as today.
12. `nbf` with leeway, and `exp`.
13. `sub` parses.
14. **Point 3 for an issuer-signed token:** `claims.check_issuer_signed()`, never a re-implementation: `sub` is a delegated `AgentName`; `kind = agent`; `parent = sub.parent()`; `chain.len() == depth`, at most 8; `tier::check`; **`iat` present and `iat ≤ nbf < exp`**; `exp − nbf ≤ 24 h`. It doesn't check §5.9's lead, which point 3 doesn't ask of nodes. **The converse for an authority-signed one:** a dotted agent `sub`, a `parent`, or a `chain` is refused; `kind = agent` needs a root `AgentName`.
15. `revocations.refuses_token(TokenRef { id, kid, chain, principal: sub, issued_at, signer })`.

New `TokenError` variants: `IssuerCertificate(IssuerCertError)`, `X5c { reason }`, `KidMismatch`, `Claims(ClaimsViolation)` (point 3.1 to 3.8, the tier's refusals inside it), `NotDelegated` and `LineageMismatch` (the converse), `IssuedInTheFuture`. `Revoked` is reused. All lowercase; the enum is already `#[non_exhaustive]`.

`Verified` gains `signer: Signer::{Authority, Issuer { name: NodeId }}` and the full `TokenClaims`.

**The cache** (P11) is built in `Loaded::from_pem`, so each identity generation has its own.

**The fuzz target** `fuzz/fuzz_targets/token.rs` feeds arbitrary bytes to `TokenVerifier::verify_identified` (with a fixed authority, a fixed issuer certificate in the cache, and an empty denylist) and arbitrary DER to `IssuerCertificate::check`. Neither may panic. `fuzz/Cargo.toml` gains `fleetringd` as a path dependency without default features; `fuzz.yml` adds the target to its matrix.

**Tests** (`tests/delegated_tokens.rs`, unit tests in `token.rs`; a test `DelegationSigner` from an S2 authority):

- **I2: refused on every node,** one case each: `sub = agent-42`; `agent-42.c7` with `parent = agent-43`; a chain of the wrong length; a chain of 9; an entry missing `kid`; an entry with an invalid `kid`; `fleet`; `admin:agent-43-*`; `read:pub-*`; `grant:agent-42.d1.*` for `c7`; `admin:agent-42.c7x*`; a 25 h lifetime; no `iat`; `iat > nbf`; `nbf = exp`; `nbf > exp`; another fleet.
- **The header, refused:** `x5c` holding two certificates; `x5c` holding a node or a client certificate; `x5c` in base64url; `kid` other than the certificate key's ID.
- **The converse, refused:** a dotted `sub` without `x5c`; an authority-signed token with `parent` or `chain`; authority-signed `kind = agent` with `sub = workload`.
- **`iat`:** `any_token_with_iat_over_30s_ahead_is_refused` (root and delegated); `authority_token_with_backdated_nbf_and_later_iat_still_verifies`; `authority_token_without_iat_still_verifies`.
- **Accepted:** `agent-42.c7` with `admin:agent-42.c7-*` and `fork:agent-42-memory`; `tests/scopes.rs` passes untouched (0.7 JWTs).
- **P11:** `x5c_cache_never_skips_the_validity_window`, with a certificate that expires between two calls; `x5c_cache_is_dropped_when_the_authority_bundle_changes`, which replaces the `ca` file, waits for the reload, and shows a certificate from the removed authority refused.

### S4c: the connection rule, the recheck, audit, and freshness

```rust
pub enum Carriage { Direct, Relayed }
Credential::Token { id, key, expires_at, signer: SignerKind, carriage: Carriage, claims: Arc<TokenClaims> }
```

- **No client certificate:** a token on `Peer::Plaintext` is refused with `UNAUTHENTICATED`, "a bearer token needs a client certificate", before verification, whatever signed it (§4.1). `Peer::Node` keeps its refusal. This is the gRPC boundary's rule; the router's HTTP front door, whose clients present bearer tokens without certificates by design (ADR-0037), keeps its own path (`router::auth::authenticate`) unchanged, and its forwarded calls reach nodes on the router's certificate, relay-carried.
- **Direct** when the certificate's principal is `sub`, or, **for an issuer-signed token only**, an ancestor of `sub` (the principal followed by `.` is a byte prefix of `sub`).
- **Relayed** when neither applies and the certificate's principal sets `forward_tokens`.
- Anything else is `PERMISSION_DENIED`. Every match lists its variants (`auth` already denies wildcard arms).
- `Authorized::guard` and `token_holds` take the `Credential` and recheck through `refuses_token` with the chain, `sub`, `iat`, and `signer`, so a stream below a revoked ancestor ends within `STREAM_RECHECK`.
- **Audit:** `who()` writes `token:<jti>`, plus `parent=<p> chain=<id,…> issuer_key=<kid>` for a delegated token, and never token bytes.
- **Freshness (P9):** `Source::Control { handle, freshness }`. `with_revocations` matches `ControlFreshness` exhaustively.

**Tests** (`tests/delegation.rs`, unit tests in `boundary.rs`):

- **I9:**
  - `token_on_a_connection_without_a_client_certificate_is_refused` (an `Authorizer` with a TLS identity and `Peer::Plaintext`, root and delegated);
  - `child_token_on_agent_43_agent_420_agent_4_or_node_certificate_is_refused`;
  - `child_and_grandchild_tokens_ride_the_root_certificate`;
  - `root_or_service_token_never_rides_an_ancestor_certificate` (`workload.prod`'s token on `workload`'s certificate);
  - `child_token_is_relay_carried_through_a_forward_tokens_principal_and_the_router`.
- **I5, node half:**
  - `revoking_the_root_jti_ends_the_child_within_a_heartbeat_and_its_watch_within_recheck`;
  - `revoked_key_of_a_chain_copy_ends_the_grandchild`;
  - `revoked_chain_id_ends_every_copy_of_the_grandchild`;
  - `open_router_session_ends_on_an_ancestors_revocation`: a grandchild's database session through the router ends within `STREAM_RECHECK` of its parent's delegation ID being revoked on the data node.
- **The release blocker, node half.** Signers A and B built in the test, the chains of section 14: `Revoked::Key(A)`, `Revoked::Token(c7)`, and `Revoked::Token(root)` each end `r1`; `Revoked::Token(c7)` ends `r1` and `r2`; `Revoked::Key(A)` ends `r1` only.
- **I8:** `audit_line_names_lineage_and_never_token_bytes`.
- **P9:** `stale_acknowledged_control_state_refuses_every_token_and_certificates_still_work`; `sole_writer_state_is_always_current`.

### R1's fixes to S3 to S4c (2026-11-12)

Security review R1 of S1 to S4c led to these changes, with tests:

- **Nodes refuse two more issuer-signed tokens** (spec §7.1, check 4; ADR-0047's dated note to point 1.4): `exp` after the issuer certificate's end (`OutlivesCertificate`), so an open stream can't outlive the certificate; and a first chain entry whose `kid` isn't a trusted authority key (`UntrustedRoot`), so removing an authority from the `[tls] ca` bundle ends its root tokens' descendants.
- **`null` header fields and claims are refused** as malformed, never read as absent (spec §6.2 and §6.3).
- **The release blocker's node half** now certifies the issuers with a second authority, so revoking the certifying authority ends `c7`, `r1`, and `r2`, on requests and streams, but not the root; removing the `certified_by` branch of `Signer::refusal_in` fails the test.
- **The issuer pair is checked again when the trusted authorities change:** its stamp holds their key IDs, and `IssuerPair::signer` refuses a pair the current authorities don't certify. The node warns at load and reload when the issuer key is readable by other users.
- **`fleetring cert issuer` renews in place,** through temporary names, key first, as dev mode replaces its pair (`write_issuer_pair`).
- **`Identity` notices file changes by content digest,** not modification time and size, since the authority bundle decides which issuer certificates a node trusts.
- **Vectors and fuzzing:** `tests/tokens_vectors.rs` verifies the issuer-signed vectors; the vectors gain `refused_tokens`; the `token` fuzz target also signs fuzzed claims with its issuer key, with seeds in `fuzz/seeds/token`.

**For S10's `SECURITY.md`:**

- Retiring an authority means `fleetring token revoke --key-id <authority key ID>`, not removing it from the `[tls] ca` bundle alone: the revocation ends its root tokens, the issuer certificates it certified, and every descendant whose chain names its key, on every node at once, while a bundle change reaches each node only when its files change, and ends no open stream.
- After revoking an authority key, also revoke the issuer keys it certified. A descendant minted from the copy one of those issuers signed, by an issuer another authority certified, names the issuer's key in its chain but not the authority's, so the authority's revocation alone doesn't end it.

## 11. S5 and S6: minting (FR-091)

### S5: control state

```rust
// delegation/record.rs
pub struct Delegation { pub claims: TokenClaims, pub parent_token: TokenId, pub operation: OperationId,
                        pub requested_by: PrincipalName, pub recorded_by: NodeId,
                        pub recorded_at: Timestamp, pub kept_until: Timestamp }
pub struct CanonicalRequest { pub segment: Box<str>, pub scopes: Option<Scopes>,      // None = the defaults; Some is normalized
                              pub lifetime: Option<Millis>, pub not_before: Option<Timestamp>, pub caller_token: TokenId }
pub enum OperationOutcome { Recorded, Revoked { at: Timestamp } }                    // the tombstone (§6.2)
pub struct DelegationOperation { pub requested_by: PrincipalName, pub request: CanonicalRequest,
                                 pub delegation: DelegationId, pub outcome: OperationOutcome, pub kept_until: Timestamp }
pub struct RateKey { pub kind: RateKind, pub scope: RateScope }
pub enum RateKind { Delegation, AncestorRevocation }                                // the second is used from S8a
pub enum RateScope { Caller(PrincipalName), Lineage(AgentName) }                    // Lineage counts delegated agents only
// state.rs, each `#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]`
pub(crate) delegations: BTreeMap<DelegationId, Delegation>,
pub(crate) delegation_operations: BTreeMap<OperationId, DelegationOperation>,
pub(crate) rate_windows: BTreeMap<RateKey, VecDeque<Timestamp>>,
// command.rs
pub struct Command { pub at: Timestamp, #[serde(default, skip_serializing_if = "Option::is_none")] pub recorded_by: Option<NodeId>, pub action: Action }
Action::RecordDelegation {                                                          // needs [Feature::Delegation]
    operation: OperationId, requested_by: PrincipalName, request: CanonicalRequest,
    caller: CallerToken { claims: TokenClaims, kid: KeyId },                       // as presented (P3)
    id: DelegationId,
    scopes: Scopes, not_before: Timestamp, expires_at: Timestamp,                  // resolved by the handler (P4)
}
Applied::DelegationRecorded { id: DelegationId, claims: TokenClaims, resent: Resent }
Rejection::{DelegationIdTaken, DelegationStructure { reason }, DelegationClaims(ClaimsViolation), NonCanonicalScopes,
            NotNarrowed(GrantStepError), CallerRevoked { by: Revoked }, ForgedLineage { field },
            DelegationLimit { limit: Limit::{Live, CallerRate, LineageRate, Depth, Backstop} }, Unstamped, … }
```

`apply` (`delegation/apply.rs`) runs in this order and changes nothing on a refusal:

1. The feature gate.
2. **`recorded_by`** present, or `Unstamped`.
3. **Prune** expired records, operations, tombstones, and rate-window times at the effective time (`delegation::forget_through`), as `revoke_token` calls `forget_through`. Pruning happens only on the accepted path's state copy, so a refusal still changes nothing.
4. **A resend.** A known `operation` from the same `requested_by` with an equal `request` returns the record (`resent: Yes`), or refuses a tombstoned or expired one. Another requester: `OperationNotYours`. Another request: `OperationConflict { field }` in the order segment, scopes, lifetime, `not_before`, `caller_token`. Scopes compare as normalized sets, so a resend with its scopes reordered is the same request.
5. **Structure.** `requested_by == caller.principal`, an `AgentName`; `caller.kind == Agent`; `caller.owner` present; the child is `caller.principal.child(segment)`, depth ≤ 8; times are whole seconds; `iat = floor(effective)`, `nbf` raised to it; `scopes == scopes.normalized()` and at most `MAX_SCOPES`; then the child's claims, built from these, pass `TokenClaims::check_recordable` (`DelegationClaims`): the child a delegated agent name, `kind`, `parent`, a chain of one link per level, `tier::check`, `iat ≤ nbf < exp`, `exp − nbf ≤ 24 h`, and `nbf ≤ iat + 24 h` (P14), the one implementation the signer and the verifier share; `exp ≤ caller.claims.expires_at`.
6. **Against a delegated parent's record** (the caller is delegated): the child's owner is the record's owner; the child's chain is the record's chain plus `{caller.id, caller.kid}`; `record.claims.scopes.grant_step_strict(scopes)` succeeds (`NotNarrowed`); `exp ≤ record.claims.expires_at`.
7. **Revocation.** `refuses_token(caller)` with the caller's derived signer: its ID, key, whole chain, and (from S8a) its principal and ancestors.
8. **Forged lineage.** For a delegated caller: `delegations[caller.id]` exists, is live, and `record.claims == caller.claims`, field by field (I6).
9. **Limits.** Live records in the lineage below 256; the caller's `Delegation` window below 30; for a delegated caller, the lineage's `Delegation` window below 60 (the root never counted there); unpruned records in the lineage below 8,192 (the backstop).
10. **Insert** the record (`kept_until = effective + (exp − command.at) + 30 s + 1 h`), the operation (`kept_until` of the record plus 1 h), and the effective time into the caller's window and, for a delegated caller, the lineage's. `DelegationIdTaken` makes the proposer draw again.

**`RevokeToken` with `Revoked::Token(t)`** (the operator's, today's action) also prunes, in its own `apply`: every record whose ID or chain names `t` goes, its operation becomes a tombstone kept 1 h from this revocation, and the entry's `until` is raised to at least the longest pruned `kept_until` plus 30 s (§11.4, §11.5). With no delegation state, nothing changes, so pre-0.8 logs replay byte for byte. `Revoked::Key` prunes nothing. S8a adds the principal and ancestor-rule cases.

**`Tick`** prunes the same. **The timer:** `delegation::tick_due(state, now)` is true when the earliest `kept_until` of a record, operation, or tombstone has passed (and, from S8a, an ancestor-rule entry's `until`), and `control/failover.rs`'s loop submits `Tick` then, beside the fork driver's.

**`recorded_by` (P12):** `ControlOptions::recorder(NodeId)`, and `commit_group` stamps it. S6 passes the node's ID from `daemon.rs`.

**`features()`** lists every `Action` variant explicitly, with no wildcard arm; `RecordDelegation` needs `Delegation`.

**`Feature::Delegation`** (`"delegation"`) joins `ALL`, stored under `newer_features`. `fleetringd/src/software.rs` gains `running_in(Reporting::{Release, Preview})`; `Preview` adds `Delegation` only under `cfg(feature = "unreleased-delegation")`. `running()` stays `running_in(Release)`. The software test asserts every feature but `Delegation` for `Release`, and for `Preview` without the cargo feature.

**Tests** (`fleetring-control/tests/it/delegation.rs`, `delegation_replay.rs`, `replay.rs`, `rollback.rs`, `software.rs`):

- **I1 and I2, whoever proposes:** `apply_refuses_out_of_tier_scopes_whoever_proposes`; `apply_refuses_non_canonical_or_too_many_scopes`; `apply_refuses_exp_after_the_callers`; `apply_refuses_iat_after_nbf_lifetime_over_24h_nbf_over_24h_ahead_and_depth_nine`; `apply_refuses_times_not_whole_seconds`.
- **Against the parent's record:** `apply_refuses_a_child_wider_than_its_recorded_parent` (strict grant step), `apply_refuses_a_child_whose_chain_omits_its_parent`, `apply_refuses_another_owner_or_a_later_exp_than_the_parent_record`.
- **I3:** `recorded_child_extends_the_callers_chain_by_its_copy`, a property over random callers.
- **I6:** `apply_refuses_an_unrecorded_delegated_caller`; `apply_refuses_presented_claims_differing_from_the_record`, one case per field.
- **I7:** `resend_same_requester_same_request_records_once`; `resend_with_scopes_reordered_is_the_same_request`; `resend_from_another_requester_is_not_yours`; `resend_with_a_changed_field_conflicts_naming_it`; `racing_resends_record_once`; `resend_of_a_revoked_delegation_is_refused_by_its_tombstone`; `delegation_operations_are_separate_from_fork_operations`.
- **I4, minting half:** `caller_revoked_first_is_refused` (token, chain ID, chain key, own key); `prop_no_interleaving_leaves_a_live_child_of_a_revoked_parent` over `RevokeToken`, with equal stamps (S8a extends it).
- **Limits and rates:** `live_delegations_per_root_are_capped_at_256`; `revoked_delegations_free_their_slots`; `rate_per_caller_counts_effective_times_and_a_lagging_clock_cant_reopen_it`; `lineage_rate_counts_delegated_agents_and_never_refuses_the_root`; `revocation_never_reopens_the_rate_window` (mint 30, revoke them all, and the 31st within the minute is still refused); `records_per_lineage_stay_bounded_under_mint_and_revoke_churn` (S5 with `RevokeToken`; S8a adds principal churn), asserting §6.3's bound of about 6,000 records.
- **Pruning:** `records_outlive_their_tokens_then_are_pruned`; `record_delegation_and_revoke_token_prune_without_a_tick`; `tombstone_lasts_one_hour_from_the_revocation`; `operation_forgotten_after_its_retention_records_anew`; `fleet_token_revocation_until_covers_the_records_it_prunes`; `tick_due_when_the_earliest_kept_until_passes`.
- **`recorded_by`:** `record_delegation_without_recorded_by_is_refused`; `recorded_by_comes_from_the_stamp_and_replays_identically`; `commands_without_recorded_by_serialize_as_before`.
- **`features()`:** `every_delegation_action_needs_the_feature` (S5 for `RecordDelegation`; S8a adds its actions).
- **Replay (model-based):** `delegation_replay.rs` follows `fork_replay.rs`'s `step_action(state, step, clock)`. Steps are drawn (delegate from a live root token or recorded delegation with a valid or deliberately invalid request, resend an operation, revoke a live token, advance the clock across a window or a `kept_until`, tick), and the action is built against the current state, so most commands are meant to succeed. Over fixed seeds it asserts:
  - replay from scratch, from snapshots, and through serialization gives identical states and outcomes;
  - accepted counts per action: at least 100 `RecordDelegation`, 20 resends, and 20 revocations that prune, and at least one refusal of each `Limit` but `Backstop`;
  - `Limit::Backstop` never appears.
- **Byte stability:** `state_without_delegations_serializes_as_before`, against `pre_0_8_state.json`; `log_without_delegations_serializes_as_before`, against `pre_0_8_log.json`.
- **Rollback:** `a_log_with_delegation_enabled_cant_load_without_it`.
- **I8:** `control_state_never_holds_token_bytes`.
- **`fleetringd`:** `control_writer_stamps_its_node_id`; `timer_ticks_when_a_kept_until_passes_on_a_busy_control_plane`.

### S6: `Control.DelegateToken` and `Control.ListDelegations`

The proto:

```proto
// Mints a delegated agent's token after recording the delegation (ADR-0047 point 5). The token
// never reaches a log. Needs the `delegation` fleet feature.
// Scope: an agent's token whose grant covers the child (checked in the handler).
rpc DelegateToken(DelegateTokenRequest) returns (DelegateTokenResponse);
message DelegateTokenRequest {
  bytes operation_id = 1; string segment = 2; repeated string scopes = 3;
  optional uint64 lifetime_ms = 4; optional uint64 not_before_ms = 5;
}
message DelegateTokenResponse {
  string token = 1; string delegation_id = 2; string principal = 3; repeated string scopes = 4;
  uint64 not_before_ms = 5; uint64 expires_at_ms = 6; bool capped = 7; string issuer_key_id = 8;
}
// Lists live delegation records, never tokens, for incident response (ADR-0047 point 6.4).
// Scope: fleet
rpc ListDelegations(ListDelegationsRequest) returns (ListDelegationsResponse);
message ListDelegationsRequest { optional string recorded_by = 1; optional uint64 recorded_from_ms = 2; optional uint64 recorded_to_ms = 3; }
message DelegationRecord { string principal = 1; string delegation_id = 2; string parent = 3; repeated ChainLink chain = 4;
                           uint64 issued_at_ms = 5; uint64 expires_at_ms = 6; string recorded_by = 7; }
message ChainLink { string id = 1; string kid = 2; }
message ListDelegationsResponse { repeated DelegationRecord delegations = 1; }
```

`Requirement::Delegator` admits a token-authenticated principal; its `Display` is the `Scope:` line. The handler reads the `Credential` through `Authorized` (`auth/boundary.rs`).

The `DelegateToken` handler (`control/service.rs`, with the pure parts in `control/delegation.rs`) checks in this order. Every refusal comes before anything is logged.

1. **The caller.** `Credential::Token` with `Carriage::Direct`, or `PERMISSION_DENIED` "delegation needs a direct connection with the certificate of the caller or an ancestor"; `claims.kind == Agent`; `sub` is an `AgentName`.
2. **The feature.** `state.check_features` for `RecordDelegation`, or `FeatureUnsupported` naming the member.
3. **The issuer** (§1.6): configured, its key not revoked, its certificate not expired. Otherwise `FAILED_PRECONDITION` naming the file and `fleetring cert issuer`.
4. **Time.** If `state.now()` leads `self.clock.now()` by more than 30 s: `UNAVAILABLE`, "control time leads this control node's clock by <n> s". Otherwise `now = max(clock, state.now())`.
5. **A resend from state**, answered without logging: requester, then canonical request, then live and not refused by `refuses_token`, then re-signed with the current pair if its certificate outlives `exp`. Refusals: "this operation's delegation has expired", "this operation's delegation is revoked".
6. **Scopes** (`resolve_scopes`): explicit, `grant_step_strict`; defaults, `grant_step` of `{admin:<child>-*, stream:<child>-*}`, empty refused as "your token holds no grant over agent-42.c7-"; then `tier::check`, naming the first scope that fails.
7. **Times** (`resolve_times`): `not_before` defaults to `now`; an explicit one before `now − 30 s`, after `now + 24 h`, or at or after `P.exp` is refused; the default lifetime is `min(2 h, P.exp − nbf)`, with `capped`; an explicit lifetime over `P.exp − nbf`, over 24 h, or under 60 s is refused; both floored; the issuer certificate must expire after `exp`.
8. **Size** (P13): a trial signature over the resolved claims must fit `MAX_TOKEN_LEN`.
9. **The ID:** 16 bytes from `self.entropy`, base64url-encoded.
10. **Submit** `RecordDelegation`, drawing again on `DelegationIdTaken` (at most 4 draws).
11. **Sign** the recorded claims. A signing failure answers `UNAVAILABLE` "retry with the same operation ID". The token is never logged, put in an error, or used as a metric label.
12. **The audit line:** caller, child, scopes, expiry, operation, delegation ID, issuer key ID, and result.

`ListDelegations` needs `fleet`, reads state, filters by `recorded_by` and the window of `recorded_at`, and returns records only.

The rest of S6:

- `ControlClient::delegate_token` resends on `UNAVAILABLE` under the same operation ID; `ControlClient::list_delegations`.
- `daemon.rs`: gives `ControlService` the `IssuerPair`, passes `ControlOptions::recorder(node_id)`, and chooses `Reporting::Preview` only in dev mode or the simulation's environment; `membership.rs` and `sim/client.rs` report `running_in` that choice.
- `crates/fleetringd/README.md`: the issuer, `cert issuer`, and the two RPCs.
- **Simulation.** `Fleet::builder().control_issuer(name)`; `Fleet::rotate_issuer(host)` writes a new pair, picked up by a reload or after `crash` and `restart`. A delegation workload records each token's outcome in the `History`. `tests/sim/main.rs` gains `mod delegation;`.

**Tests** (`tests/delegate_token.rs` against a dev node, `#![cfg(feature = "unreleased-delegation")]`; `tests/sim/delegation.rs` for several nodes, gated the same way; unit tests in `control/delegation.rs`):

- **I1:** `root_default_child_gets_admin_and_stream_on_its_prefix_capped_and_said`; `read_only_root_default_child_gets_read_and_stream`; `admin_requested_by_a_read_only_root_is_not_covered`; `no_grant_is_refused`; `write_agent_42_memory_outside_the_grant_is_refused`; `fleet_requested_is_refused`; `grant_agent_42_star_for_a_child_is_refused_by_the_tier`; `lifetime_remaining_plus_one_second_25h_and_59s_are_refused`; `not_before_31s_past_24h_and_1s_ahead_or_at_parent_exp_is_refused`; `segments_with_dot_dash_or_uppercase_are_refused`; `child_name_over_64_bytes_is_refused`; `depth_nine_is_refused`; `delegation_whose_token_would_exceed_8_kib_is_refused_before_recording`; `prop_resolved_scopes_equal_the_grant_step`.
- **The caller and the issuer:** `token_without_kind_or_service_token_cannot_delegate`; `certificate_alone_cannot_delegate`; `relay_carried_token_cannot_delegate` (I9); `no_issuer_revoked_issuer_expired_issuer_and_short_issuer_are_failed_precondition`; `delegation_refused_while_control_time_leads_the_clock`.
- **Grandchildren:** `child_with_grant_delegates_within_its_subtree_and_never_outlives_its_parent`.
- **I7:** `retry_same_key_returns_identical_bytes`; `retry_after_rotation_returns_same_id_and_claims_signed_by_b_and_both_verify`; `retry_with_scopes_reordered_returns_the_same_delegation`; `retry_from_another_principal_reveals_nothing`; `retry_with_another_field_is_already_exists`; `retry_of_an_expired_or_revoked_delegation_is_refused_never_reminted`; `lost_reply_retry_records_once` (simulation).
- **I6:** `forged_token_with_unrecorded_id_cannot_delegate`; `issuer_key_resigned_real_claims_with_wider_scopes_cannot_delegate`.
- **The feature gate:** `member_without_delegation_refuses_naming_it_and_logs_nothing` (simulation, `JoinWith` without the feature); after the first delegation, that member can't join. `non_dev_daemon_with_the_preview_never_reports_delegation`.
- **`ListDelegations`:** `list_delegations_needs_fleet_and_never_returns_tokens`; `list_delegations_filters_by_recorded_by_and_window`.
- **I8:** `delegation_never_logs_the_token` (a delegation, a retry, and five refusals).
- **The simulation:** `sim::delegation::delegate_under_partitions_and_control_restart`, with `history` checking no token is used outside its scopes; `sim::delegation::revocation_races_delegation` (I4 end to end with `RevokeToken` until S8a).

### R2's fixes to S5 and S6 (2026-11-13)

Security review R2 of S5 and S6 led to these changes, with tests:

- **A refused delegation is never logged.** The writer keeps the preview `apply`'s outcome for a command that needs `Feature::Delegation` and answers its rejection with `SubmitError::Refused`, taking no log index (`control/node.rs`, `refused_unlogged`). A rate limit's refusals no longer grow `control.db`, and a refused first use leaves no command the 0.7 line can't decode while the feature is off. A rejection changes nothing, `State::now` included, so the log replays the same. Other commands' rejections are still logged: their callers read them from `Committed`, and the previous line decodes them. S8a's ancestor-rule revocations need `Feature::Delegation`, so the rule covers them, and their handlers must read `SubmitError::Refused` as a rejection, as `delegate` does.
- **A resend returns only its own operation's record** (`record.operation == operation`, else `DelegationExpired`), so a later delegation that drew a forgotten record's ID is never handed to the first operation's requester.
- **A resend must present the copy of its token the delegation was minted from:** a `kid` other than the record's last chain link's is refused with `DelegationConflict { field: CallerKey }`, `ALREADY_EXISTS` (ADR-0047's dated note to point 8). The handler and `apply` share it through `State::delegation_resend`.
- **The CI simulation jobs and the nightly sweeps** build with `unreleased-delegation`, so `sim::delegation` runs under `--cfg tokio_unstable` and random seeds; S9 removes the feature from them.
- **New tests** kill mutants that survived: the handler's `check_lineage` on a resend, the effective time in `iat`, `recorded_at` and `kept_until`, the stamp in the live count, the 8,192 backstop, `DelegationIdTaken`, `ListDelegations`' live filter, the issuer refusal for a revoked certifying authority key, and the handler's denylist re-check.

## 12. S7a and S7b: the CLI (FR-092)

### S7a: root agents that can delegate

- `agent::delegation_support(client) -> Support::{Every, Lacking { node, release }, Unknown}` reads the first `WatchMembership` update. Without a client, or without `fleet`, it is `Unknown`.
- On `Every`, `agent add`, dev `mcp --agent`, and `agent renew` mint `TokenSpec::agent(owner)` with the 0.8 defaults: read-write `admin`, `stream`, and `grant`; read-only `read`, `stream`, and `grant`; each on `agent-<id>-*` and `agent-<id>.*`.
- Otherwise FR-081's defaults, with "this agent can delegate after `agent renew` once every node runs 0.8", naming the lacking node.
- `AgentId::widened` upgrades exactly 0.7's default or FR-081's. `token issue` gains no way to set `kind` (§2.6).

**Tests** (`tests/agents.rs`): `fresh_agent_holds_grant_kind_and_owner_when_every_member_reports_delegation` (`#[cfg(feature = "unreleased-delegation")]`); `agent_add_on_a_fleet_lacking_delegation_mints_fr081_defaults_and_says_why`; `renew_upgrades_exactly_0_7_or_fr081_defaults_never_flag_scopes`; `token_issue_cannot_mint_kind_agent`; `version_1_identity_still_serves`.

### S7b: children

**`agent delegate <child-id> [--scope]… [--ttl] [--not-before] [--operation-id] [-o table|json]`:**

- Resolves the parent identity and creates `<agents dir>/<child-id>/` (`0700`) under `.lock`.
- Writes `operation` before the call, and resends up to 5 times within 60 s on `UNAVAILABLE` (including "revoked less than a second ago") or a lost reply. A re-run that finds `operation` but no `identity.toml` reuses it.
- Writes `token` (`0600`), then `identity.toml` (version 2, `issued_by = "delegation"`, with `principal`, `owner`, `parent`, `root`, `scopes`, `read_only`, `token_id` (the delegation ID), `token_expires_at`, `issuer_key_id`, and `operation`; no `cert_expires_at`).
- Prints the trusted-local-development line. Exit codes and hints follow `docs/cli.md` §16.

**The rest of S7b:**

- `AgentRecord` accepts `cert_expires_at` absent only at version 2 with `issued_by = "delegation"`. `IssuedBy::Delegation` is added.
- `--agent 42.c7` and `FLEETRING_AGENT=42.c7` resolve to `<agents dir>/42/`'s certificate and key, with `<agents dir>/42.c7/token`. `agent add 42.c7` is refused, naming `agent delegate`.
- `mcp --agent 42.c7` serves the child's token and never mints. It names the parent, the owner, and the expiry, and prints the trusted-local-development line.
- `agent list` gains a `PARENT` column; `agent show` adds the lineage fields; the JSON adds `parent`, `root`, `owner`, `delegation_id`, `issuer_key_id`, and `operation_id`, with `cert_expires_at` `null` for a child.
- **`agent list --fleet [--recorded-by <node-id>] [--since <time>]`** calls `ListDelegations` with `fleet`, prints the records, and says that `recorded_by` is advisory after a compromise (§6.1).

**Tests** (`tests/agents.rs`, `tests/mcp.rs`; the delegation cases `#[cfg(feature = "unreleased-delegation")]`): `delegate_then_act_as_the_child_creates_agent_42_c7_scratch`; `child_is_refused`, one case each (writing `agent-42-memory`; creating `agent-42-x`, `agent-43-x`, or `agent-42.c70-x`; reading `agent-42.d1-x`; delegating `42.c7.r1` without `grant`); `child_with_fork_scope_forks_agent_42_memory`; `read_only_parents_child_shows_read_only`; `ttl_and_scope_refusals_and_the_capped_note`; `child_directory_holds_only_token_identity_and_lock`; `copied_child_token_with_agent_43_certificate_is_refused`; `rerun_after_crash_reuses_the_operation_id_and_records_once`; `agent_add_dotted_is_refused_naming_delegate`; `version_2_child_record_refused_by_the_0_7_version_gate`; `agent_list_fleet_prints_live_records_by_window_and_needs_fleet`; `mcp_as_child_serves_only_its_prefix`; `mcp_tool_list_has_no_delegate_or_token_tool` (§7).

## 13. S8a, S8b, and S9: revocation (FR-093)

### S8a: control state

- **`Revoked::Principal { fleet: Option<FleetRef>, principal: PrincipalName }`** as a key, with `before` in the entry (P7). `fleet` is always `None` in 0.8: `apply` refuses `Some`, and a node treats `Some` as unreadable.
- **Entries** become P7's `Entry { until, added, before, of }`. `insert` raises `before` and `until` in place for an existing principal, and an entry added with `fleet` stays `Fleet`.
- **`refuses_token`** gains principals: `principal.is_at_or_below(Q)` and `iat × 1000 ≤ before` for `SignerKind::Issuer`, `iat × 1000 ≤ before + 30,000` for `SignerKind::Authority`; a missing `iat` is refused. Checking `sub` alone is equivalent to ADR §3's "`sub` or any ancestor derived from `sub`", since an ancestor of `sub` is at or below `Q` only if `sub` is, and both readings compare the token's own `iat`. Nodes, the recheck, and `apply` change together (P1).
- **Actions** (P6), each needing `Feature::Delegation`, listed explicitly in `features()`:
  - `Action::RevokePrincipal { principal }`: with `fleet`.
  - `Action::RevokeAsAncestor { target: AncestorTarget::{Token(TokenId), Principal(AgentName)}, caller: CallerToken }`.
  - `Action::RevokeToken { revoked: Revoked::Principal { .. }, .. }` is refused by `apply` (`Rejection::PrincipalNeedsItsAction`); `features()` gives it `Delegation` as well, so a mixed fleet refuses it first.
- **The ancestor rule in `apply`** (§11.3): the caller is an agent name; a root caller's presented claims are authority-signed (derived), a delegated caller's are recorded, live, and equal to its record (as §5.9); every target is an agent name at or below the caller. `Token(id)`: `id` is the caller's own presented ID, or a recorded delegation whose principal is below the caller; otherwise `NotFound` ("no live delegation with that token id"). `Principal(Q)`: `Q` is the caller, or the principal of a recorded delegation below it. `Revoked::Key` is never a target.
- **`before`** is the effective time. **`until`** (§11.5): a root principal entry, 30 days, 30 s, and 1 h from the effective time; a delegated principal entry, the longest covered `kept_until` plus 30 s; an ancestor-rule token entry for a delegation, the longest `kept_until` of it and the records below it plus 30 s; for the caller's own root token, its `exp` as a duration from the effective time, plus 30 s and 1 h. With `fleet`: today's rule, raised to the longest pruned `kept_until` plus 30 s. Every replacement keeps `until` at least the longest it replaces.
- **Replacement** (§11.4): only a principal entry for `Q` replaces others: `Ancestor` principal entries below `Q`, and `Ancestor` token entries whose `of` is at or below `Q`. Root-token entries and `Fleet` entries are never replaced. A token entry replaces nothing.
- **Pruning** (§11.4): every `Revoked::Token` and `Revoked::Principal`, by `fleet` or the ancestor rule, removes the records whose ID or chain names the token or whose principal is at or below `Q`, and tombstones their operations for 1 h. All of it happens in the same `apply`, with the expired-state pruning of S5.
- **Bounds** (§11.6): at most 1,024 counted `Ancestor` entries per root lineage, refused only when a revocation would raise the count (never one that raises an existing key or replaces at least one entry, and never a root's revocation of its own principal or a root token it presented), naming the caller's child whose subtree holds the most entries; `AncestorRevocation` windows of 30 per caller and 60 from the lineage's delegated agents, the root never counted there.
- **`RecordDelegation`** gains, through `refuses_token`, the principal check on the caller, and the **same-second refusal** (§5.9, §11.5): a `Revoked::Principal` covering the child's own name with `before ≥ iat × 1000` refuses with `Rejection::RevokedThisSecond`, which the handler answers with `UNAVAILABLE`.
- `tick_due` also covers ancestor-rule entries' `until`.
- **The wire.** `Revocation` gains `oneof` case `PrincipalRevocation principal = 4 { optional string fleet = 1; string principal = 2; uint64 before_ms = 3; }`. `RevokeTokenRequest` gains `string principal = 2`; a request whose `revocation` holds the principal case is `INVALID_ARGUMENT`, so a caller never sends `before`. `convert.rs` maps the case both ways.

**Tests** (`tests/it/revocation.rs`, `delegation.rs`, `delegation_replay.rs`, `replay.rs`, `rollback.rs`):

- `principal_revocation_matches_agent_42_and_below_never_agent_420`;
- `principal_entries_are_one_per_fleet_and_principal_raised_in_place`;
- `before_is_the_effective_time_and_the_leeway_is_by_signer`;
- `missing_iat_counts_as_issued_before_every_before`;
- `until_follows_the_name_and_what_the_entry_covers`; `fleet_principal_revocation_until_covers_the_records_it_prunes`;
- `only_principal_entries_replace_and_never_root_token_or_fleet_entries`;
- `token_entry_replaces_nothing_with_a_reused_segment` (`42.c7` revoked by ID, delegated again, a grandchild under each, then `agent-42.c7`'s principal revoked: neither grandchild revives);
- `revocation_prunes_what_it_ends_and_tombstones_their_operations`;
- `same_second_redelegation_of_a_revoked_name_is_refused_as_retryable`;
- `prop_no_interleaving_leaves_a_live_child_of_a_revoked_parent`, extended to the new actions, with equal stamps (I4);
- **I10:** `ancestor_rule_allows_self_own_token_and_recorded_delegations_below`; `ancestor_rule_refuses_siblings_parents_other_roots_unrecorded_names_and_keys`; `services_cannot_use_the_ancestor_rule`; `forged_caller_claims_cannot_revoke`;
- **I11:** `ancestor_entries_capped_per_lineage_root_self_revocation_exempt`; `cap_never_refuses_a_revocation_that_replaces`; `ancestor_rate_per_caller_and_lineage_never_counts_the_root`; `revocation_never_reopens_the_rate_window`, for ancestor revocations; `records_per_lineage_stay_bounded_under_mint_and_principal_revoke_churn`;
- **The feature boundary:** `every_delegation_action_needs_the_feature` (all of them); `fleet_principal_revocation_on_a_mixed_fleet_is_refused`; `revoke_token_with_a_principal_target_is_refused`;
- `fleet_some_is_refused_in_0_8`;
- the model-based replay generator gains the new actions and asserts accepted counts for each, and that the backstop and the cap's refusal of a replacing revocation never appear;
- `a_log_with_a_principal_entry_cant_load_on_0_7`;
- **Byte stability:** `revocations_without_new_entries_serialize_as_before`; `revocation_entries_serialize_stably_once_delegation_is_enabled`, a fixture with one entry of each shape (fleet token, fleet key, fleet principal, ancestor principal, ancestor token with `of`, ancestor root token), whose bytes are pinned.

### S8b: nodes and the handler

**The unreadable-denylist rule.** `revocations_from_proto` returns `Result<Revocations, UnreadableEntry { index, reason }>`. An absent, unknown, or malformed case (including `fleet: Some`) makes the join or heartbeat store `Denylist::Unusable { since, reason }`. While unusable, `with_revocations` answers `UNAVAILABLE` for every token, and certificates keep working. The node logs the entry once per distinct list.

**`RevokeToken`.**

- `Requirement::FleetOrAncestor`; its `Display` is "fleet, or the ancestor rule".
- With `fleet`: the operator path is unchanged; `principal` becomes `RevokePrincipal`.
- Without `fleet`: a token-authenticated `AgentName`, `Carriage::Direct`, becomes `RevokeAsAncestor`. **A relay-carried token** is accepted only when the target is `Token` of its own `jti`; anything else is `PERMISSION_DENIED`.
- The request's `until_ms` is ignored without `fleet`; the response states the `before` and `until` that `apply` chose.
- `NotFound` maps to `NOT_FOUND` "no live delegation with that token id"; `RevokedThisSecond` from `DelegateToken` maps to `UNAVAILABLE` "agent-42.c7 was revoked less than a second ago; retry".
- The audit line records the caller, the target, and `authority=fleet|ancestor`.
- `crates/fleetringd/README.md`: who may revoke what.

**Tests** (`tests/delegation.rs`, `tests/control.rs`; delegation cases gated on the preview feature):

- `unreadable_entry_makes_every_token_unavailable_until_a_readable_list_arrives` (a control stub that sends an unknown `oneof` case);
- `certificates_keep_working_with_an_unusable_list`;
- `principal_revocation_ends_tokens_and_open_streams_within_recheck` (I5);
- `root_token_signed_29s_ahead_is_refused_and_delegated_token_minted_after_is_not` (the leeway by signer);
- `ancestor_revocation_relay_carried_is_refused` and `relay_carried_token_revokes_only_its_own_jti` (I9);
- `request_cannot_carry_before`;
- `agent_42_cannot_revoke_agent_43_agent_43_x_agent_420_or_agent_4`; `child_cannot_revoke_parent_or_sibling`; `unknown_token_id_is_not_found_revealing_nothing`; `ancestor_cannot_revoke_a_key`;
- `same_second_redelegation_answers_unavailable`;
- `member_without_delegation_refuses_ancestor_revocations_naming_it`.

### S8b's notes (2026-11-13)

What S8b settled where this section was silent or wrong about the code:

- **The heartbeat's denylist is read in `membership.rs`**, not `data/node.rs`; S8b changed its two call sites to `Denylist::receive`. The unusable state lives in `auth/denylist.rs` (`Unusable { since, entry }`, `UnreadableEntry { index, reason }`), and `Denylist::load` returns it as an error, so no caller can check a token against an unusable list by mistake.
- **The principal checks on nodes needed no new code:** S8a's `refuses_token` already judged principals with the leeway by `TokenRef::signer`, and `Signer::refusal_in` already called it on verification and the recheck. S8b pins it on both paths, on the control node's own state and on a data node's heard list.
- **One request may name a token and a principal** (`RevokeTokenRequest { revocation, principal }`). S9's `agent remove <root>` needs both, and a root's token authenticates no second request once either lands. The principal is revoked first, then the token; a delegated caller's revocation of its own principal skips a token that is its own or a delegation recorded below it, which the principal covers and whose revocation `apply` would refuse as unrecorded. If the token's revocation fails after the principal's landed, the status says the principal is revoked. The reply gains `before_ms` and `principal_until_ms`; `until_ms` keeps 0.7's meaning for the token or key.
- **`ControlClient::delegate_token` resends on `UNAVAILABLE`**, so a CLI sees "revoked less than a second ago" only through its own retries; the S8b test uses a client that sends once.
- **The handler, before `apply`:** refuses a key target and a relay-carried request other than the caller's own `jti` alone (`PERMISSION_DENIED`), a non-agent token without `fleet` (`PERMISSION_DENIED`, so a service never enables the feature), and a caller its current state revokes (`CallerRevoked`, as `delegate` does). Both delegation-gated paths check the build's reporting and the fleet's feature first, as `delegate` does.

### R3's fixes to S8a and S8b (2026-11-13)

- **A root's revocation of itself is outside the rates** (finding 2). `RevokeAsAncestor` of the root's own principal or presented root token is neither counted nor refused by the caller's rate, as the cap already exempts it; each ends the token that asked. `AncestorTarget::PrincipalAndToken` carries both of `agent remove <root>`'s targets in one command, so they land together or not at all, and `apply` gives a token the principal covers no entry (`Applied::PrincipalAndTokenRevoked`, `token_until: None`). The handler sends one command for an ancestor-rule request that names both, for any caller, replacing S8b's delegated-caller skip; with `fleet` it still sends two. The RPC is unchanged.
- **The cap has two budgets per lineage** (finding 1, the owner's option (b), ADR-0047 §11.6 and I11 as amended). An entry counts in the root's own budget if the root added it (`Added::Ancestor { by == root }`), otherwise in the descendants' shared one, each 1,024; the budget is derived from `added`, so the state, its bytes, and replay are unchanged. A revocation is refused only when it adds more new entries to the caller's budget than it replaces from that budget, and would take it past 1,024: a root at its own cap that would replace only descendants' entries is refused, which keeps the 2,048 bound. `RevocationLimit::RootEntries` says "the root's own budget is full" and suggests waiting, revoking the root itself, or `fleet`; `Entries` still names the busiest child, counted in the descendants' budget.
- **An ancestor-rule target needs only to be recorded** (finding 4), as §11.3 says: a delegation past its `exp` by the leader's clock is kept for its margin, while lagging nodes may still accept it, so `apply` now matches targets by `is_kept`, not `is_live`. The caller's own record must still be live (I6).
- **An unusable denylist is visible** (finding 5): `/readyz`'s `denylist` check fails with "denylist entry <i> is unreadable (<reason>); tokens are refused", and the new gauge `fleetring_denylist_unusable` reads 1, where before readiness read only the list's age and the sampler reported 0 entries.
- **A principal and a token it covers succeed for any caller** (finding 6): with both in one command, `apply` decides coverage, for a root or any caller naming a lower principal and a delegation recorded at or below it, so the handler never answers "…is revoked, but the token isn't" for a token already dead.
- **Tests R3 added** (finding 7): `revocation_races` runs by token and by principal with `fleet`, and by principal and by token under the ancestor rule, through the simulation's `RevokePrincipal` and `RevokeAs` operations; and tests now kill the surviving mutants (a `Fleet` principal entry replaced, a twin at the caller's name taken as below it, root-token and root-principal entries counted, `tick_due` blind to ancestor entries, the handler's own caller-revoked check, and its build guard on the ancestor path). The replay generator sends `PrincipalAndToken` for every other self or root principal revocation and checks both budgets after every command.

### S9: signing time, the CLI's revocations, reporting the feature, and the release blocker

- **Signing time.** `TokenSigner::issue` writes `iat = spec.issued_at`, apart from `nbf`; it adds no refusal. Every caller (`agent.rs`, `main.rs token issue`, `sim/fleet.rs`) passes its clock's reading at signing time.
- **The CLI.** `agent remove <child>` revokes the child's principal as its nearest local ancestor identity. `agent remove <root>` revokes, without `fleet`, the root's principal and its own token ID, and prints when a replacement may be minted (`before` plus 30 s). `agent renew <child>` delegates again with a new operation ID, then revokes the old delegation ID. `agent renew <root>` uses the ancestor rule without `fleet`, through a relay too (its own `jti`). `token revoke --principal <name>` asks for the name to be typed; `--token-id` and `--principal` follow the ancestor rule with `--agent`. A delegation the CLI resends after "revoked less than a second ago" succeeds.
- **Reporting the feature.** `SUPPORTED` gains `Feature::Delegation`; `Reporting` and `running_in` are deleted; the `unreleased-delegation` feature, the `lib.rs` guard, and every `cfg(feature = "unreleased-delegation")` are deleted. Gate: `git grep -n "unreleased-delegation\|unreleased_delegation"` prints nothing.
- **Tests:** `token_iat_is_signing_time_apart_from_nbf`; `future_nbf_token_claims_signing_time_iat`; `0_7_tokens_with_iat_equal_nbf_still_verify`; `agent_remove_child_ends_child_and_grandchild_parent_keeps_working`; `agent_remove_root_without_fleet_ends_the_subtree_by_principal_and_token_id`; `new_root_token_minted_31s_after_self_revocation_works`; `remove_then_delegate_the_same_child_within_a_second_retries_and_works`; `renew_child_revokes_the_old_delegation_id`; `renewing_a_parent_never_renews_children`; `racing_agent_remove_and_agent_delegate_never_leave_r1_usable`; `this_build_reports_delegation`; the release-blocker tests of section 14.

### S9 and S10, as built (2026-11-13)

- **S9** landed as written above: `iat` as signing time (`TokenSpec::signed_at`), the CLI's revocations (`agent remove` and `agent renew` for children and roots, `token revoke --principal`, and the ancestor rule with `--agent`), the release-blocker tests (`tests/release_blocker.rs`), and the flip: `SUPPORTED` holds `Feature::Delegation`, and the preview feature, its `lib.rs` guard, and every `cfg(feature = "unreleased-delegation")` are deleted, as is its use in the simulation jobs. The release blocker's leader change restarts a control node on its port, which needed the daemon to free a stopped node's port and rebind it while old connections linger (`f7a582b`).
- **`agent remove <root-id>`** tries the ancestor rule first, as section 13 says, so on a fleet of 0.8 nodes it enables `delegation`; it falls back to revoking the token ID with `fleet` only when the control plane refuses the agent's own revocation. `agent renew <root-id>` revokes the replaced token with `fleet` first, which keeps rollback open. The release notes say so (S10).
- **S10** updated `SECURITY.md` (the delegation threat model, revocation, retiring an authority, the unusable denylist, trusted local development, and the clocks), the agent skill and quickstart, the lexicon (the delegation terms are current), `docs/cli.md` (0.8.0, with §16 built), the roadmap, the READMEs, and the 0.8.0 release notes in `RELEASING.md`, and added FR-064's checklist lines for the freshness hook and `recorded_by`. The ADR notes to 0025, 0026, 0039, 0043, and 0044 were already in place from FR-089. The "0.8.0 isn't cut between FR-090 and FR-093" line section 15 asks for in `RELEASING.md` isn't added: FR-093 has landed, so it would be removed in the same release.

## 14. The release blocker, and the invariants and compromises mapped to tests

**The release blocker: subtree revocation across issuer rotation and leader changes (I5).**

In 0.8 a "leader change" is a control node restart with new issuer files, and a "rotation" is a reload of new files without a restart (section 1). Every case runs once with a restart and once with a reload, on a fresh fleet of three daemons (`tests/release_blocker.rs`, after S9 removes the preview, so the daemons need no dev mode), and in the simulation (`sim::delegation::subtree_revocation_across_issuers`, over the CI seeds, with `rotate_issuer`, `crash`, and `restart`).

**Setup.**

1. Root `agent-42` delegates `42.c7` under issuer key A with explicit scopes `admin:agent-42.c7-*`, `stream:agent-42.c7-*`, `admin:agent-42.c7.*`, `stream:agent-42.c7.*`, and `grant:agent-42.c7.*`, so `c7` may delegate.
2. Rotate to issuer key B.
3. `42.c7` delegates `42.c7.r1` (defaults) from its A-signed copy. The chain of `r1` is `[{root, authority}, {c7, A}]`.
4. `agent-42`, the requester of step 1, retries step 1's operation with the same canonical request, and receives a B-signed copy of `c7` with the same delegation ID.
5. `42.c7` delegates `42.c7.r2` from the B copy. The chain of `r2` is `[{root, authority}, {c7, B}]`.
6. Each of `c7` (both copies), `r1`, and `r2` opens a `WatchChanges` stream on its own database.

**Cases.**

| Case | What is revoked, and by whom | Must end within one heartbeat interval, streams within `STREAM_RECHECK` | Must survive |
|---|---|---|---|
| 1 | `Revoked::Key(A)` (operator) | `c7`'s A copy and `r1` | `c7`'s B copy and `r2` |
| 2 | `Revoked::Token(c7)` (operator; separately, by `agent-42` under the ancestor rule) | `c7` (both copies), `r1`, and `r2` | `agent-42` |
| 3 | `Revoked::Token(root jti)` (operator) | everything below `agent-42` | — |
| 4 | `Revoked::Principal(agent-42.c7)` (by `agent-42`) | `c7`, `r1`, and `r2` | `agent-42` |
| 5 | `agent remove 42` (by `agent-42` itself) | everything at or below `agent-42` | a new root token minted more than 30 s later |
| 6 | A reused segment: `Revoked::Token(c7)` by `agent-42`; `agent-42` delegates `42.c7` again (`c7′`), which delegates `r1′`; then `Revoked::Principal(agent-42.c7)` by `agent-42` | `r1`, `r2`, `c7′`, and `r1′`; no replacement revives `r1` or `r2` | `agent-42` |

After each case, a delegation attempted from any ended token is refused (I4).

| Invariant | Tests |
|---|---|
| I1 Narrowing | S5's `apply_refuses_*` and parent-record tests; S6's `root_default_child_*`, `lifetime_*`, `prop_resolved_scopes_equal_the_grant_step` |
| I2 The tier | S1b's tier tests; S1c's vectors; S4b's refusal list and converse; S5's `apply_refuses_out_of_tier_scopes_whoever_proposes` |
| I3 Lineage | S5's `recorded_child_extends_the_callers_chain_by_its_copy`; S4c's `child_and_grandchild_tokens_ride_the_root_certificate` |
| I4 Ordering | `prop_no_interleaving_*` (S5, S8a); `same_second_redelegation_*` (S8a, S8b); `sim::delegation::revocation_races_delegation`; S9's `racing_agent_remove_and_agent_delegate_*` |
| I5 Subtree revocation | the release blocker; S4c's I5 tests and `open_router_session_ends_*`; S8a's reused-segment test; S8b's `unreadable_entry_*` and `principal_revocation_ends_*`; S4c's freshness tests |
| I6 Forged lineage | S5's `apply_refuses_an_unrecorded_*` and `*_differing_from_the_record`; S6's `forged_token_*` and `issuer_key_resigned_*`; S8a's `forged_caller_claims_cannot_revoke` |
| I7 Retries | S5's resend tests; S6's `retry_*` and `lost_reply_retry_records_once` |
| I8 No bytes at rest | S4c's audit test; S5's `control_state_never_holds_token_bytes`; S6's `delegation_never_logs_the_token` |
| I9 The connection rule | S4c's I9 tests; S6's `relay_carried_token_cannot_delegate`; S8b's relay tests |
| I10 The ancestor rule's reach | S8a's I10 tests; S8b's refusal list |
| I11 Bounded denylist and records | S8a's I11 tests; S5's and S8a's churn tests; the replay generator's backstop assertion |

| Compromise (ADR §8) | Tests |
|---|---|
| An issuer key | S4b's I2 list with a valid issuer certificate; S6's `forged_token_*` and `issuer_key_resigned_*`; release-blocker case 1 |
| A control node | S5's `apply_*` tests with arbitrary proposers; release-blocker case 4; S6's `list_delegations_filters_by_recorded_by_and_window` |
| A relay's certificate key | `child_token_is_relay_carried_*`; `relay_carried_token_cannot_delegate`; `relay_carried_token_revokes_only_its_own_jti` |
| A root agent's certificate key | the existing test that `agent-<id>` has no `[[principal]]`; `child_and_grandchild_tokens_ride_the_root_certificate` |
| A child's token | `copied_child_token_with_agent_43_certificate_is_refused`; `child_token_on_agent_43_agent_420_agent_4_or_node_certificate_is_refused` |
| A parent's token | release-blocker cases 2 and 6; `agent_remove_child_ends_child_and_grandchild_*`; S5's limits |

## 15. Upgrade from 0.7 (FR-096, #146)

**What must keep working**, and the stage that proves each item:

- **A 0.7 JWT verifies unchanged.** `tests/scopes.rs` untouched (S4b); `0_7_tokens_with_iat_equal_nbf_still_verify` (S9).
- **A token another JWT library minted with a backdated `nbf`** still verifies (S4b's `authority_token_with_backdated_nbf_*`).
- **A version 1 `identity.toml` still serves** (S7a). A version 2 child record is refused by 0.7's version gate (S7b).
- **0.7 control state and logs load and re-serialize byte for byte.** The three maps are skipped when empty, `recorded_by` when absent (S5); `Fleet` entries keep 0.7's form (P7, S8a).
- **While the fleet is mixed:** `DelegateToken` and the ancestor actions answer `FeatureUnsupported` (S6, S8b); `agent add` and `agent renew` mint FR-081's defaults and say why (S7a); a token with `grant`, `x5c`, or the new claims is refused by a 0.7 node (FR-096's nightly test).
- **Rollback is allowed until the first `RecordDelegation`, `Revoked::Principal`, or ancestor-rule revocation, and refused after.** `recorded_by` on commands logged before then is ignored by a 0.7 binary, since `Command` has no `deny_unknown_fields`. Proof: the `rollback.rs` cases in S5 and S8a, and FR-096's nightly run.
- **`[control] issuer_*` and 0.7.** `ControlSettings` is `deny_unknown_fields`, so a 0.7 binary refuses these keys. The release notes (S10, finished by FR-096) say to add them after every node runs 0.8 and to remove them before a rollback. Dev mode never writes them into a 0.7 directory's configuration (S3).
- **`RELEASING.md`** gains "0.8.0 isn't cut between FR-090 and FR-093" (S10); FR-096 removes it.

## 16. Risks

| Risk | What the plan does |
|---|---|
| **Moving the lattice changes what `apply` accepts later.** | S1a moves code without change; S1c pins the vectors in a normative spec; ADR §6.5 makes any change that alters `apply`'s outcome a fleet feature. |
| **The node rule and the `apply` rule drift apart.** | P1: one implementation, in `fleetring-control`, called by both. |
| **No Raft in 0.8.** ADR-0040 point 4 and I5's follower bound rest on designs not yet built. | "A leader change" is tested as a restart with new files. P9's typed hook and P12's stamp are in place; S10 records what FR-064 fills in. Open point O1. |
| **`delegation` reported before it is enforced** breaks I5. | P8: a preview build reports it only in dev mode and the simulation, whatever its debug assertions; S9 deletes the preview and greps that it is gone. |
| **Records or rate state grow without bound.** | Rate windows count commands, not records; pruning in `apply` and a timer `Tick`; the 8,192 backstop; the churn tests and the replay generator's assertion. |
| **A token too large to use** after many scopes. | P13: refused before recording, and by `sign`. |
| **One malformed entry stops every token fleet-wide.** | Chosen by the ADR. `apply` validates every entry before it enters state; `every_revoked_variant_round_trips_proto`; the node logs the entry. |
| **A root token's principal revocation depends on signer clock skew** (30 s). | `agent remove <root>` also revokes the token ID (S9). |
| **Retries return different bytes after a rotation.** | Tests compare claims and IDs, never bytes, except on one key. |
| **`x5c` parsing as a denial-of-service vector.** | The 8 KiB cap, cheap-first order, one certificate, the per-generation cache, and the fuzz target. |
| **Credentials leak into logs or state.** | `Debug` redacts `DelegationSigner`, `IssuerPair`, and `BearerToken`; I8 tests. |
| **`TokenSpec` churn breaks 0.7 bytes.** | `TokenSpec::new` preserves `iat = nbf` until S9. |
| **An issuer certificate used for TLS.** | It has no extended key usage, which rustls-webpki reads as "any use", so `peer_of` refuses its subject kind explicitly and S2's tests cover every listener and the client side. |
| **Review load.** | FR-090 is split into S1a to S4c, FR-092 into S7a and S7b, FR-093 into S8a, S8b, and S9; four Fable reviews, R1 before minting is built. |

## 17. Gates for every stage

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo +1.88 check --workspace --exclude fleetringd --exclude fleetring-client --exclude fleetring-wal-sqlite-tests --all-features --all-targets
cargo +1.89 check -p fleetringd -p fleetring-client -p fleetring-wal-sqlite-tests --all-features --all-targets
```

Also, before the push: the simulation suite (`--features simulation`, CI seeds); `cargo test -p fleetringd` **without** `--all-features`, which proves the stage builds and passes without the preview feature; and, for S4b, `cargo +nightly fuzz build` in `fuzz/`.

## 18. Open points for the owner

**If the owner answers ADR-0047's questions differently**, these stages change:

| ADR question | Other answer | What changes |
|---|---|---|
| 1 (relay-carried tokens) | (b) accept them | S6 and S8b drop the `Carriage::Relayed` refusals; the relay tests flip to "accepted". `Carriage` stays, for audit lines. |
| 2 (forged lineage by claims) | (b) ID only | S5's and S8a's forged-lineage step compares ID and liveness only. `issuer_key_resigned_*` becomes a test of documented exposure. |
| 3 (separator refusal on nodes) | (b) the handler only | S4b stops refusing `NoSeparator`; `tier::check` keeps it for `apply` and the handler; the spec marks the case "handler only". |
| 4 (root removes itself, token ID too) | no | S9's `agent remove <root>` revokes the principal only; release-blocker case 5 then depends on the clock assumption. |
| 5 (operation record retention) | (a) or (c) | S5's operation retention and its tests; the bounds in §6.3 change. |
| 6 (small choices) | any | Constants in S5, S6, and S8a. |

**O1. Prerequisites: accept ADR-0047, and say how ADR-0040 point 4 and `recorded_by` apply to 0.8.**
- (a) Accept ADR-0047 with answers 1 to 6 as recommended. Record that in 0.8 the one control node applies every command, so the gate over members is enough, and ADR-0040 point 4 is needed only when FR-064 builds a group. `recorded_by` is the single writer's own node ID, stamped by `commit_group` beside `at` (P12), and FR-064's state machine takes it from the entry's leader ID.
- (b) Accept ADR-0040 point 4 first.

*Recommended: (a).* No 0.8 fleet has a control group.

**O2. I5's state-age bound on a single control node.**
- (a) The one writer's state is current by definition, behind P9's typed hook, which FR-064 fills in.
- (b) Refuse every token if no command was applied within the heartbeat timeout. A quiet control-only fleet would then refuse every token.

*Recommended: (a).* ADR-0047 now says the same.

**O3. Delivery.**
- (a) One PR for all four tickets, with four Fable reviews on its diff ranges.
- (b) One PR per ticket: four CI runs, and `main` holding a half-built chain between merges.

*Recommended: (a).*

**O4. Early reporting of `delegation` (P8).**
- (a) The `unreleased-delegation` preview reports it only in dev mode and the simulation, never to a non-dev node in production, and S9 deletes it.
- (b) No early reporting. Every end-to-end test of FR-090 to FR-092 moves to S9, and failures surface late.

*Recommended: (a).* It departs from the review's "refuse to start outside dev mode", for the reason given in P8.

**O5. Small additions beyond the ADR.**
- (i) A same-second re-delegation of a revoked name: now ADR text (§5.9, §11.5), built in S8a and S8b. Nothing to decide.
- (ii) The `x5c` cache, in the loaded identity generation, keyed by the TBS bytes (P11).
- (iii) `Revoked::Principal` with `fleet: Some` refused by `apply`, and treated as unreadable by nodes, in 0.8.
- (iv) `apply` refuses a `not_before` more than 24 h after `iat` (P14), and the handler refuses a delegation whose token would exceed 8 KiB (P13).

*Recommended: (ii), (iii), and (iv).*

**O6. Bounding unpruned delegation records.** Settled by ADR §6.2 and §6.3: rate windows that count commands, pruning in `apply` and a timer `Tick`, and the 8,192 backstop. Nothing to decide.

To unblock, reply: "Accept the FR-090 to FR-093 plan with O1(a), O2(a), O3(a), O4(a), and O5 (ii) to (iv); start S1a and S2 once ADR-0047 is accepted."

## Re-check pins (2026-11-12)

A second adversarial check of this plan (Fable, high effort) returned "proceed with listed changes". ADR-0047 is accepted, at the commit that adds `nbf ≤ iat + 24 h` to point 5.9, so the "flagged to the author" note in P14 is settled. Each pin lands before the stage it names.

- **S1a, S1b: keep the wildcard lint.** `#![deny(clippy::wildcard_enum_match_arm)]` sits at the top of `scope.rs`, `matcher.rs` and `delegation/` in `fleetring-control`. The lattice is consensus code, and moving it out of `fleetringd::auth` would otherwise drop the lint.
- **S1a, S5: emulated test cost.** `fleetring-control`'s tests run on i686, emulated s390x and wasm (`ci.yml`). Time the moved 3,000-case lattice tests and the model-based replay under emulation. Put heavy cases behind `#[ignore]`, keeping a small default case count.
- **S5, S8a: check first, prune last.**
  - `apply` mutates `State` in place, and a refused command must change nothing.
  - Write every check (the 256 live, the rates, the backstop, liveness) as a function of `(state, now)` that ignores entries past their `kept_until` or `until`.
  - Prune only after the last check passes.
  - Test: a refused `RecordDelegation` or `RevokeToken` leaves the state byte-identical, and expired entries never count toward a limit.
- **S5, S8a: "live" means kept, and `exp > command.at`,** judged by the command's own stamped time, not its effective time (ADR §5.9).
- **S6: how preview reporting recognises the simulation.**
  - `Reporting::Preview` keys on `config.dev`, or on a private field set only inside the `#[cfg(feature = "simulation")]` `Environment::simulated`, with no setter.
  - Never `Execution::Inline`, which is also a production setting.
  - Never `FLEETRING_DEV_DETACHED`.
  - S6 owns `environment.rs`.
  - Test: a non-dev daemon with `[engine] execution = "inline"` never reports `delegation`.
- **S6 (optional second line):** in a preview build, `DelegateToken` and the ancestor-rule path refuse outside dev mode or the simulation (`ControlService` already holds `dev`). The start-up warning says a dev directory that enabled `delegation` must be deleted before a build without the feature uses it.
- **S6 to S8b: the sim gate includes the preview feature.** Section 17's simulation gate runs with `--features simulation,fault-hooks,unreleased-delegation`, so `sim::delegation::*` (I4 and the release blocker) runs across seeds before R2, not only on the default seed in the `--all-features` job.
- **S4a: complete call-site list.**
  - `TokenSpec { … }` literals also exist in `router/auth.rs`, `router/tests.rs`, `auth/boundary.rs`, and `tests/{scopes,mcp,database,router/fleet}.rs`.
  - S4a owns those constructor edits.
  - Where this plan says `tests/scopes.rs` is untouched, read "its assertions are untouched; its constructors moved in S4a".
- **S2, S4b: the `x5c` cache key and eviction.**
  - `IssuerCertificate::check` refuses trailing bytes after the DER, so a public certificate plus junk can't make unlimited keys.
  - *Added 2026-11-12, after the review of S1a, S1b and S2:* trailing bytes weren't enough, since x509-parser also ignores junk inside the outer `SEQUENCE` and accepts long-form lengths. `check` now requires the canonical DER, and each read extension's canonical DER; S4b keys the cache on the TBS bytes all the same, so no wrapper variant reaches a new entry.
  - The cache (16 entries) evicts expired entries first, then the oldest.
- **Review timing.** A short Fable pass reviews S1a, S1b and S2 after W2, before S4a and S5 start. R1 covers the rest of W1 to W5 as planned.
