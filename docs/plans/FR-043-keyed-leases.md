---
id: FR-043
title: Keyed leases: acquire, renew, release, and watch a lease on any key, with no database behind it
status: todo
priority: P2
size: L
milestone: M8 Access and integration
depends_on: [FR-042, FR-029, FR-047]
adrs: [ADR-0017, ADR-0018, ADR-0025, ADR-0027]
---

# FR-043: Keyed leases: acquire, renew, release, and watch a lease on any key, with no database behind it

## Context

Leases today exist only for databases: the control plane grants one holder per database per epoch, fences the rest, and the ring decides who should hold it (ADR-0018). The same mechanism answers a broader question that applications keep asking: who owns `tenant:acme`'s background job, or `agent:42`'s session, right now, and how does the owner know when it no longer does? Temporal's shard ownership and Orleans' grain directory are the usual answers: a placement decision plus a fenced, renewable lease, with the holder notified when it loses it. FleetRing already has the placement (the ring) and the fencing (epochs). What's missing is a lease that isn't tied to opening a database. This ticket is gated on an ADR that decides the semantics before any code: whether a keyed lease is placed by the ring or requested by a holder, what renewal and expiry look like without a replication stream to prove liveness, and what the holder is promised across a control-plane restart.

ADR-0027 is that record. In short: a holder requests a lease (the ring doesn't place it); a holder is the caller's authenticated identity plus an instance name, composed at the request boundary; one fleet-wide counter gives every grant a fresh epoch, which is the fencing token; expiry runs on command time, while the holder stops at a conservative deadline of its own; every grant, renewal, and release is durable in the control log before it is answered; watchers follow control state through `Control.WatchKeyLeases`; limits live in the control configuration; and the RPCs need `write` on the key to hold and `read` to watch.

## Scope

- **An ADR first** (`docs/adr/`): the lease model for keys, the epoch and fencing rules, expiry on missed renewals, and what a watcher sees. Accepted before implementation starts. *Written: ADR-0027.*
- **One control action family in `fleetring-control`:** `AcquireLease { key, holder, ttl }`, `RenewLease { key, holder, epoch }`, `ReleaseLease { key, holder, epoch }`, applied through the pure `apply` with time inside the command (ADR-0017), replay-safe, and carrying an epoch that every later command must present. Nothing else: no values, no locks on groups of keys, no fairness. *Named `AcquireKeyLease`, `RenewKeyLease`, and `ReleaseKeyLease`, since `AcquireLease` and `RenewLease` are the database lease's actions.*
- **`WatchLeases` extended** (FR-042) to keyed leases, with the key in place of the database name, so a holder learns it lost the lease the same way a data node does. *Decided by ADR-0027 point 8: a separate stream, `Control.WatchKeyLeases`, from control state, with the same snapshot-then-events shape and the same `LeaseEventKind`; `Database.WatchLeases` stays one node's view of its database leases.*
- **The Node service exposes the four calls** for co-located workers; the control node applies them. *Decided by ADR-0027 point 11: the four calls are on the `Control` service, which the control node serves and `ControlClient` reaches through leader redirects, so no relay sits in a renewal's path.*
- **Not in scope:** storing data under keys, leader election across fleets, and placement of keys by anything but the key's name.

## Definition of done

- [ ] The ADR is accepted and linked from `docs/adr/README.md`.
- [ ] A lease on a key has at most one holder per epoch; a stale holder's renew and release are refused with the current epoch, and the replay test covers every command.
- [ ] A holder that stops renewing loses the lease after its TTL, by the control plane's clock inside commands, and a watcher sees `Lost` then `Gained` for the next holder in epoch order.
- [ ] A control-plane restart neither grants a second holder nor loses a renewed lease; the simulation history checker verifies single ownership per epoch under faults.
- [ ] Every new RPC is in `fleetringd::auth::RPCS` with the requirement ADR-0027 point 10 gives, with matching `// Scope:` lines in the proto and rows in the daemon README; a principal with `write:tenant:acme/*` can hold `tenant:acme/jobs` and not `tenant:zed/jobs`, and its watch shows only keys it may read.
- [ ] A holder's identity comes from its certificate or token: two principals naming the same instance hold different leases, and neither can renew or release the other's.
- [ ] `fleetringd lease acquire|renew|release|watch|hold` and the `Client` methods are documented in the daemon README, with the contract of ADR-0027 point 6 (stop at the local deadline; pass the epoch to every resource).
- [ ] Logs and snapshots written before this ticket load and replay to the same states.

## Implementation plan

Start only once ADR-0027 is accepted and FR-047 has merged. Each step is about one commit (`feat(control): …` for `fleetring-control`, `feat(fleetringd): …` for the daemon, `Refs: FR-043`). Every new public type derives `Debug`, `Clone`, `PartialEq`, `Eq` (and `Serialize`/`Deserialize` in `fleetring-control`), is added to its crate's `assert_debug_send_sync` list, and every error enum is `#[non_exhaustive]` with lowercase messages. No `bool` parameters.

### Step 1: key-lease types and limits (`fleetring-control`)

- New file `crates/fleetring-control/src/key_lease.rs`, declared in `lib.rs` (`mod key_lease;`) and re-exported:

  ```rust
  /// A key a keyed lease is held on (ADR-0027): 1 to 255 bytes of UTF-8 without control
  /// characters, the syntax of a database name, in a namespace of its own.
  #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
  #[serde(transparent)]
  pub struct LeaseKey(DatabaseName);
  impl LeaseKey {
      pub fn as_str(&self) -> &str;
      /// The key as scope rules match it: the same prefixes govern databases and keyed leases.
      pub fn as_name(&self) -> &DatabaseName;
  }
  impl FromStr for LeaseKey { type Err = ParseDatabaseNameError; … }   // delegates to DatabaseName
  impl fmt::Display for LeaseKey { … }

  /// Who holds a keyed lease: `<identity>/<instance>`, composed by the daemon from the caller's
  /// authenticated identity. 1 to 255 bytes without control characters; compared, never parsed.
  #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
  #[serde(try_from = "String", into = "String")]
  pub struct KeyLeaseHolder(Box<str>);
  impl KeyLeaseHolder { pub fn as_str(&self) -> &str; }
  impl FromStr for KeyLeaseHolder { type Err = ParseKeyLeaseHolderError; … }
  pub enum ParseKeyLeaseHolderError {
      #[error("key lease holder is empty")] Empty,
      #[error("key lease holder is {len} bytes; the maximum is {max}")] TooLong { len: usize, max: usize },
      #[error("key lease holder contains a control character")] ControlCharacter,
  }

  /// A keyed lease as control state records it.
  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
  pub struct KeyLease { holder: KeyLeaseHolder, epoch: LeaseEpoch, ttl: Millis, granted_at: Timestamp, expires_at: Timestamp }
  impl KeyLease {
      pub fn holder(&self) -> &KeyLeaseHolder;
      pub fn epoch(&self) -> LeaseEpoch;
      pub fn ttl(&self) -> Millis;
      pub fn granted_at(&self) -> Timestamp;
      pub fn expires_at(&self) -> Timestamp;
      /// Whether the lease has expired at `at`: `at > expires_at`.
      pub fn is_expired_at(&self, at: Timestamp) -> bool;
  }

  /// How many keyed leases may be live, and the TTLs a holder may ask for (ADR-0027 point 9).
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
  pub struct KeyLeaseLimits { max_keys: u32, min_ttl: Millis, max_ttl: Millis }
  impl KeyLeaseLimits {
      /// 10,000 keys, TTLs from 5 s to 300 s.
      pub const DEFAULT: Self;
      /// # Errors
      /// [`KeyLeaseLimitsError`] if `max_keys` is 0, `min_ttl` is 0, or `min_ttl > max_ttl`.
      pub const fn new(max_keys: u32, min_ttl: Millis, max_ttl: Millis) -> Result<Self, KeyLeaseLimitsError>;
      pub const fn max_keys(&self) -> u32;
      pub const fn min_ttl(&self) -> Millis;
      pub const fn max_ttl(&self) -> Millis;
  }
  impl Default for KeyLeaseLimits { fn default() -> Self { Self::DEFAULT } }
  pub enum KeyLeaseLimitsError {
      #[error("key lease limits need at least one key")] NoKeys,
      #[error("the minimum key lease ttl must be at least 1 ms")] ZeroTtl,
      #[error("the minimum key lease ttl {min} exceeds the maximum {max}")] Inverted { min: Millis, max: Millis },
  }

  /// Whether an acquire granted a new epoch or re-armed the caller's live lease.
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
  #[serde(rename_all = "snake_case")]
  pub enum AcquireKind { Granted, Rearmed }
  ```

- `crates/fleetring-control/src/config.rs`: `ControlConfig` gains `#[serde(default)] key_leases: KeyLeaseLimits`; `ControlConfig::new()` sets `KeyLeaseLimits::DEFAULT`; add `pub const fn with_key_lease_limits(mut self, limits: KeyLeaseLimits) -> Self` and `pub const fn key_lease_limits(&self) -> KeyLeaseLimits`. The type comment says the limits are fixed with the rest of the configuration when a log is created.
- `lib.rs` module docs: one bullet on keyed leases in the list of what `State` holds, and a **Keyed leases** paragraph under *Rules* that states ADR-0027 points 4 to 6 in two sentences each.
- Tests, unit (in `key_lease.rs`): `lease_keys_take_database_name_syntax`, `holders_refuse_empty_long_and_control_characters`, `limits_refuse_zero_and_inverted_values`, `a_lease_expires_strictly_after_its_expiry`. Integration (`tests/it/scenarios.rs`): `a_control_config_from_before_key_leases_loads_with_the_defaults` (deserialize `{"lease_duration":10000,"failover_margin":2000,"heartbeat_timeout":5000}` and compare `key_lease_limits()` with `KeyLeaseLimits::DEFAULT`).

### Step 2: commands, outcomes, state, and `apply` (`fleetring-control`)

- `src/command.rs`, enum `Action`, three variants after `TransferLease`; each doc comment ends "Logs written before this action existed never contain it.":

  ```rust
  /// Grants `key`'s lease to `holder` for `ttl` (ADR-0027 point 5).
  AcquireKeyLease { key: LeaseKey, holder: KeyLeaseHolder, ttl: Millis },
  /// Extends `holder`'s lease on `key` at `epoch` by its recorded TTL.
  RenewKeyLease { key: LeaseKey, holder: KeyLeaseHolder, epoch: LeaseEpoch },
  /// Ends `holder`'s lease on `key` at `epoch`.
  ReleaseKeyLease { key: LeaseKey, holder: KeyLeaseHolder, epoch: LeaseEpoch },
  ```

- `src/apply.rs`:
  - `Applied` gains `KeyLeaseAcquired { key: LeaseKey, lease: KeyLease, kind: AcquireKind }`, `KeyLeaseRenewed { key: LeaseKey, lease: KeyLease }`, `KeyLeaseReleased { key: LeaseKey, epoch: LeaseEpoch }`.
  - `Rejection` gains, with these exact messages:

    | Variant | Fields | Message |
    |---|---|---|
    | `InvalidKeyLeaseTtl` | `ttl: Millis, min: Millis, max: Millis` | `key lease ttl {ttl} is outside {min} to {max}` |
    | `KeyLeaseHeld` | `key: LeaseKey, holder: KeyLeaseHolder, epoch: LeaseEpoch, retry_after: Millis` | `key '{key}' is leased to '{holder}' at epoch {epoch}; retry after {retry_after}` |
    | `TooManyKeyLeases` | `max_keys: u32` | `the fleet already holds {max_keys} key leases` |
    | `KeyLeaseEpochExhausted` | — | `key lease epochs are exhausted` |
    | `UnknownKeyLease` | `key: LeaseKey` | `key '{key}' has no lease` |
    | `StaleKeyLeaseEpoch` | `key: LeaseKey, given: LeaseEpoch, current: LeaseEpoch` | `epoch {given} of key '{key}' is stale; the current epoch is {current}` |
    | `NotKeyLeaseHolder` | `key: LeaseKey, holder: KeyLeaseHolder` | `'{holder}' does not hold the lease of key '{key}'` |
    | `KeyLeaseExpired` | `key: LeaseKey, epoch: LeaseEpoch` | `the lease of key '{key}' at epoch {epoch} has expired` |

  - `apply` dispatches the three actions to `acquire_key_lease(state, now, key, holder, ttl)`, `renew_key_lease(state, now, &key, &holder, epoch)`, `release_key_lease(state, key, holder, epoch)`, implementing ADR-0027 point 5 in exactly its order of checks:
    - acquire: ttl within `state.config.key_lease_limits()` (inclusive), else `InvalidKeyLeaseTtl`; if a record for `key` exists and `!is_expired_at(now)`: same holder re-arms (`ttl` replaced, `expires_at = now.saturating_add(ttl)`, epoch kept, `AcquireKind::Rearmed`), another holder is `KeyLeaseHeld` with `retry_after = Millis::new(expires_at - now + 1)`; otherwise count the records other than `key` with `!is_expired_at(now)` and refuse with `TooManyKeyLeases` when the count is `>= max_keys`; take `state.key_lease_epoch.next()` or refuse with `KeyLeaseEpochExhausted`; then, and only then, remove every record with `is_expired_at(now)`, insert the new record (`granted_at = now`), store the new counter, and return `Granted`.
    - renew: no record is `UnknownKeyLease`; another epoch `StaleKeyLeaseEpoch` (with the record's); another holder `NotKeyLeaseHolder` (with the caller's holder); expired `KeyLeaseExpired`; else `expires_at = now.saturating_add(record.ttl)`.
    - release: the first three checks of renew, then remove the record; expired records may be released.
- `src/state.rs`: `State` gains

  ```rust
  /// Keyed leases by key, expired ones included until the next acquire forgets them.
  #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
  pub(crate) key_leases: BTreeMap<LeaseKey, KeyLease>,
  /// The epoch of the last keyed lease granted, fleet-wide (ADR-0027 point 4).
  #[serde(default, skip_serializing_if = "is_zero")]
  pub(crate) key_lease_epoch: LeaseEpoch,
  ```

  (`fn is_zero(epoch: &LeaseEpoch) -> bool { *epoch == LeaseEpoch::ZERO }`, private), initialized empty in `State::new`, and three queries: `pub fn key_lease(&self, key: &LeaseKey) -> Option<&KeyLease>` (the record, expired or not), `pub fn key_leases_held(&self, at: Timestamp) -> impl Iterator<Item = (&LeaseKey, &KeyLease)>` (records not expired at `at.max(self.now)`, in key order), and `pub fn key_lease_epoch(&self) -> LeaseEpoch`. The `skip_serializing_if` attributes keep snapshots of fleets without keyed leases byte-identical to today's.
- Tests:
  - New module `crates/fleetring-control/tests/it/key_leases.rs` (add `mod key_leases;` to `tests/it/main.rs`), using `common::run` and a config with `KeyLeaseLimits::new(3, Millis::new(100), Millis::new(10_000))`:
    - `a_key_lease_is_acquired_renewed_and_released`: acquire at 0 with ttl 1,000 is `Granted` at epoch 1 expiring at 1,000; renew at 600 expires at 1,600; release at 700 removes it; `key_leases_held` is empty.
    - `a_second_holder_is_refused_until_expiry_and_then_gets_a_higher_epoch`: `b` at 500 gets `KeyLeaseHeld { retry_after: 501 ms, … }`; `b` at 1,001 is `Granted` at epoch 2.
    - `the_holder_reacquiring_keeps_its_epoch_and_rearms_the_ttl`: `a` at 0 (ttl 1,000), `a` again at 800 with ttl 2,000: `Rearmed`, epoch 1, expires at 2,800.
    - `a_stale_holder_cannot_renew_or_release_after_a_new_grant`: after `b` takes epoch 2, `a`'s renew and release at epoch 1 are `StaleKeyLeaseEpoch { given: 1, current: 2 }`; `b`'s renew naming holder `a` is `NotKeyLeaseHolder`.
    - `renewing_an_expired_lease_is_refused_even_if_nobody_took_it`: renew at 1,001 is `KeyLeaseExpired`; release at 1,001 succeeds.
    - `a_ttl_outside_the_limits_is_rejected`: 99 and 10,001 are `InvalidKeyLeaseTtl`; 100 and 10,000 are granted.
    - `the_limit_counts_only_live_leases_and_an_acquire_forgets_expired_ones`: three live keys refuse a fourth with `TooManyKeyLeases { max_keys: 3 }`; after one expires, the fourth is granted and `key_lease()` of the expired key is `None`.
    - `epochs_never_repeat_across_releases_or_keys`: acquire, release, acquire on `k1`, then `k2`: epochs 1, 2, 3.
    - `keyed_leases_and_database_leases_do_not_share_a_namespace`: a database `tenant:acme` with a lease at epoch 1 and a keyed lease on `tenant:acme` granted at epoch 1; dropping the database leaves the keyed lease; releasing the keyed lease leaves the database lease.
    - `a_rejected_key_lease_command_changes_nothing`: for each rejection above, the state before equals the state after, `now` included.
    - `a_snapshot_without_key_leases_serializes_as_before`: a state built from the pre-existing scenario commands serializes with no `key_leases` or `key_lease_epoch` field.
  - `tests/it/scenarios.rs`, `rejection_messages_are_lowercase`: add `KeyLeaseHeld` and `StaleKeyLeaseEpoch` with their exact strings.
  - `tests/it/common/mod.rs`: `pub(crate) const KEYS: [&str; 4] = ["k0", "k1", "k2", "k3"]`, `pub(crate) const HOLDERS: [&str; 3] = ["node:n0/w", "principal:app/w1", "principal:app/w2"]`; `config()` adds `.with_key_lease_limits(KeyLeaseLimits::new(3, Millis::new(100), Millis::new(2_000)).expect(…))`; `action()` matches `op.kind % 20` instead of `% 16`, keeping arms 0 to 15 as they are (and FR-047's `RevokeToken` arm if it is there), and adds: `16 | 17` acquire (`KEYS[a % 4]`, `HOLDERS[b % 3]`, ttl `50 + (b * 37) % 2_100` ms, so some fall outside the limits); `18` renew aimed at the current record's holder and epoch unless `b % 5 == 0` (then a wrong holder) or `b % 7 == 0` (then epoch minus one); `19` release aimed the same way.
  - `tests/it/replay.rs`, `the_generated_log_exercises_every_kind_of_change`: count `Applied::KeyLeaseAcquired`, `KeyLeaseRenewed`, and `KeyLeaseReleased` as `"key acquired"`, `"key renewed"`, `"key released"`, and require each. The two replay tests then cover the new commands as they cover every other.
  - `tests/it/properties.rs`, three properties over `drive`: `no_two_key_leases_share_an_epoch` (every `KeyLeaseAcquired { kind: Granted }` epoch is new across all keys), `a_key_has_at_most_one_live_holder` (after every command, at most one record per key, and every record in `key_leases_held(now)` has `expires_at >= now`), `key_lease_epochs_only_grow_per_key` (for each key, granted epochs strictly increase).

### Step 3: protocol, control service, and control client (`fleetringd`)

- `crates/fleetring-proto/proto/fleetring/v1/fleetring.proto`, service `Control`, after FR-047's `RevokeToken` (new messages only; no existing field changes):

  ```proto
  // Grants a key's lease (ADR-0027): to the caller's identity and `instance`, for `ttl_ms`. A key
  // leased to another holder answers FAILED_PRECONDITION with `fleetring-retry-after-ms` metadata.
  // Scope: write on the lease key.
  rpc AcquireKeyLease(AcquireKeyLeaseRequest) returns (KeyLeaseResponse);
  // Extends the caller's lease at `epoch` by its TTL. An expired lease isn't renewed.
  // Scope: write on the lease key.
  rpc RenewKeyLease(RenewKeyLeaseRequest) returns (KeyLeaseResponse);
  // Ends the caller's lease at `epoch`.
  // Scope: write on the lease key.
  rpc ReleaseKeyLease(ReleaseKeyLeaseRequest) returns (ReleaseKeyLeaseResponse);
  // Every live keyed lease under `prefix`, then one event per lease gained or lost, in epoch order
  // per key. Shows only the keys the caller may read.
  // Scope: read per lease key listed, or fleet.
  rpc WatchKeyLeases(WatchKeyLeasesRequest) returns (stream KeyLeaseUpdate);

  message AcquireKeyLeaseRequest {
    string key = 1;
    // 1 to 64 of A-Z, a-z, 0-9, '.', '_', '-'.
    string instance = 2;
    uint64 ttl_ms = 3;
  }
  message RenewKeyLeaseRequest {
    string key = 1;
    string instance = 2;
    uint64 epoch = 3;
  }
  message ReleaseKeyLeaseRequest {
    string key = 1;
    string instance = 2;
    uint64 epoch = 3;
  }
  message KeyLeaseResponse {
    string key = 1;
    // `<identity>/<instance>`, as the control state records it.
    string holder = 2;
    uint64 epoch = 3;
    uint64 ttl_ms = 4;
    // Set when an acquire re-armed the caller's live lease instead of granting a new epoch.
    bool rearmed = 5;
  }
  message ReleaseKeyLeaseResponse {
    uint64 epoch = 1;
  }
  message WatchKeyLeasesRequest {
    // A literal key prefix; empty for every key.
    string prefix = 1;
  }
  message KeyLeaseUpdate {
    oneof update {
      HeldKeyLeases held = 1;
      KeyLeaseEvent event = 2;
    }
  }
  message HeldKeyLeases {
    repeated HeldKeyLease leases = 1;
  }
  message HeldKeyLease {
    string key = 1;
    string holder = 2;
    uint64 epoch = 3;
  }
  message KeyLeaseEvent {
    string key = 1;
    string holder = 2;
    uint64 epoch = 3;
    LeaseEventKind kind = 4;
  }
  ```

- `crates/fleetringd/src/control/key_leases.rs` (new, `mod key_leases;` in `control/mod.rs`):
  - `pub(crate) const RETRY_AFTER_KEY: &str = "fleetring-retry-after-ms";`
  - `pub(crate) fn holder_for(caller: &Caller, instance: &str) -> Result<KeyLeaseHolder, Status>`: `INVALID_ARGUMENT` unless `instance` is 1 to 64 bytes of `[A-Za-z0-9._-]`; `Caller::Node(id)` gives `node:{id}/{instance}`, `Caller::Principal { name, .. }` gives `principal:{name}/{instance}`, `Caller::Anonymous` gives `anonymous/{instance}`.
  - `pub(crate) type KeyLeaseEvents = BTreeMap<LeaseKey, (KeyLeaseHolder, LeaseEpoch)>;` and the pure `pub(crate) fn key_lease_events(before: &KeyLeaseEvents, after: &KeyLeaseEvents) -> Vec<KeyLeaseEvent>`: in key order, a key only in `before` gives `LOST` at its epoch; only in `after` gives `GAINED`; in both with a different epoch gives `LOST` (old) then `GAINED` (new).
  - `pub(crate) fn key_lease_stream(states: watch::Receiver<Arc<State>>, prefix: String, visible: impl Fn(&LeaseKey) -> bool + Send + 'static) -> KeyLeaseStream`: built like `ControlService::watch_membership` (`WatchStream::new(states)` and `flat_map`), it sends the snapshot of `key_leases_held(state.now())` filtered by prefix and `visible` on the first state, then for each later state the events between the filtered held sets.
- `crates/fleetringd/src/control/service.rs`:
  - handlers `acquire_key_lease`, `renew_key_lease`, `release_key_lease`: parse `key` as `LeaseKey` (`INVALID_ARGUMENT` otherwise), call `auth::require_key(&request, key.as_name())?`, compose the holder with `holder_for(authorized(&request)?.caller(), &instance)`, check `ttl_ms` fits `Millis`, `submit` the action, and answer `KeyLeaseResponse` (`rearmed` from `AcquireKind`);
  - `watch_key_leases`: `key_lease_stream(self.node.watch(), prefix, move |key| authorized.sees(Action::Read, key.as_name()))`, with the `Authorized` cloned out of the request;
  - `rejection_status`: `UnknownKeyLease` is `NOT_FOUND`; `InvalidKeyLeaseTtl` is `INVALID_ARGUMENT`; `TooManyKeyLeases` is `RESOURCE_EXHAUSTED`; the rest `FAILED_PRECONDITION`. `KeyLeaseHeld` also sets the metadata `fleetring-retry-after-ms` to `retry_after` in decimal.
- `crates/fleetringd/src/control/absent.rs` (`NoControl`): the four methods answer `FAILED_PRECONDITION` naming the control node, as the others do.
- `crates/fleetringd/src/control/client.rs`: `acquire_key_lease`, `renew_key_lease`, `release_key_lease` (unary, retried and redirected like `renew_lease`), and `watch_key_leases(&self, request: WatchKeyLeasesRequest) -> Result<Streaming<KeyLeaseUpdate>, Status>` like `watch_membership`.
- `crates/fleetringd/src/config.rs`, `[control]`: `key_lease_max_keys: Option<u32>`, `key_lease_min_ttl_ms: Option<NonZeroU64>`, `key_lease_max_ttl_ms: Option<NonZeroU64>`, mapped into `ControlConfig::with_key_lease_limits` (validation errors name the field). Documented as applying only when a new control log is created.
- Tests:
  - Unit tests in `control/key_leases.rs`: `holders_are_composed_from_the_caller` (all three caller kinds, and refused instances: empty, 65 bytes, `a/b`, a space), `events_follow_gains_losses_and_replacements` (the three cases and key order), `the_stream_starts_with_a_filtered_snapshot`.
  - `crates/fleetringd/tests/control.rs`, with a control node whose limits are `KeyLeaseLimits::new(100, Millis::new(200), Millis::new(10_000))`:
    - `key_leases_are_acquired_renewed_released_and_watched`: a watcher opened first sees an empty snapshot; acquire as instance `w1` (ttl 500 ms) answers epoch 1 and holder `principal:<client>/w1`; acquire as `w2` is `FAILED_PRECONDITION` with `fleetring-retry-after-ms` between 1 and 501; renew succeeds; release succeeds; the watcher saw `GAINED 1` then `LOST 1`.
    - `an_unrenewed_key_lease_expires_and_the_watcher_sees_it`: acquire with ttl 300 ms and stop; within 300 ms plus two tick intervals the watcher sees `LOST`; `w2` then acquires at epoch 2 and the watcher sees `GAINED 2`.
    - `a_key_lease_survives_a_control_node_restart`: acquire with ttl 10 s, restart the control node from its log, renew at the same epoch succeeds, and `w2`'s acquire is still refused.

### Step 4: authorization (on top of FR-047)

- `crates/fleetringd/src/auth/policy.rs`, `RPCS`, after `RevokeToken`: `AcquireKeyLease`, `RenewKeyLease`, `ReleaseKeyLease` with `Requirement::Key(Action::Write)`, and `WatchKeyLeases` with `Requirement::Listing(Action::Read)`. The existing policy tests then require the `// Scope:` lines written in step 3 and four rows in the daemon README's scope table:

  ```text
  | `Control.AcquireKeyLease` | write on the lease key |
  | `Control.RenewKeyLease` | write on the lease key |
  | `Control.ReleaseKeyLease` | write on the lease key |
  | `Control.WatchKeyLeases` | read per lease key listed, or fleet |
  ```

- Tests in `crates/fleetringd/tests/scopes.rs`:
  - `holding_a_key_needs_write_on_it`: a principal with `write:tenant:acme/*` acquires, renews, and releases `tenant:acme/jobs`, and is refused `PERMISSION_DENIED` ("AcquireKeyLease needs write on 'tenant:zed/jobs'") on `tenant:zed/jobs`; a principal with only `read:tenant:acme/*` is refused on `tenant:acme/jobs`.
  - `watching_key_leases_shows_only_readable_keys`: with leases on `tenant:acme/a` and `tenant:zed/b`, a watcher with `read:tenant:acme/*` sees only the first in its snapshot and events; a principal with no scope is refused.
  - `two_principals_naming_one_instance_hold_different_leases`: `app` and `ops`, both with `write:k*`, each acquire `k1` as instance `w`: the second is refused as held by `principal:app/w`; `ops` can't release `app`'s epoch (`NotKeyLeaseHolder`).
  - `a_token_holds_as_its_principal`: a token for `app` acquires; the holder is `principal:app/w`.

### Step 5: client and command line

- `crates/fleetringd/src/data/client.rs`:

  ```rust
  /// A keyed lease the caller holds (ADR-0027). Act on it only while `is_valid()`, and pass
  /// `epoch` to every resource you change on its behalf.
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct KeyLeaseGrant { pub key: LeaseKey, pub instance: String, pub holder: String, pub epoch: LeaseEpoch, pub ttl: Duration, pub valid_until: tokio::time::Instant }
  impl KeyLeaseGrant { pub fn is_valid(&self) -> bool; }   // tokio::time::Instant::now() < valid_until

  impl Client {
      /// `valid_until` is the instant the request was sent plus `ttl - ttl / 10`.
      pub async fn acquire_key_lease(&self, key: &LeaseKey, instance: &str, ttl: Duration) -> Result<KeyLeaseGrant, KeyLeaseError>;
      pub async fn renew_key_lease(&self, grant: &KeyLeaseGrant) -> Result<KeyLeaseGrant, KeyLeaseError>;
      pub async fn release_key_lease(&self, grant: KeyLeaseGrant) -> Result<(), KeyLeaseError>;
      pub async fn watch_key_leases(&self, prefix: &str) -> Result<KeyLeaseUpdates, Status>;
  }
  #[non_exhaustive]
  pub enum KeyLeaseError {
      #[error("key '{key}' is leased to '{holder}'; retry after {retry_after:?}")]
      Held { key: LeaseKey, holder: String, retry_after: Duration },  // from FAILED_PRECONDITION with the metadata
      #[error("the key lease is gone: {status}")]
      Lost { status: Status },  // NOT_FOUND, or FAILED_PRECONDITION without the metadata on renew and release
      #[error(transparent)]
      Rpc(#[from] Status),
  }
  pub enum KeyLeaseUpdate { Snapshot(Vec<HeldKeyLease>), Event(KeyLeaseEvent) }
  pub struct HeldKeyLease { pub key: LeaseKey, pub holder: String, pub epoch: LeaseEpoch }
  pub struct KeyLeaseEvent { pub key: LeaseKey, pub holder: String, pub epoch: LeaseEpoch, pub change: LeaseChange }  // FR-042's replication::LeaseChange
  pub struct KeyLeaseUpdates { … }  impl KeyLeaseUpdates { pub async fn next(&mut self) -> Result<Option<KeyLeaseUpdate>, Status>; }
  ```

  The send instant is read with `tokio::time::Instant::now()` immediately before the RPC (ADR-0020).
- `crates/fleetringd/src/main.rs`: a `Lease(LeaseCommand)` subcommand group:
  - `lease acquire <KEY> --instance <I> --ttl <DURATION>` prints `granted <key> epoch <n> holder <h>` (or `rearmed …`);
  - `lease renew <KEY> --instance <I> --epoch <N>` prints `renewed <key> epoch <n>`;
  - `lease release <KEY> --instance <I> --epoch <N>` prints `released <key> epoch <n>`;
  - `lease watch [--prefix <P>]` prints `held <key> epoch <n> holder <h>` per snapshot entry, then `gained …` and `lost …` lines;
  - `lease hold <KEY> --instance <I> --ttl <DURATION>` acquires, waiting `retry_after` while held, prints `gained <key> epoch <n>`, renews every `ttl / 3`, prints `lost <key> epoch <n>` and exits with status 3 when a renewal is refused or the deadline passes, and on SIGINT or SIGTERM releases, prints `released …`, and exits 0.
  Durations parse with `parse_ttl` in `main.rs`, extended to accept `<n>ms` as well as `<n>s`, `<n>m`, `<n>h`, and `<n>d` (with a unit test `durations_parse_in_milliseconds_too`).
- Tests in `crates/fleetringd/tests/daemon.rs` (binary tests): `the_lease_commands_acquire_renew_release_and_watch` (a one-node fleet; `lease watch` in the background; acquire, renew, release; the watch printed `gained` and `lost` lines with the same epoch) and `lease_hold_releases_on_sigint` (Unix only: `hold` prints `gained`, gets SIGINT, prints `released`, exits 0, and the watch printed `lost`). Losing a held key to another holder is covered by the simulation (step 6), not by binary tests.

### Step 6: simulation

- `crates/fleetringd/src/sim/workload.rs`:
  - `Operation` gains `AcquireKeyLease { key: LeaseKey, instance: String, ttl: Duration }`, `RenewKeyLease { key: LeaseKey, instance: String, epoch: LeaseEpoch, ttl: Duration }` (the TTL the holder computes its deadline from), and `ReleaseKeyLease { key: LeaseKey, instance: String, epoch: LeaseEpoch }`; `Operation::database()` returns `None` for them.
  - `Ack` gains `KeyLease { holder: String, epoch: LeaseEpoch }` and `KeyLeaseReleased`.
  - New workload `KeyLeaseContender::new(keys: Vec<LeaseKey>, instance: &str, ttl: Duration, holds: u32, renewals: u32)`: picks a key with the run's `SimRng`, acquires, on `Held` waits `retry_after`, on a grant renews every `ttl / 3` until `renewals` renewals succeeded, then releases; a refused or unanswered renewal drops the lease and starts over; finishes after `holds` holds. It never acts after `invoked_at + ttl - ttl / 10` of its last successful acquire or renewal.
- `crates/fleetringd/src/sim/client.rs`: execute the three operations through the host's `ControlClient`, mapping `KeyLeaseHeld` (FAILED_PRECONDITION with the retry metadata) to a rejection the workload can read.
- `crates/fleetringd/src/sim/fleet.rs`: `FleetBuilder::key_lease_watcher(self) -> Self` adds a client host `key-leases` that follows `Control.WatchKeyLeases("")` for the whole run, reconnecting as `lease_watcher` does, and records each snapshot entry and event as a `KeyLeaseObservation { key, holder, epoch, seen: LeaseSeen }` in the `History`. `Timings` gains `key_lease_limits: KeyLeaseLimits` (default `KeyLeaseLimits::DEFAULT`) applied to the control node's configuration.
- `crates/fleetringd/src/sim/check.rs`: `Invariant::KeyLeasesExclusive` ("key leases exclusive") and `Invariant::KeyLeaseEventsMatchControl` ("key lease events match control"), and:

  ```rust
  /// For each key, the intervals in which holders believed they held it never overlap across
  /// epochs. A holder believes it holds a key from the invocation of each acknowledged acquire or
  /// renewal for `ttl - ttl / 10`, cut at the invocation of its release of that epoch.
  pub fn key_leases_exclusive(history: &History) -> Result<(), Violation>;
  /// Every `GAINED` or snapshot entry names a holder and epoch the control log granted for that
  /// key, events for one key arrive in increasing epoch order, and every acknowledged acquire's
  /// epoch appears in the control log with the same holder.
  pub fn key_lease_events_match_control(history: &History, control: &ControlLog) -> Result<(), Violation>;
  ```

  `ControlLog` gains `key_lease_holders(&self) -> &BTreeMap<(LeaseKey, LeaseEpoch), KeyLeaseHolder>`, filled while replaying; `acknowledged_control_survives` also requires every acknowledged acquire to be in it. `Fleet::finish` runs both new checks.
- `crates/fleetringd/tests/sim/key_leases.rs` (add `mod key_leases;` to `tests/sim/main.rs`), with `key_lease_limits = KeyLeaseLimits::new(64, Millis::new(1_000), Millis::new(30_000))`, a control host, one data host, the key-lease watcher, and contenders `holder-1`, `holder-2`, `holder-3` over keys `job:0` to `job:3`, ttl 2 s:
  - `key_leases_stay_exclusive_under_faults` (`seeds::FIXED`): partition `holder-1` from the control host for 5 s, crash `holder-2` for 4 s, crash and restart the control host (down 1.5 s), then let the workloads finish. Asserts, besides `finish()`: the watcher saw at least one key go `LOST` and then `GAINED` by a different holder, and every key was granted at least twice.
  - `a_control_restart_keeps_every_renewed_key_lease` (`seeds::FIXED`): ttl 10 s, one contender renewing every 1 s; crash the control host for 2 s; the contender's next renewal succeeds at the same epoch, and no other epoch was granted for its key.

### Step 7: documentation

- `crates/fleetringd/README.md`: a "Keyed leases" section (what they are for, the commands, the holder contract of ADR-0027 point 6 with a short example of passing the epoch along, the limits and their load), and the four scope rows.
- `deploy/fleetringd.toml`: the three `[control]` limits, commented, noting they apply only to a new control log.
- `docs/review.md`: keyed-lease renewals added to the per-lease command-log costs E4 addresses.
- ADR-0027's status to Accepted with the date, its row in `docs/adr/README.md`, and this ticket's Progress.

## Open risks

- **Command-log load.** At the default limits the worst case is 6,000 renewals a second (ADR-0027 point 9). The simulation and tests run far below it; a benchmark of the control node under keyed-lease load belongs with E4.
- **Advisory without epochs.** A resource that ignores the epoch gets mutual exclusion only under the bounded clock-rate assumption. Fenced writes to FleetRing databases are a possible follow-up ticket.
- **FR-047 is still in review.** Steps 3 and 4 use its `Caller`, `authorized`, `require_key`, and `RPCS`; if those names change before it merges, follow them.
- **FR-062 renamed what this plan builds on.** `require_key` is `require_database`, and `Requirement::Key` is `Requirement::Database`, whose message says "on the database". A lease key isn't a database, so keyed leases need their own requirement, such as `Requirement::LeaseKey(Action)` printing "on the lease key", which this plan's `Scope:` lines and README rows already say. `lease watch` now exists for the database leases one node holds (`--node <node-id>` or `--address`); the keyed-lease watch adds `--prefix` to the same command, and the two forms exclude each other.
- **Generator churn.** Widening the replay generator's selector from 16 to 20 changes every generated log; the tests regenerate their logs, so nothing golden breaks, but counts in `the_generated_log_exercises_every_kind_of_change` may need the log length raised.

## Progress

- 2026-11-02: ADR-0027 proposed; implementation plan written. Waiting for the ADR's acceptance and for FR-047.
