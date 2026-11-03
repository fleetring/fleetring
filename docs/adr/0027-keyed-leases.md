# ADR-0027: Keyed leases: a fenced, renewable lease on any key, requested by a named holder and kept in control state

- **Status:** Accepted (2026-11-03)
- **Date:** 2026-11-02
- **Related:** ADR-0017 (control state machine), ADR-0018 (leases, epochs, quorums), ADR-0020 (simulation), ADR-0022 (failover), ADR-0025 and ADR-0026 (principals and scopes); tickets FR-042, FR-043, FR-047

## Context

A database lease names one node that may write a database at one epoch (ADR-0018). Applications ask the same question about things that have no database: who runs `tenant:acme`'s nightly job, who owns `agent:42`'s session right now, and how does the owner learn that it no longer does? Temporal's shard ownership, Orleans' grain directory, Chubby's locks with sequencers, and etcd's leases all answer it the same way: a renewable lease with an expiry, and a number that grows with every new owner so that the resources an owner touches can refuse a stale one.

FleetRing has the pieces. The control plane is a replicated state machine with time inside its commands (ADR-0017), epochs already fence database writers, and FR-042 streams lease changes to watchers. FR-043 asks for a lease that isn't tied to opening a database, and leaves these questions to this record:

- Is a keyed lease placed by the ring, or requested by whoever wants it?
- Who is a holder, given that the worker that wants the lease is not a cluster member?
- What do acquire, renew, and release carry, what is refused, and what is the fencing token?
- How does a lease expire when no replication stream proves the holder alive, and what does a holder believe about its own lease without trusting clocks?
- What does a watcher see, and where does it watch?
- Do keyed leases share a namespace with database leases?
- What are the limits, given that every renewal is a logged control command?
- Which scope does each new RPC need under ADR-0025 and the requirement table of ADR-0026?

Four constraints are fixed. `apply` stays pure: no clock, no randomness, no principals (ADR-0017, ADR-0025 point 5). Safety never depends on clocks (ADR-0018 point 6): clocks may decide when something happens, never whether two owners can act at once on a resource that checks epochs. The control plane must move to Raft without changing the logic (ADR-0017). Every RPC has exactly one requirement in `fleetringd::auth::RPCS` (ADR-0026 point 1).

## Decision

1. **A keyed lease is requested, not placed.** Whoever asks first while a key is free gets its lease. The control plane doesn't consult the ring for keyed leases and never moves one. A holder is a worker process that the control plane can't start, stop, or move, so placing the lease would only mean refusing everyone but the ring's choice, which a worker can do for itself: a sidecar that wants ring placement asks for the key only when the ring the node publishes names its node first for that key (`Ring::primary`). Placement stays a function of the key's name, as FR-043 requires, and the lease stays a function of who asked.

2. **A holder is `<identity>/<instance>`, composed at the request boundary.** The caller names an *instance*: 1 to 64 bytes of ASCII letters, digits, `.`, `_`, and `-`, which tells apart several workers of one principal. The service prefixes the identity the request boundary resolved (ADR-0026): `node:<id>` for a node certificate, `principal:<name>` for a principal (certificate or token, after forwarding), and `anonymous` on a plaintext connection. So `principal:billing/worker-3` holds a lease, and another principal can't present that holder whatever instance it names. The control state stores the composed holder as an opaque string of 1 to 255 bytes and only compares it; it never learns what a principal is (ADR-0025 point 5).

3. **Keys use database-name syntax in a namespace of their own.** A lease key is 1 to 255 bytes of UTF-8 without control characters, like a database name, and scopes match it with the same prefix matcher. Keyed leases live in their own table in control state: the keyed lease on `tenant:acme` and the database lease of `tenant:acme` are unrelated records, and neither command family can create, renew, or end the other's. The same prefix rule still governs both, so `write:tenant:acme/*` lets a principal write the tenant's databases and lease the tenant's keys.

4. **Epochs come from one counter for all keyed leases, and the epoch is the fencing token.** Every grant takes the next value of `State::key_lease_epoch`, a counter that only grows, so epochs strictly increase along each key and are never reused, even after a record is released and forgotten. The holder must hand its epoch to every resource it changes on the lease's behalf, and such a resource must refuse an epoch lower than the highest it has seen, as followers refuse old primaries (ADR-0018 point 2). FleetRing can't fence resources it doesn't own; it guarantees the numbers.

5. **Three commands, with these fields and rules.** Each runs at its stamp, as every command does (`now = max(stamp, State::now)`), and a rejection changes nothing.

   | Command | Fields | Accepted when | Effect |
   |---|---|---|---|
   | `AcquireKeyLease` | `key: LeaseKey`, `holder: KeyLeaseHolder`, `ttl: Millis` | `ttl` is within the limits (point 9), and the key has no record, or its record has expired, or its record names this holder | No live record: forget every record that has expired at `now`, then grant the next epoch with `expires_at = now + ttl`. A live record of this holder: keep its epoch, set its `ttl` to the new one, and set `expires_at = now + ttl` (a retried acquire after a lost reply). |
   | `RenewKeyLease` | `key`, `holder`, `epoch: LeaseEpoch` | the record exists, names `epoch` and `holder`, and hasn't expired | `expires_at = now + ttl`, with the `ttl` recorded at acquire |
   | `ReleaseKeyLease` | `key`, `holder`, `epoch` | the record exists and names `epoch` and `holder`, expired or not | the record is removed |

   Rejections are checked in this order, and each is its own `Rejection` variant: a `ttl` outside the limits (`InvalidKeyLeaseTtl`); a live record of another holder (`KeyLeaseHeld`, which carries the holder, its epoch, and `retry_after`, the span until it can expire); no room for a new key (`TooManyKeyLeases`); an exhausted counter (`KeyLeaseEpochExhausted`); for renew and release, no record (`UnknownKeyLease`), then another epoch (`StaleKeyLeaseEpoch`, with the current one), then another holder (`NotKeyLeaseHolder`); for renew only, an expired record (`KeyLeaseExpired`). A stale holder's renew and release are therefore always refused, and a renewal never brings an expired lease back: the holder acquires again and gets a new epoch.

6. **Expiry is judged by command time; a holder's belief is judged by its own clock, conservatively.** A record has expired once `now > expires_at`. The control node's failover timer submits a `Tick` whenever no command arrived for a tick interval (250 ms by default, ADR-0022 point 1), so expiry is noticed without traffic. A holder computes its own deadline when it sends a request, before the control node can stamp it: `valid_until = sent + ttl - ttl / 10`, measured on its monotonic clock, and stops acting on the lease at that instant unless a renewal sent earlier has succeeded since. The control node stamps the command at or after `sent`, so it grants the key to anyone else only after `sent + ttl`. The two never overlap while the holder's clock runs no more than a tenth slower than the control node's over one TTL: a bound on clock *rate*, never on synchronized clocks. If even that fails (a process paused past its deadline, a clock jump), the epoch still fences: the next holder's epoch is higher, and every resource that checks epochs refuses the old one. That is the whole safety argument, and it is ADR-0018's.

7. **No acknowledged grant, renewal, or release is lost, and no unexpired lease gets a second holder.** The control node answers a keyed-lease command only after the command is durable in its log, as for every command (ADR-0017 point 3). Under Raft the answer waits for a majority of control replicas, and a new leader is elected by a majority that must contain one of them: the commit quorum W and the election quorum Q intersect because W + Q > R, the same intersection ADR-0018 relies on for data. A restarted or newly elected control node therefore replays every acknowledged command before it applies a new one, and its state names the same holder and epoch for every key. No grace period after a restart is needed, unlike database leases (ADR-0022 point 6): a database primary keeps serving while the control plane is away because fencing protects its writes, whereas a key holder that can't renew stops at its own deadline, so a key that looks expired after a long outage really is free. A restart shorter than the remaining TTL loses nothing; the holder's next renewal succeeds at the same epoch.

8. **Watchers follow the control state, through `Control.WatchKeyLeases`.** The stream starts with every unexpired keyed lease whose key starts with the request's prefix (empty for all), with holder and epoch, then sends one event per change: `GAINED` when a key gets a holder at an epoch, `LOST` when that epoch ends, by release, by expiry at the first command or tick after `expires_at`, or by a new grant. It is derived by comparing successive published states, as `WatchMembership` is, so a watcher that falls behind sees the net change, and events for one key arrive in epoch order. A holder learns that it lost a lease from a refused renewal, from a `LOST` event, or from its own deadline passing, whichever comes first; the deadline alone is authoritative. FR-042's `Database.WatchLeases` stays what it is, one node's view of the database leases it serves, because a keyed lease has no node behind it.

9. **Limits are part of the control configuration, fixed when a log is created.** `ControlConfig` gains `KeyLeaseLimits`: at most 10,000 unexpired keyed leases, and TTLs from 5 s to 300 s. Like the other timings, they travel in the genesis configuration and in snapshots, so a log replays under the limits it was written with, and a log written before keyed leases existed loads with the defaults. Expired records are kept until the next accepted acquire, which forgets them, so the table never holds more than the limit plus the expired leftovers of one acquire. The limit exists because every renewal is a logged command: with holders renewing every third of their TTL, the worst case is `max_keys × 3 / min_ttl`, 6,000 commands per second at the defaults, beside one database renewal per primary every 2 s. Operators of busy clusters lower the limit when they create the control log; moving renewals off the command log is the scale work of roadmap epic E4, for both kinds of lease.

10. **Scopes: `write` on the key to hold, `read` to watch.** In the requirement table of ADR-0026:

    | RPC | Requirement |
    |---|---|
    | `Control.AcquireKeyLease` | write on the key |
    | `Control.RenewKeyLease` | write on the key |
    | `Control.ReleaseKeyLease` | write on the key |
    | `Control.WatchKeyLeases` | read per key listed, or cluster |

    Holding a key's lease is acting on the key, which is what `write` means; the principal that writes `tenant:acme`'s database is the worker that should own `tenant:acme`'s jobs. `admin` would also grant creating, dropping, and draining, which no worker needs. The watch is a listing: it shows only the keys the caller may read, and refuses a caller who may read none, as `WatchLeases` does. Nodes pass every check, as always.

11. **The surface is small.** Three unary RPCs and one server stream on the `Control` service, which the control node serves and which `ControlClient` already reaches through leader redirects; methods on `fleetringd::Client`; and `fleetringd lease acquire|renew|release|watch|hold` on the command line. Nothing else: no values stored under keys, no locks over groups of keys, no fairness or queueing among waiters. Waiters retry after `retry_after`.

## Alternatives considered

- **Ring-placed keyed leases**, granted only to the ring's preferred live node for the key. Deterministic, and it ties worker placement to data placement. But the holder is a worker, not a node, and the control plane can't move a worker when the ring changes; it could only refuse it, leaving the key unowned until the right worker turns up. Workers that want ring placement get it by checking the ring before they ask.
- **The node holds the lease for its co-located worker**, renewing it with its database leases. One renewal stream per node instead of per key, but a worker that crashes while its node lives would keep the lease forever. The holder has to be the process that does the work, so the holder renews.
- **One epoch counter per key, kept forever.** Epochs per key would start at 1 like database epochs, but the record of every key ever leased would have to stay in control state so that a released key never reuses an epoch. A cluster-wide counter lets released and expired records go.
- **A shared namespace with database leases**, where acquiring `tenant:acme` would take or wait for the database's lease. It would let a worker's acquire interfere with replication's failover, which runs on quorums and fencing that a worker can't take part in. Workers that want to follow a database's primary already can, with `WatchLeases` (FR-042).
- **Relaying through the data node's `Database` service, and extending its `WatchLeases`.** FR-043 sketched it that way. A relay puts a second process in every renewal's path, so a lease could expire because an unrelated node was slow, and the per-node `WatchLeases` stream has no node to attribute a keyed lease to. The control service is where the state lives, and the client already finds its leader.
- **etcd-style leases as objects that keys attach to.** More general, and more surface: lease IDs, attach and detach, and keep-alive streams. FR-043 asks for acquire, renew, release, and watch on a key, and one lease per key covers every case it names.
- **Keeping keyed leases off the command log**, in the control node's memory. Renewals would cost nothing, but a control restart or a Raft leader change would lose them and could grant a second holder before the first one's deadline. That trade belongs with node-level leases in E4, designed for both kinds of lease at once.
- **A grace period after a control restart**, as ADR-0022 point 6 gives database leases. Explained in point 7: a key holder stops at its own deadline, so a grace would keep keys unowned for no safety gain.
- **`admin` on the key, or a fifth action such as `lease`.** `admin` over-grants (point 10). A new action would change ADR-0025's scope algebra for a case that `write` already describes.

## Consequences

- `fleetring-control` gains a lease-key type, a holder type, three actions, their outcomes and rejections, a table and a counter in `State`, and limits in `ControlConfig`, all through serde defaults, so existing logs and snapshots load and replay unchanged. The replay test and its generator cover the new commands.
- The control service gains four RPCs and a stream like `WatchMembership`; the proto change is additive. FR-043 depends on FR-047, whose request boundary resolves the identity a holder is composed from and whose table gets four rows.
- Applications get Chubby's contract, not a stronger one: the lease says who should act and the epoch lets resources refuse whoever shouldn't. A resource that ignores epochs gets mutual exclusion only as good as the bounded clock-rate assumption. The daemon README says so next to the commands.
- Every renewal is a control command, so keyed leases share the command log's throughput with database renewals and heartbeats. `docs/review.md` lists it with the other per-lease costs that E4 addresses.
- A later ticket can make FleetRing databases fence on keyed-lease epochs (a write that carries `{key, epoch}` and is refused below the highest seen), which would turn the advisory contract into an enforced one for writes inside the cluster.
