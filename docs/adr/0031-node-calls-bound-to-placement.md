# ADR-0031: Node-to-node calls are bound to placement: a node acts only on databases placed on it, at epochs control state gives it

- **Status:** Accepted (2026-11-03)
- **Date:** 2026-11-02
- **Related:** ADR-0017 (control state machine), ADR-0018 (epochs), ADR-0022 (failover), ADR-0025 (principals and scopes), ADR-0026 (scope enforcement, proposed with FR-047); tickets FR-046, FR-047, FR-050

## Context

FR-046 made node identity unforgeable: a node's certificate names its ID, and the membership calls refuse a request that names another node. FR-047 narrows node certificates to node calls. But a node call still accepts any node for any database. A stolen node key can open a `Replicate` stream to any replica at a higher epoch, which the replica records durably, fencing the real primary out, and then have it apply forged batches; fence any replica for a promotion that doesn't exist; read any database whole through `Snapshot` or `Fetch`; and report itself as a candidate. Epochs contain much of this, since a forged epoch is above nothing the control plane granted, but no code verifies that the node presenting an epoch is the one control state gave it to.

Kubernetes' NodeRestriction admission plugin is the model: a kubelet's credential may only modify its own Node object and the pods bound to it. For FleetRing the equivalent is placement: a stolen node key should reach only the databases placed on that node, and only in the role control state gives it there.

Three constraints: `apply` stays pure and unaware of who called (ADR-0025 point 5); replicas keep accepting their primary while the control plane is down (ADR-0018 point 6); and placement is in flux during membership changes, and later during live migrations (roadmap E6), when an old and a new replica set are both legitimate for a while.

## Decision

1. **Every node call names a binding, and the receiver checks it before touching state.** A binding says which role the calling node must have for the database the request names. The requirement table of ADR-0026 gains a binding for every `node` row, and a test refuses a node RPC without one:

   | RPC | Caller | Binding | Checked by | Against |
   |---|---|---|---|---|
   | `Control.Join`, `Heartbeat`, `Leave`, `Drain` | the node | **self**: the request names the caller | control service | certificate (FR-046) |
   | `Control.AcquireLease` | the node | **self**, and a live, undraining replica-set member | control service, then `apply` | certificate, then control state (existing `NotInReplicaSet`) |
   | `Control.RenewLease` | the node | **self**, and the holder at the epoch | control service, then `apply` | certificate, then control state (existing `NotLeaseHolder`) |
   | `Control.StartPromotion` | a node | **replica**: in the database's replica set | control service | control state |
   | `Control.RecordFenced`, `CompletePromotion` | the candidate | **candidate** at the request's epoch | control service | control state |
   | `Control.TransferLease` (from a node) | the holder | **holder** at the current epoch | control service | control state |
   | `Replication.Replicate` | the primary | **holder or candidate** at the stream's epoch, and the receiver is a replica | receiving follower | its epoch record, else control state |
   | `Replication.Fence` | the candidate | the request's `candidate` is the caller, and **candidate** at the epoch | receiving replica | control state |
   | `Replication.Fetch` | the candidate | **candidate** the replica was fenced for at the epoch | receiving replica | its epoch record |
   | `Replication.Snapshot`, `Repair` (FR-045) | a follower | **follower**: one the primary streams the database to, at the epoch it serves | primary | its own follower set |
   | `Replication.Promote` | the control node | the receiver is the **candidate** at the epoch | receiving node | control state |

2. **The control node checks against its state before it submits; `apply` doesn't change.** A promotion's candidate, a lease's holder at an epoch, and a database's replica set are all in control state. The service reads the published state and refuses before logging anything. For the rows that name an epoch (candidate, holder), the answer for that epoch never changes once given (an epoch has one candidate and one holder), so a check made against one state stays true when the command applies later, under Raft too. `AcquireLease` and `RenewLease` are already bound: FR-046 checks the requesting node against its certificate, and `apply` already requires a replica-set member or the holder. No command gains a caller field, so logs replay unchanged.

3. **A data node learns who may lead an epoch from control state, once, and then trusts its durable record.** A replica already records durably the highest epoch it has seen and the node it named as holder (ADR-0022 point 4). Before it records a *new* holder for an epoch, from a stream or a fence, it asks the control plane who leads the database now: `Locate`, whose `Placement` gains the candidate of a promotion in progress. It accepts the caller only if control state names it the holder or the candidate at exactly that epoch. Once recorded, the pair is trusted without asking again, so a follower that restarts while the control plane is down still accepts its primary. A caller the record doesn't know, while the control plane can't be reached, is answered `UNAVAILABLE` and retries; becoming a new primary needs the control plane anyway.

4. **Placement in flux: both rings count for one membership epoch, and epochs decide the rest.** Holder and candidate checks are per epoch and don't depend on the ring. The ring-based checks (a follower that a stream reaches must be a replica; a node that asks a primary for a snapshot must be one of its followers) accept a node in the replica set under the receiver's current ring or the ring before it, so a membership change, where nodes publish the new ring at different moments, never refuses a legitimate stream or snapshot. A database's replica-set membership is decided in one function, `replicas_of`, which live migration (E6) extends with its target replica set until its cutover epoch, after which only the new set counts.

5. **Refusals are `PERMISSION_DENIED`, name the binding, and are audited.** The message is `<Method> refused for node '<node>': <reason>`, where the reason is one of `the request names node '<other>'`, `not a replica of database '<db>'`, `not the holder of database '<db>' at epoch <e>`, `not the candidate of database '<db>' at epoch <e>`, or `not a follower of database '<db>' on this node`. The response carries `fleetring-binding` metadata (`self`, `replica`, `holder`, `candidate`, `follower`) so that tests and tooling don't parse messages. Each refusal is logged once on the `fleetringd::audit` target with the node, method, database, epoch, and binding, as FR-047 logs scope refusals. A check that can't be made because control state is unreachable is `UNAVAILABLE`, never `PERMISSION_DENIED`.

6. **What a stolen node key can still do.** Act as that node: serve, follow, and lead the databases placed on it, at the epochs control state gives it; report its own positions in heartbeats (reports from outside a replica set are recorded but never make a node a candidate and never show in `Locate`); and join or drain itself. It can't stream, fence, fetch, snapshot, or promote any other database. Revoking a node is removing it from the cluster and refusing its certificate (FR-046's authority rotation), as today.

## Alternatives considered

- **Lease certificates signed by the control node,** carried in every stream and fence and verified offline. No control round trip at all, but it needs a signing key on the control node, its distribution to every node, and a rotation story, to save one `Locate` per new epoch per replica.
- **Checking caller roles inside `apply`,** with a caller field on every node command. It would put identities into the replicated log, against ADR-0025 point 5, and change what old logs replay to, for a check the service can make against the same state (point 2).
- **Relying on epochs alone.** Epochs stop a deposed primary; they don't stop a node that was never a holder from presenting the current or a higher epoch first.
- **Refusing everything during membership changes until all nodes publish the same ring.** Simple, and it would stall replication on every join and leave.

## Consequences

- The control service gains four state checks before submitting; replication gains an epoch-authority check before recording a new holder, one `Locate` per new epoch per replica, and a ring check for streams and snapshots. `Placement` gains one optional field. The control crate and the logs don't change.
- `fleetringd::auth::RPCS` rows for node calls gain a binding, stated in the proto and the daemon README like the scopes, and enforced by the same kind of table test.
- `SECURITY.md`'s threat model (FR-048) can state the blast radius of a stolen node key as the databases placed on that node.
