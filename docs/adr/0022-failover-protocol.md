# ADR-0022: Failover: the candidate drives the promotion, and logs are ranked by log epoch

- **Status:** Accepted (2026-10-28)
- **Date:** 2026-10-28
- **Amends:** ADR-0018, points 4, 5, and 7
- **Related:** ADR-0016, ADR-0017, ADR-0019, ADR-0020; tickets FR-029, FR-031

## Context

ADR-0018 fixes the safety rules of a failover: epochs fence, W + Q > R, the new primary adopts the most advanced log among Q fenced members, and divergent tails are discarded. Building it (FR-031) left four questions open:

- **Who drives a promotion?** The control plane issues epochs, but it holds no logs, and data nodes hold no control state.
- **What is "the most advanced log"?** Positions are compared as bytes (ADR-0016), which orders positions within one primary's log. Two logs written by different primaries can share a position and differ in content: after two failovers, a replica that missed the middle epoch can hold an older primary's unacknowledged frames at a higher position than another replica's acknowledged ones.
- **Truncate back to where?** ADR-0018 point 5 names the adopted position, which only the new primary knows.
- **What does a graceful shutdown do,** and how does liveness move on without traffic (FR-029)?

## Decision

1. **The control node starts promotions and tells the candidate; the candidate does the rest.** A timer on the control node submits a `Tick` command when no command has arrived for a tick interval, so that liveness and expiry are judged without traffic. It submits `StartPromotion` for every database whose promotion the state machine would accept (`State::promotions_due`), then calls `Replication.Promote` on each promotion's candidate, again every second until the promotion completes or is replaced. The candidate:
   1. fences its own replica, then the other members in parallel with `Replication.Fence(database, epoch, candidate)`, and records each with `RecordFenced`, until Q are fenced;
   2. adopts the most advanced log among them (point 2 below), fetching the frames its own copy lacks from that member with `Replication.Fetch`;
   3. calls `CompletePromotion`, records the adopted position as where its epoch's log begins, and serves.

   A fenced replica records the epoch and the candidate durably, steps down if it served the database at an older epoch, and reports its position, all while no stream applies to its copy.
2. **Logs are ranked by log epoch, then position.** Every replica records durably the epoch of the primary that wrote its log's last frames: a follower when the first batch of a stream applies, before it acknowledges it; a primary when it starts to serve; a follower that installs a copy from its primary at once. Fences and heartbeats report it (`DatabaseStatus.epoch`), the state machine ranks promotion candidates by it before their positions, and candidates adopt by it. This is Raft's "last log term, then index" rule, and it is what makes "most advanced" well defined across failovers.
3. **Each epoch's log begins at the adopted position.** The new primary sends it with every stream (`ReplicateOpen.epoch_start`). A follower whose log came from an earlier epoch and runs past it truncates back to it before its first acknowledgement. A copy that can't be truncated there, because the position is in another log generation, tells the primary it needs a snapshot (FR-032). (*Amended 2026-10-28, with FR-032:* the follower says so in its first acknowledgement, `ReplicateAck.needs_snapshot`, and takes no frames until the primary answers with `Reseed`, the same re-seed a follower behind the last checkpoint gets. A copy can't be truncated to or behind its last checkpoint, which it records durably as the primary does. An installed snapshot records the primary's epoch as its log epoch, so a re-seeded copy is never truncated again.)
4. **A follower accepts a stream only from its epoch's holder:** the candidate a fence named, or the first primary that streamed at that epoch.
5. **Leases are renewed by the data node that holds them,** at a renewal interval well under the lease duration. A refusal that names a newer epoch or another holder demotes it: it stops serving, closes the database, and follows the new primary, whose stream truncates its unacknowledged tail. A control plane it can't reach changes nothing.
6. **A control node that has just started waits one lease duration plus the failover margin before it starts any promotion,** so that leases which look expired only because the control node was down get renewed first.
7. **A graceful shutdown leaves the cluster** (`Control.Leave`) unless `[membership] on_shutdown = "stay"`. (*Amended 2026-10-29, with FR-038:* it drains first. A node that is about to leave, or that an operator drains with `fleetringd node drain`, is marked as draining (`Control.Drain`): it gets no new lease and is never a candidate. It then hands each lease it holds to a follower with `Control.TransferLease`, which starts the promotion of point 1 at the next epoch without waiting for the lease to expire, with a chosen candidate: the follower the holder names, which is the one that has acknowledged the most and to which the holder has shipped its tail, or else the most advanced live follower by point 2. The candidate fences, adopts, and completes exactly as after an expiry, so a transfer is as safe as a failover; the holder refuses writes before it asks, steps down once the epoch is issued, and leaves only once its databases have a primary again. A lease that can't be handed over expires and fails over as before.)

## Alternatives considered

- **The control node drives the whole promotion,** fencing replicas and copying frames itself. It would need a data path it otherwise never has, and the candidate would still have to catch up.
- **Candidates poll for promotions in heartbeat replies.** No new RPC, but a heartbeat interval on every failover's critical path.
- **Comparing positions alone.** Correct with one failover, but a replica that missed an epoch can win with an older primary's unacknowledged tail and lose acknowledged writes.
- **Truncating to the old primary's last acknowledged position.** Only the old primary knows it, only while it runs, and a follower's divergent tail needs the same treatment.

## Consequences

- The state machine stays pure and replay-safe: `Tick` and the log epoch in reports are new, and logs without them load and replay to the same states.
- A divergence below an epoch's start, which takes two failovers with a replica down for the middle one, is detected by the WAL's checksums rather than repaired; such a replica needs a snapshot (FR-032). (*Amended 2026-10-28, with FR-032:* such a replica is re-seeded. When the primary's frames don't continue its copy, the follower takes them no further and sets `ReplicateAck.needs_snapshot`, and the primary answers with `Reseed`, as in point 3.)
- A promotion whose candidate can't catch up stalls, and the next one, a lease duration plus the margin later, picks the most advanced candidate from fresher reports.
- With the daemon's defaults (10 s leases renewed every 2 s, a 2 s margin, a 250 ms tick), writes move to a new primary within about 12 s of a primary's last renewal. A planned departure moves them in the time the promotion itself takes, a few round trips per database, since the candidate already holds the log.
