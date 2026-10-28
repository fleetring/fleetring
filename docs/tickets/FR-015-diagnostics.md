---
id: FR-015
title: Ring diagnostics and expected load shares
status: done
priority: P2
size: S
milestone: M3 Change and interop
depends_on: [FR-009]
adrs: [ADR-0003, ADR-0006]
---

# FR-015: Ring diagnostics and expected load shares

## Goal

Surface topology and balance problems, such as the small-zone skew trade-off in ADR-0006, before they hurt in production. This feeds `fleetring status` and design reviews.

## Scope

- **`Ring::diagnostics() -> Vec<Diagnostic>`.** `Diagnostic` is a `#[non_exhaustive]` enum that implements `Display` and has a `severity() -> Severity` method (`Info` or `Warning`). Variants:
  - `UnderReplicated { nodes, replication_factor }`
  - `FewerZonesThanReplicas { zones, replication_factor }`
  - `FewerHostsThanReplicas { hosts, replication_factor }`
  - `ZoneReplicaSkew { zone, weight_share_bps, replica_share_bps }`
  - `LowTokenCount { node, tokens }`, for the vnode strategy when a node has fewer than 64 tokens
- **`Ring::expected_shares(samples: NonZeroU32) -> Shares`** reports each node's primary share and replica share in basis points.
  - Rendezvous primary shares are exact: wᵢ/W.
  - All other shares are sampled from a fixed internal corpus.
- **No floats.** Shares are integer basis points, per ADR-0003.

## Acceptance criteria

- [x] With the 10/10/2 zone fixture and R = 3, `ZoneReplicaSkew` is reported for the 2-node zone, and its shares match sampling within 1 percentage point.
- [x] Healthy fixtures, such as three equal zones with R = 3, report no warnings.
- [x] Output order is deterministic: sorted by severity, then kind, then subject.

## Progress

- 2026-10-27: Done.
  - **`Ring::diagnostics()`** reports only the deepest topology shortfall: nodes, then hosts, then zones. It also reports zone skew (replica share above 1.5× the weight share and at least 2 percentage points over it) and low vnode token counts. Results are sorted by severity, with a stable order within each severity.
  - **`Ring::expected_shares()`** gives exact primary shares (weight ratios, or token arcs from `TokenTable::ownership`) and sampled replica shares. All values are integer basis points, so the library stays float-free.
- The 10/10/2 zone fixture is flagged, and an independent corpus count agrees within 1 percentage point.
