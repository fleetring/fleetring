# FleetRing placement spec, version 1

- **Status:** Final, as of `fleetring` 0.1.0 (FR-019). Its outputs never change: a change to which nodes a key maps to needs a new spec version.
- **Normative:** yes. The key words MUST, MUST NOT, SHOULD, and MAY are used as in RFC 2119.
- **Conformance:** an implementation conforms if it reproduces every case in `crates/fleetring/tests/vectors/placement-v1.json` bit for bit. The Rust crate `fleetring` is the reference implementation. The vector file's `layout` field describes its format, and `scripts/placement_ref.py` is an independent Python implementation of this spec that passes every vector.
- **Decisions behind this spec:** ADR-0002 (hashing), ADR-0003 (integer arithmetic), ADR-0004 (strategies), ADR-0006 (replica selection), ADR-0007 (bounded loads), ADR-0008 (versioning and fingerprints).

Placement is a pure function. Its inputs are a **ring** (nodes, a strategy, a seed, a replication factor, and a hasher) and a **key**: the raw bytes of whatever must live somewhere, such as a database name, room ID, tenant ID, or cache key. Its output is an ordered list of nodes. Two implementations of this spec MUST return the same list for the same inputs, on every platform.

## 1. Conventions

- All integers are unsigned.
- Multi-byte integers inside hash inputs and encodings are **little-endian**: `le32(n)` is 4 bytes and `le64(n)` is 8 bytes.
- `a ‖ b` is byte concatenation.
- Arithmetic MUST NOT overflow. Products that would exceed 64 bits are computed in 128 bits. Floating point MUST NOT be used anywhere.
- Keys are opaque byte strings. They are hashed exactly as given: no Unicode normalization, case folding, or trimming.

## 2. Ring inputs

| Input | Domain | Default |
|---|---|---|
| Nodes | 1 to 2³² − 1 nodes with distinct IDs | — |
| Node ID | 1–64 bytes of `[a-z0-9._:-]` | — |
| Weight | integer, 1 to 2³² − 1 | 1 |
| Zone | absent (the default zone) or a label (same grammar as IDs) | absent |
| Host | absent (the node is its own host) or a label | absent |
| Strategy | `rendezvous`, or `vnodes(tokens_per_weight)` with `tokens_per_weight` ≥ 1 | `rendezvous` |
| Seed | `u64` | 0 |
| Replication factor *R* | 1–255 | 3 |
| Hasher | `xxh3-64` (§3) | `xxh3-64` |
| Epoch | `u64`; never affects placement | 0 |

A ring MUST be rejected if any of these hold:
- it has no nodes;
- two nodes share an ID;
- a host label appears in more than one zone (absent counts as one zone);
- with the vnodes strategy, it would hold more than 2²² tokens in total (§6).

**Canonical order.** Nodes are sorted by ID, comparing bytes. A node's **index** *i* is its position in this order. Placement MUST NOT depend on the order in which nodes were supplied.

## 3. Hashing

`H(bytes, seed)` is `XXH3_64bits_withSeed(bytes, len, seed)` from xxHash 0.8 or later. The XXH3 output format has been frozen since xxHash 0.8.0.

All hashes in a ring use the ring's seed *s*:
- **Node hash:** `nh[i] = H(id[i], s)`, where `id[i]` is the UTF-8 bytes of node *i*'s ID.
- **Key hash:** `kh = H(key, s)`.

## 4. Fixed-point logarithm

`L(x)` approximates `−log₂((x + 1) / 2⁶⁴)` in Q32 fixed point (32 fractional bits) for a 64-bit `x`.

**Table.** For *j* = 0…1024, `T[j] = round(2³² · log₂(1 + j/1024))`. Every entry is at least 7.4 × 10⁻⁴ units (about 2⁻¹⁰·⁴) from a rounding boundary, so the rounding mode and the precision of the computation don't matter. The closest case is j = 684. `T[0] = 0` and `T[1024] = 2³²`. The vectors file lists all 1025 entries.

**Algorithm.**

```text
L(x):
  if x = 2⁶⁴ − 1: return 0
  v ← x + 1                          # 1 ≤ v < 2⁶⁴
  e ← ⌊log₂ v⌋                       # 0 … 63
  m ← v · 2^(63 − e)                 # 64-bit mantissa, top bit set
  j ← (m >> 53) & 1023               # next 10 bits after the leading 1
  r ← m & (2⁵³ − 1)                  # remaining 53 bits
  f ← T[j] + ⌊(T[j+1] − T[j]) · r / 2⁵³⌋   # 128-bit product
  return (64 − e) · 2³² − f
```

`L` is non-increasing in `x`. It's exact when `x + 1` is a power of two, and within 740 units (about 1.7 × 10⁻⁷) of the true value otherwise.

## 5. Rendezvous strategy

For a key with hash `kh`, every node *i* gets:
- a **draw** `x[i] = H(le64(nh[i]) ‖ le64(kh), s)`, a 16-byte input;
- a **logarithm** `l[i] = L(x[i])`.

**Order.** Node *a* precedes node *b* when the first of these comparisons that differs says so:

1. `l[a] · w[b] < l[b] · w[a]`, compared as 128-bit integers;
2. `x[a] > x[b]`;
3. `a < b` (index order, which is ID order).

This is a total order. When `w[a] = w[b]`, step 1 is equivalent to step 2, so implementations MAY skip computing `L` for equal weights. The order of all nodes for a key is its **candidate order**.

## 6. Vnode strategy

With `tokens_per_weight` = *t*, node *i* owns `c[i] = w[i] · t` tokens. The ring MUST be rejected if `Σ c[i] > 2²²`; compute the sum in 64 bits, saturating.

- **Token positions.** Token *k* of node *i*, for 0 ≤ *k* < `c[i]`, sits at `H(le64(nh[i]) ‖ le32(k), s)`, a 12-byte input.
- **Sorting.** All `(position, i)` pairs are sorted by position, then by node index.

**Candidate order** for a key with hash `kh`:
1. Start at the first pair whose position is ≥ `kh`. If none is, start at the first pair.
2. Walk the pairs in order once around the circle, wrapping from the last pair to the first.
3. Take each node the first time it appears.

## 7. Replica selection

A ring's nodes fall into **zones** and **hosts**:
- Two nodes are in the same zone exactly when both lack a zone or both have the same zone label.
- Two nodes are on the same host exactly when both have the same host label. A node without a host label is on a host of its own.

**Preference list.** Given a candidate order restricted to the eligible nodes (§8), the preference list is built in three passes over that order:

1. **Zones.** Take each node whose zone isn't yet taken. Mark its zone and its host as taken.
2. **Hosts.** Take each remaining node whose host isn't yet taken. Mark its host as taken.
3. **Everything else.** Take every remaining node.

The concatenation of the three passes is the preference list. The **replica set** at replication factor *R* is the first `min(R, length)` entries of the list. Its first entry is the **preferred primary**.

Consequences that conforming implementations MUST preserve, because they follow from the above:
- the replica set for *R* is a prefix of the replica set for *R* + 1;
- with *Z* eligible zones, the first `min(R, Z)` replicas are in distinct zones;
- adding, removing, or reweighting one node changes at most one member of any replica set.

## 8. Filters

A **filter** is a predicate over nodes, such as "the node is up". Filtering removes ineligible nodes from the candidate order **before** the passes of §7. A filtered preference list equals the preference list of the ring rebuilt without the ineligible nodes.

## 9. Bounded loads

Inputs:
- a load `load[i]` for every node: an integer in any unit, 0 to 2⁶⁴ − 1, with absent nodes counting as 0;
- a **headroom** ε: an integer percentage, 0 to 65535;
- an item load `item`, 0 to 2⁶⁴ − 1;
- the number of replicas to place, *r* = min(*R*, *N*), where *N* is the number of nodes in the ring, eligible or not.

Let `total = Σ load[i]` and `W = Σ w[i]`. Node *i*'s capacity is:

```text
q = total + r · item
f = (100 + ε) · w[i]
d = 100 · W
capacity[i] = ⌊q / d⌋ · f + ⌈(q mod d) · f / d⌉        # equals ⌈(100 + ε) · q · w[i] / (100 · W)⌉
```

Every intermediate value fits in 128 bits.

**Selection.**
1. Node *i* is eligible when `load[i] + item ≤ capacity[i]`.
2. Select the replica set using that filter (§8).
3. If it has fewer than *r* members, append nodes from the unfiltered preference list, skipping nodes already chosen, until it has *r* members. Such a placement is **saturated**: exactly the placements with a replica on a node that isn't eligible. A replica set that has *r* members after step 2 is not saturated, even when *r* < *R*.

A placement is **home** when it equals the key's unfiltered replica set (§7): the same nodes in the same order. A saturated placement can still be home.

## 10. Canonical ring encoding

Used by the fingerprint (§11). Little-endian throughout.

```text
le32(1)                                        # format version
u8 strategy                                    # 0 = rendezvous, 1 = vnodes
[le32(tokens_per_weight)]                      # present only for vnodes
le64(seed)
u8 replication_factor
le32(len(hasher_name)) ‖ hasher_name           # UTF-8, e.g. "xxh3-64"
le32(node_count)
for each node in canonical order:
  le32(len(id)) ‖ id
  le32(weight)
  u8 zone_present ‖ [le32(len(zone)) ‖ zone]   # 0 = default zone, 1 = present
  u8 host_present ‖ [le32(len(host)) ‖ host]   # 0 = own host, 1 = present
```

The epoch is not encoded: it never affects placement.

## 11. Fingerprint

```text
fingerprint = XXH3_64bits_withSeed(encoding ‖ probes, 0)
probes      = for i in 0..64:
                key = "fleetring-probe-" ‖ two-digit decimal i      # "fleetring-probe-00" … "-63"
                set = replica set of key at the ring's replication factor
                le32(len(set)) ‖ le32(index of each member, in order)
```

The fingerprint always uses XXH3 with seed 0, whatever the ring's hasher and seed. Because it covers the probe placements, a different hasher, seed, or algorithm changes it, and so does a bug in a port. Peers exchange `(epoch, fingerprint)`; two peers with the same epoch and different fingerprints MUST treat it as an error.

## 12. Versioning

- This is placement spec version 1. Any change to the output of §3–§9 or §11 for the same inputs requires a new version, whatever the crate version.
- The vectors file is append-only. New cases MAY be added, but existing cases MUST NOT change.
- Ring specs record the format version (§10). A reader MUST reject versions it doesn't implement.

## 13. Composition (cells)

A **cells layer** places a key in two steps: its **selector** picks one of its **cells**, and that cell places the key. A cell is a ring (§2) or another cells layer, so layers nest, for example a layer of regions whose cells are layers of cells whose cells are rings. All of a key's replicas come from one ring, the key's **leaf ring**, which spreads them across its own zones and hosts (§7). Layers partition keys; they never spread one key's replicas across cells.

**Layer inputs.**

| Input | Domain | Default |
|---|---|---|
| Cells | 1 to 2³² − 1 cells with distinct IDs, each a ring or a cells layer | — |
| Cell ID | 1–64 bytes of `[a-z0-9._:-]`, the grammar of node IDs (§2) | — |
| Exact rules | byte strings, each mapping to a cell ID; a byte string has at most one exact rule | none |
| Prefix rules | byte strings, each mapping to a cell ID; a byte string has at most one prefix rule | none |
| Selector ring | absent, or a ring (§2) whose node IDs are cell IDs | absent |

The exact and prefix rules form the layer's **directory**. The prefix rule for the empty byte string, if there is one, is the **catch-all**. The selector ring's node weights are its cells' shares of the keys it selects for. They are explicit inputs, never derived from the cells' contents, so a change inside a cell never changes which cell any key maps to.

**Canonical order.** Cells are sorted by ID, comparing bytes. The rules of each kind are sorted by their byte strings, comparing bytes, so a byte string sorts before every longer string it is a prefix of.

A layer MUST be rejected if any of these hold:
- it has no cells;
- two cells share an ID;
- it has no selector ring and no catch-all, so some keys would reach no cell;
- it has a selector ring and a catch-all, so the selector ring would never be used;
- a rule maps to a cell ID that is not a cell of the layer;
- a node of the selector ring has an ID that is not a cell of the layer;
- a cell is the target of no rule and no node of the selector ring;
- it has a selector ring, and some ring inside one of its cells has the selector ring's seed. The rings inside a cell are the cell itself if it is a ring, and otherwise its selector ring, if it has one, and the rings inside each of its cells, at every depth.

A nested layer is validated as a layer in its own right.

**Selecting a cell.** A layer's cell for a key `k` is given by the first of these steps that applies:

1. **Exact rule:** the cell of the exact rule whose byte string equals `k`.
2. **Longest prefix:** the cell of the prefix rule with the longest byte string among those that are a prefix of `k`. The empty string is a prefix of every key, so the catch-all applies to every key that no other rule matches.
3. **Selector ring:** the ID of `k`'s preferred primary on the selector ring (§7), read as a cell ID. The first pass of §7 always takes the first node of the candidate order, so this is the first node of `k`'s candidate order on the selector ring (§5, §6). The selector ring's replication factor, zones, and hosts don't affect the choice.

Rules compare raw bytes, with no normalization (§1). The rejection rules above guarantee that step 3 is reached only when the layer has a selector ring, and that every step yields a cell of the layer.

**Placing a key.** A key's **path** is the list of cell IDs that the layers choose, from the top layer to the leaf. The top layer selects a cell for the key. If that cell is a ring, it is the key's leaf ring and the path ends; otherwise the cell is a layer, which selects again. Each selector ring hashes the key with its own seed (§3), and so does the leaf ring.

The key's replica set is its replica set on the leaf ring (§7), at the leaf ring's replication factor, and its preference list is its preference list on the leaf ring.

**Filters.** A filter (§8) applies only inside the leaf ring. The path never depends on a filter: when a filter rejects every node of the leaf ring, the key has no eligible replicas, and it does not move to another cell.

**Seed independence.** The last rejection rule keeps every ring's key hash independent of the selector rings above it. A ring sharing a seed with a selector ring above it would hash keys in step with that selector's choice. Under vnodes, a cell's keys would all come from the arcs the cell won above, so the cell's ring would see a skewed slice of its circle. Under rendezvous, a node whose ID equals its cell's ID would draw exactly as its cell did, and so win far more than its share of the cell's keys.

*Informative:* one rule keeps seeds distinct. Give each ring the seed `H(path, 0)` (§3), where `path` is the UTF-8 bytes of the IDs of the cells that hold the ring, joined with `/`: `eu/eu-3` for the ring of cell `eu-3` inside cell `eu`, `eu` for the selector ring of cell `eu`, and the empty string for the top layer's selector ring.

**Layer fingerprint.** A layer's fingerprint covers its directory, its selector ring, and its cells, at every depth. Little-endian throughout; `string(b)` is `le32(len(b)) ‖ b`.

```text
encoding = "CELL"                                    # layer tag: the 4 bytes 43 45 4c 4c
           u8 mode                                   # 0 = within: all replicas in one cell
           le32(exact_rule_count)
           for each exact rule, in canonical order:
             string(byte_string) ‖ string(cell_id)
           le32(prefix_rule_count)
           for each prefix rule, in canonical order:
             string(byte_string) ‖ string(cell_id)
           u8 selector_present ‖ [le64(selector_fingerprint)]   # 0 = absent, 1 = present
           le32(cell_count)
           for each cell, in canonical order:
             string(cell_id) ‖ le64(cell_fingerprint)
fingerprint = XXH3_64bits_withSeed(encoding, 0)
```

`selector_fingerprint` is the selector ring's fingerprint (§11), which covers the cells' weights. A cell's fingerprint is its ring's fingerprint (§11) or, for a nested layer, that layer's fingerprint by this section. Epochs appear nowhere, so they never affect a layer's fingerprint. The layer tag sets a layer's encoding apart from a ring's (§10), which starts with its format version. Version 1 defines only mode 0. Peers compare layers by their fingerprints, as they compare rings (§11).

**Versioning.** §12 applies to this section too: a change to the paths, replica sets, or fingerprints this section gives for the same inputs requires a new spec version. The golden vectors for cells layers are cases in the same vectors file, described by its `layout` field.
