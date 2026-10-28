---
id: FR-004
title: "Domain types: NodeId, Zone, Host, Weight, ReplicationFactor, Node, Epoch"
status: todo
priority: P0
size: M
milestone: M1 Hashing and model
depends_on: [FR-001]
adrs: [ADR-0005, ADR-0006]
---

# FR-004: Domain types: `NodeId`, `Zone`, `Host`, `Weight`, `ReplicationFactor`, `Node`, `Epoch`

## Goal

Model identity and topology so that invalid values can't be represented (ADR-0005, ADR-0006).

## Scope

- **Labels: `NodeId`, `Zone`, `Host`.** Validated newtypes that share one private validator. The grammar allows 1–64 bytes of `[a-z0-9._:-]`.
  - Construction: `FromStr`, `TryFrom<&str>`, `TryFrom<String>`.
  - Access: `as_str()`, `AsRef<str>`, `Display`.
  - Derives: `Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash`.
  - Store the value as `Box<str>`.
- **`ParseLabelError`.** One error type covering all three labels, since parsing any of them is the same situation. It reports:
  - which label kind failed;
  - the reason: empty, too long, or an invalid byte at a given index.

  It implements `Display` with lowercase messages and no trailing punctuation, implements `Error`, and is `Send + Sync + 'static`.
- **`Weight`.** Wraps a nonzero `u32` and defaults to 1. It has `TryFrom<u32>`, `get()`, and a `const ONE`.
- **`ReplicationFactor`.** Wraps a nonzero `u8` and defaults to 3. It has `TryFrom<u8>` and `get()`.
- **`Node`.** Holds an id, a weight, an optional zone, and an optional host.
  - Construction: `Node::new(id)`, refined with `with_weight`, `with_zone`, and `with_host`.
  - Accessors: `id()`, `weight()`, `zone()`, `host()`.
  - Derives: `Debug, Clone, PartialEq, Eq`.
- **`Epoch`.** A newtype over `u64`. It derives `Copy`, `Ord`, and `Default`, and implements `Display`. FR-007 and FR-013 use it.

## Acceptance criteria

- [ ] Property tests:
  - every string that matches the grammar parses and round-trips through `Display`;
  - every string outside the grammar fails with the correct reason.
- [ ] `Weight` and `ReplicationFactor` reject 0.
- [ ] A compile-time assertion shows every type is `Send + Sync`.
- [ ] Public signatures have no `bool` parameters and no `get_` prefixes. Pure constructors and accessors are `#[must_use]`.
- [ ] Every public item is documented, with an example that uses `?`.

## Notes

- serde impls arrive in FR-013. They deserialize through `TryFrom<String>`, so serde can never construct an invalid label.
- Start with the strict grammar. Relaxing it later is non-breaking (ADR-0005).
