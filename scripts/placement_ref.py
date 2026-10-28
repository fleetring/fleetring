"""Independent reference implementation of FleetRing placement spec v1.

Written from docs/spec/placement-v1.md alone, not ported from the Rust crate, to show that the
spec is complete: it recomputes every vector in the golden file and reports any mismatch. It
needs only the PyPI `xxhash` package, which wraps the C xxHash library, and Python's integers,
which are exact at any size, so no product can overflow.

    python3 -m venv .venv && .venv/bin/pip install xxhash
    .venv/bin/python scripts/placement_ref.py crates/fleetring/tests/vectors/placement-v1.json

Exits 0 and prints a summary when every vector matches; prints each mismatch and exits 1
otherwise.
"""

from __future__ import annotations

import bisect
import functools
import json
import re
import sys
from decimal import ROUND_FLOOR, ROUND_HALF_EVEN, Decimal, localcontext
from typing import Callable, Iterable, Optional

import xxhash

# Section 1: conventions.

U64_MAX = (1 << 64) - 1


def le32(n: int) -> bytes:
    """Encodes n as 4 little-endian bytes."""
    return n.to_bytes(4, "little")


def le64(n: int) -> bytes:
    """Encodes n as 8 little-endian bytes."""
    return n.to_bytes(8, "little")


# Section 3: hashing.

HASHER = "xxh3-64"


def H(data: bytes, seed: int) -> int:
    """XXH3_64bits_withSeed(data, len(data), seed)."""
    return xxhash.xxh3_64_intdigest(data, seed=seed)


# Section 4: the fixed-point logarithm.

Q32 = 1 << 32

# The spec's claim about T: every entry sits at least this far, in Q32 units, from a rounding
# boundary, so the rounding mode and precision don't matter.
ROUNDING_MARGIN = Decimal("7.4e-4")


def log2_table() -> list[int]:
    """T[j] = round(2^32 * log2(1 + j/1024)) for j = 0..1024, computed to 60 digits."""
    table = []
    with localcontext() as context:
        context.prec = 60
        ln2 = Decimal(2).ln()
        for j in range(1025):
            exact = (Decimal(1024 + j) / 1024).ln() / ln2 * Q32
            margin = abs(exact - exact.to_integral_value(rounding=ROUND_FLOOR) - Decimal("0.5"))
            if margin < ROUNDING_MARGIN:
                raise AssertionError(f"T[{j}] = {exact} is within {margin} of a rounding boundary")
            table.append(int(exact.to_integral_value(rounding=ROUND_HALF_EVEN)))
    return table


T = log2_table()


def L(x: int) -> int:
    """-log2((x + 1) / 2^64) in Q32, by the table algorithm of section 4."""
    if x == U64_MAX:
        return 0
    v = x + 1
    e = v.bit_length() - 1
    m = v << (63 - e)
    j = (m >> 53) & 1023
    r = m & ((1 << 53) - 1)
    f = T[j] + (T[j + 1] - T[j]) * r // (1 << 53)
    return (64 - e) * Q32 - f


# Section 2: ring inputs.

LABEL = re.compile(r"[a-z0-9._:-]{1,64}")
MAX_TOKENS = 1 << 22


class Ring:
    """A validated ring, with its nodes in canonical order."""

    def __init__(self, spec: dict) -> None:
        if spec["hasher"] != HASHER:
            raise ValueError(f"unsupported hasher {spec['hasher']!r}")
        self.seed = int(spec["seed"], 16)
        self.replication = spec["replication_factor"]
        if not 1 <= self.replication <= 255:
            raise ValueError(f"replication factor {self.replication} is out of range")
        strategy = spec["strategy"]
        self.kind = strategy["kind"]
        if self.kind not in ("rendezvous", "vnodes"):
            raise ValueError(f"unknown strategy {self.kind!r}")
        self.tokens_per_weight = strategy.get("tokens_per_weight", 0)
        if self.kind == "vnodes" and self.tokens_per_weight < 1:
            raise ValueError("vnodes need tokens_per_weight >= 1")

        nodes = sorted(spec["nodes"], key=lambda node: node["id"].encode())
        if not nodes:
            raise ValueError("a ring needs at least one node")
        for node in nodes:
            for field in ("id", "zone", "host"):
                if field in node and not LABEL.fullmatch(node[field]):
                    raise ValueError(f"bad {field} {node[field]!r}")
            if not 1 <= node["weight"] < 1 << 32:
                raise ValueError(f"node {node['id']}: weight {node['weight']} is out of range")
        self.ids = [node["id"] for node in nodes]
        if len(set(self.ids)) != len(self.ids):
            raise ValueError("two nodes share an ID")
        self.weights = [node["weight"] for node in nodes]
        self.zone_labels = [node.get("zone") for node in nodes]
        self.host_labels = [node.get("host") for node in nodes]
        host_zones: dict[str, Optional[str]] = {}
        for host, zone in zip(self.host_labels, self.zone_labels):
            if host is not None and host_zones.setdefault(host, zone) != zone:
                raise ValueError(f"host {host!r} is in more than one zone")

        # Section 7: absent zones form one default zone; an unlabeled node is its own host.
        self.zones = [("zone", zone) if zone else ("default",) for zone in self.zone_labels]
        self.hosts = [
            ("host", host) if host else ("own", i) for i, host in enumerate(self.host_labels)
        ]
        self.node_hashes = [H(id_.encode(), self.seed) for id_ in self.ids]

        if self.kind == "vnodes":
            total = sum(w * self.tokens_per_weight for w in self.weights)
            if total > MAX_TOKENS:
                raise ValueError(f"{total} tokens exceed 2^22")
            # Section 6: (position, index) pairs, sorted by position, then by index.
            self.tokens = sorted(
                (H(le64(self.node_hashes[i]) + le32(k), self.seed), i)
                for i, w in enumerate(self.weights)
                for k in range(w * self.tokens_per_weight)
            )
            self.positions = [position for position, _ in self.tokens]

    @property
    def node_count(self) -> int:
        return len(self.ids)

    # Sections 5 and 6: candidate orders.

    def candidate_order(self, key: bytes) -> list[int]:
        """Every node's index, best first, for key."""
        kh = H(key, self.seed)
        if self.kind == "rendezvous":
            return self._rendezvous_order(kh)
        return self._vnode_order(kh)

    def _rendezvous_order(self, kh: int) -> list[int]:
        scores = []
        for i, nh in enumerate(self.node_hashes):
            x = H(le64(nh) + le64(kh), self.seed)
            scores.append((i, x, L(x)))

        def compare(a: tuple[int, int, int], b: tuple[int, int, int]) -> int:
            (ia, xa, la), (ib, xb, lb) = a, b
            left, right = la * self.weights[ib], lb * self.weights[ia]
            if left != right:
                return -1 if left < right else 1
            if xa != xb:
                return -1 if xa > xb else 1
            return -1 if ia < ib else (1 if ia > ib else 0)

        scores.sort(key=functools.cmp_to_key(compare))
        return [i for i, _, _ in scores]

    def _vnode_order(self, kh: int) -> list[int]:
        count = len(self.tokens)
        start = bisect.bisect_left(self.positions, kh)
        if start == count:
            start = 0
        order: list[int] = []
        seen: set[int] = set()
        for step in range(count):
            _, i = self.tokens[(start + step) % count]
            if i not in seen:
                seen.add(i)
                order.append(i)
                if len(order) == self.node_count:
                    break
        return order

    # Sections 7 and 8: preference lists and replica sets.

    def preference(self, key: bytes, eligible: Callable[[int], bool] = lambda i: True) -> list[int]:
        """The three diversity passes over the candidate order restricted to eligible nodes."""
        order = [i for i in self.candidate_order(key) if eligible(i)]
        taken: set[int] = set()
        zones_taken: set[tuple] = set()
        hosts_taken: set[tuple] = set()
        result = []
        for i in order:
            if self.zones[i] not in zones_taken:
                zones_taken.add(self.zones[i])
                hosts_taken.add(self.hosts[i])
                taken.add(i)
                result.append(i)
        for i in order:
            if i not in taken and self.hosts[i] not in hosts_taken:
                hosts_taken.add(self.hosts[i])
                taken.add(i)
                result.append(i)
        result.extend(i for i in order if i not in taken)
        return result

    def replicas(self, key: bytes, eligible: Callable[[int], bool] = lambda i: True) -> list[int]:
        """The replica set at the ring's replication factor."""
        return self.preference(key, eligible)[: self.replication]

    # Section 9: bounded loads.

    def replicas_to_place(self) -> int:
        """r = min(R, N), where N counts every node of the ring."""
        return min(self.replication, self.node_count)

    def capacities(self, loads: list[int], epsilon: int, item: int) -> list[int]:
        if not all(0 <= value <= U64_MAX for value in loads + [item]) or not 0 <= epsilon <= 65535:
            raise ValueError("loads and items are 64-bit, and headroom is 0 to 65535")
        total = sum(loads)
        q = total + self.replicas_to_place() * item
        d = 100 * sum(self.weights)
        result = []
        for w in self.weights:
            f = (100 + epsilon) * w
            capacity = (q // d) * f + -(-((q % d) * f) // d)
            # The split form must equal the plain ceiling the spec states beside it.
            assert capacity == -(-((100 + epsilon) * q * w) // (100 * sum(self.weights)))
            result.append(capacity)
        return result

    def bounded(self, key: bytes, loads: list[int], capacities: list[int], item: int):
        """Returns (replicas, saturated, home) for a bounded-load placement."""
        has_room = [loads[i] + item <= capacities[i] for i in range(self.node_count)]
        chosen = self.replicas(key, lambda i: has_room[i])
        r = self.replicas_to_place()
        saturated = len(chosen) < r
        for i in self.preference(key):
            if len(chosen) == r:
                break
            if i not in chosen:
                chosen.append(i)
        home = chosen == self.replicas(key)
        return chosen, saturated, home

    # Sections 10 and 11: the canonical encoding and the fingerprint.

    def encoding(self) -> bytes:
        out = le32(1)
        if self.kind == "rendezvous":
            out += bytes([0])
        else:
            out += bytes([1]) + le32(self.tokens_per_weight)
        out += le64(self.seed) + bytes([self.replication])
        out += string(HASHER.encode())
        out += le32(self.node_count)
        for i in range(self.node_count):
            out += string(self.ids[i].encode()) + le32(self.weights[i])
            out += optional(self.zone_labels[i]) + optional(self.host_labels[i])
        return out

    def fingerprint(self) -> int:
        probes = b""
        for i in range(64):
            members = self.replicas(b"fleetring-probe-%02d" % i)
            probes += le32(len(members)) + b"".join(le32(j) for j in members)
        return H(self.encoding() + probes, 0)


def string(data: bytes) -> bytes:
    return le32(len(data)) + data


def optional(label: Optional[str]) -> bytes:
    if label is None:
        return bytes([0])
    return bytes([1]) + string(label.encode())


# Checking the vector file.


class Checker:
    """Counts checks per category and collects mismatches."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.failures: list[str] = []

    def check(self, category: str, got: object, want: object, context: str) -> None:
        self.counts[category] = self.counts.get(category, 0) + 1
        if got != want:
            self.failures.append(f"{context}: got {got!r}, want {want!r}")


def pattern(length: int) -> bytes:
    """The XXH3 vector input: byte i is (31 * i + 7) mod 256."""
    return bytes((31 * i + 7) % 256 for i in range(length))


def check_case(case: dict, checker: Checker) -> None:
    name = case["name"]
    try:
        ring = Ring(case["ring"])
    except ValueError as error:
        checker.check("rings", str(error), "a valid ring", name)
        return
    checker.check("rings", True, True, name)
    checker.check("fingerprints", f"{ring.fingerprint():016x}", case["fingerprint"], name)

    def ids(indices: Iterable[int]) -> list[str]:
        return [ring.ids[i] for i in indices]

    for entry in case["keys"]:
        key = bytes.fromhex(entry["key"])
        checker.check(
            "replica sets", ids(ring.replicas(key)), entry["replicas"], f"{name}: key {entry['key']}"
        )

    for scenario in case.get("filters", []):
        down = set(scenario["down"])
        for entry in scenario["keys"]:
            key = bytes.fromhex(entry["key"])
            got = ids(ring.replicas(key, lambda i: ring.ids[i] not in down))
            context = f"{name}: key {entry['key']} with {sorted(down)} down"
            checker.check("filtered replica sets", got, entry["replicas"], context)

    for scenario in case.get("bounded", []):
        epsilon, item = scenario["headroom"], int(scenario["item_load"])
        listed = {node: int(load) for node, load in scenario["loads"].items()}
        loads = [listed.get(id_, 0) for id_ in ring.ids]
        capacities = ring.capacities(loads, epsilon, item)
        where = f"{name}: headroom {epsilon}%, item {item}"
        got_capacities = {ring.ids[i]: str(c) for i, c in enumerate(capacities)}
        checker.check("capacity sets", got_capacities, scenario["capacity"], f"{where}: capacities")
        for entry in scenario["keys"]:
            key = bytes.fromhex(entry["key"])
            replicas, saturated, home = ring.bounded(key, loads, capacities, item)
            want = (entry["replicas"], entry["saturated"], entry["home"])
            context = f"{where}: key {entry['key']} (replicas, saturated, home)"
            checker.check("bounded placements", (ids(replicas), saturated, home), want, context)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} PATH/TO/placement-v1.json", file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as file:
        vectors = json.load(file)
    checker = Checker()

    for entry in vectors["xxh3_64"]["cases"]:
        seed = int(entry["seed"], 16)
        got = f"{H(pattern(entry['len']), seed):016x}"
        checker.check("xxh3_64", got, entry["digest"], f"XXH3-64 of {entry['len']} bytes, seed {entry['seed']}")
    for j, want in enumerate(vectors["log2_table"]):
        checker.check("log2_table", T[j] if j < len(T) else None, want, f"T[{j}]")
    checker.check("log2_table", len(vectors["log2_table"]), len(T), "table length")
    for entry in vectors["neg_log2_q32"]:
        checker.check("neg_log2_q32", L(int(entry["x"], 16)), entry["l"], f"L({entry['x']})")
    for case in vectors["cases"]:
        check_case(case, checker)

    summary = ", ".join(f"{count} {category}" for category, count in checker.counts.items())
    print(f"placement_ref: {len(vectors['cases'])} cases; checked {summary}")
    print(f"C xxHash {xxhash.XXHASH_VERSION} via python-xxhash {xxhash.VERSION}")
    if checker.failures:
        for failure in checker.failures[:50]:
            print(f"MISMATCH {failure}")
        if len(checker.failures) > 50:
            print(f"... and {len(checker.failures) - 50} more")
        print(f"FAILED: {len(checker.failures)} mismatches")
        return 1
    print("OK: every vector matches")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
