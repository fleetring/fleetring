#!/usr/bin/env python3
"""FR-094 measurements beside the workflow: each against its own fresh dev node.

  latency     DelegateToken latency over ~200 calls (CLI wall time and the node's own duration)
  fanout      how fast one root lineage can fan out until the limits refuse (30/min per caller,
              60/min from a lineage's delegated agents, 256 live per root)
  revocation  time from revocation to refusal, on MCP requests, CLI requests, and open streams
  forksize    fork time and disk for 10, 100, and 250 MiB sources
  perchild    control-plane round trips for one child's whole life, step by step

Usage: measure.py <what> --run-dir DIR --port PORT
"""

from __future__ import annotations

import argparse
import json
import re
import threading
import time
from pathlib import Path

from harness import Fleet, du, fresh, summary


def node(run_dir: Path, port: int) -> Fleet:
    fresh(run_dir)
    f = Fleet(run_dir, port)
    f.start()
    return f


# ----- latency --------------------------------------------------------------------------------

def latency(f: Fleet) -> dict:
    roots = [f"lat{i}" for i in range(7)]
    for r in roots:
        assert f.cli("agent", "add", r).ok
    # Baselines: a CLI process that calls nothing on the fleet, and one that makes one data-path call.
    base_local = [f.cli("agent", "show", roots[0], "-o", "json").secs for _ in range(20)]
    f.cli("db", "create", f"agent-{roots[0]}-x", agent=roots[0])
    base_call = [f.cli("db", "show", f"agent-{roots[0]}-x", "-o", "json", agent=roots[0]).secs for _ in range(20)]
    wall, server, refused = [], [], []
    for r in roots:
        for i in range(29):
            before = f.metrics()
            res = f.cli("agent", "delegate", f"{r}.k{i}", "--ttl", "10m", "-o", "json", "-q")
            after = f.metrics()
            if not res.ok:
                refused.append(res.err.strip())
                continue
            wall.append(res.secs)
            server.append(after.get("dur/control/DelegateToken", 0) - before.get("dur/control/DelegateToken", 0))
    f.snapshot_tokens()
    return {"calls": len(wall), "refused": refused, "cli_wall_ms": summary(wall), "server_ms": summary(server),
            "baseline_cli_no_rpc_ms": summary(base_local), "baseline_cli_one_rpc_ms": summary(base_call)}


# ----- fan-out --------------------------------------------------------------------------------

def classify(err: str) -> str:
    e = err.lower()
    if "256" in e:
        return "live-256"
    if "lineage" in e or "60" in e:
        return "rate-lineage"
    if "30" in e or "a minute" in e or "per minute" in e:
        return "rate-caller"
    return "other"


def burst(f: Fleet, parent: str, prefix: str, n: int, threads: int = 8, ttl: str = "10m") -> dict:
    out: list[tuple[str, int, str, float]] = []
    lock = threading.Lock()
    names = iter(f"{parent}.{prefix}{i}" for i in range(n))

    def worker():
        while True:
            with lock:
                name = next(names, None)
            if name is None:
                return
            r = f.cli("agent", "delegate", name, "--ttl", ttl, "-o", "json", "-q")
            with lock:
                out.append((name, r.code, r.err.strip(), time.monotonic()))

    t0 = time.monotonic()
    ts = [threading.Thread(target=worker) for _ in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    ok = [o for o in out if o[1] == 0]
    refused = [o for o in out if o[1] != 0]
    return {"parent": parent, "asked": n, "ok": len(ok), "refused": len(refused),
            "elapsed_s": round(time.monotonic() - t0, 3),
            "ok_per_s": round(len(ok) / max(1e-9, (max(o[3] for o in ok) - t0) if ok else 1), 1),
            "first_refusal": refused[0][2] if refused else None,
            "refusal_kinds": sorted({classify(o[2]) for o in refused}),
            "ok_names": [o[0] for o in ok]}


def fanout(f: Fleet) -> dict:
    root = "fan"
    assert f.cli("agent", "add", root).ok
    res: dict = {}
    # 1. The root alone, as fast as 8 threads go.
    res["root_burst"] = burst(f, root, "a", 40)
    for k in ("ok_names",):
        res["root_burst"].pop(k)
    # 2. A minute later: three children that can delegate, then all three fan out at once.
    # 61 s by the harness's clock; control time stalls while this host's wall clock is stepped back
    # (see the write-up), so the window can take a few seconds longer to reopen.
    time.sleep(61)
    kids = [f"{root}.m{i}" for i in range(3)]
    waited = 0.0
    for k in kids:
        while True:
            r = f.cli("agent", "delegate", k, "--ttl", "30m", "--scope", f"admin:agent-{k}-*", "--scope", f"admin:agent-{k}.*",
                      "--scope", f"grant:agent-{k}.*", "-q")
            if r.ok or "a minute" not in r.err or waited > 30:
                break
            time.sleep(1)
            waited += 1
        assert r.ok, r.err
    res["extra_wait_for_window_s"] = waited
    results = {}
    ts = [threading.Thread(target=lambda k=k: results.__setitem__(k, burst(f, k, "g", 30, threads=4))) for k in kids]
    t0 = time.monotonic()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    res["lineage_burst"] = {"elapsed_s": round(time.monotonic() - t0, 3),
                            "ok_total": sum(r["ok"] for r in results.values()),
                            "per_child": {k: {x: v[x] for x in ("ok", "refused", "first_refusal", "refusal_kinds")} for k, v in results.items()}}
    # 3. Keep going at the full rate until the 256-live limit refuses.
    live = 3 + res["root_burst"]["ok"] + res["lineage_burst"]["ok_total"]
    t0 = time.monotonic()
    rounds = 0
    hit_256 = None
    while time.monotonic() - t0 < 360 and not hit_256:
        rounds += 1
        time.sleep(66)
        bursts = {}
        ts = [threading.Thread(target=lambda p=p: bursts.__setitem__(p, burst(f, p, f"r{rounds}x", 30, threads=4)))
              for p in [root, *kids]]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        live += sum(b["ok"] for b in bursts.values())
        for b in bursts.values():
            if b["first_refusal"] and "256" in b["first_refusal"]:
                hit_256 = b["first_refusal"]
    res["live_limit"] = {"live_when_refused": live, "minutes": rounds, "refusal": hit_256}
    # 4. Revoking one grandchild frees its slot at once.
    victim = next(iter(sorted(p.name for p in f.agents_dir.glob(f"{kids[0]}.g*"))), None)
    if victim:
        rm = f.cli("agent", "remove", victim, "--yes")
        again = f.cli("agent", "delegate", f"{root}.after", "--ttl", "5m", "-q")
        res["after_one_revocation"] = {"removed": victim, "remove_ok": rm.ok, "delegate_code": again.code,
                                       "delegate_err": again.err.strip()[:300]}
    f.snapshot_tokens()
    return res


# ----- revocation -----------------------------------------------------------------------------

def revocation(f: Fleet) -> dict:
    root = "rev"
    assert f.cli("agent", "add", root).ok
    trials = []
    for i in range(12):
        how = "agent remove" if i < 8 else "token revoke --token-id"
        cid = f"{root}.t{i}"
        info = json.loads(f.cli("agent", "delegate", cid, "--ttl", "10m", "-o", "json", "-q").out)["agents"][0]
        f.snapshot_tokens()
        m = f.mcp(cid)
        db = f"agent-{cid}-db"
        m.call("db_create", {"name": db, "expires_in": "1h"})
        m.call("exec", {"database": db, "sql": "CREATE TABLE t (x)"})
        watch = f.popen("db", "watch", db, "-o", "jsonl", agent=cid, name=f"watch-{cid}")
        time.sleep(1.2 + (i % 4) * 0.25)  # spread the revocation across the stream's recheck period
        stop = threading.Event()
        mcp_refused: dict = {}
        cli_refused: dict = {}

        def poll_mcp():
            while not stop.is_set():
                ok, body, _ = m.call("query", {"database": db, "sql": "SELECT count(*) FROM t"}, expect_ok=None)
                if not ok:
                    mcp_refused["t"] = time.monotonic()
                    mcp_refused["error"] = body.get("error")
                    return
                time.sleep(0.01)

        def poll_cli():
            while not stop.is_set():
                r = f.cli("db", "query", db, "SELECT 1", agent=cid)
                if not r.ok:
                    cli_refused["t"] = time.monotonic()
                    cli_refused["err"] = r.err.strip()[:300]
                    return

        threads = [threading.Thread(target=poll_mcp)]
        if how != "agent remove":  # agent remove deletes the identity, so the CLI can't even try
            threads.append(threading.Thread(target=poll_cli))
        for t in threads:
            t.start()
        time.sleep(0.5)
        t0 = time.monotonic()
        if how == "agent remove":
            r = f.cli("agent", "remove", cid, "--yes")
        else:
            r = f.cli("token", "revoke", "--token-id", info["delegation_id"], "--yes", agent=root)
        t1 = time.monotonic()
        end = None
        while time.monotonic() - t1 < 10:
            if watch.poll() is not None:
                end = time.monotonic()
                break
            time.sleep(0.005)
        for t in threads:
            t.join(10)
        stop.set()
        m.close()
        if watch.poll() is None:
            watch.kill()
        trials.append({"how": how, "revoke_ok": r.ok, "revoke_cli_s": round(t1 - t0, 4),
                       "mcp_refused_s": round(mcp_refused["t"] - t1, 4) if "t" in mcp_refused else None,
                       "mcp_refused_from_start_s": round(mcp_refused["t"] - t0, 4) if "t" in mcp_refused else None,
                       "cli_refused_s": round(cli_refused["t"] - t1, 4) if "t" in cli_refused else None,
                       "stream_end_s": round(end - t1, 4) if end else None,
                       "stream_end_from_start_s": round(end - t0, 4) if end else None,
                       "mcp_error": mcp_refused.get("error"), "cli_error": cli_refused.get("err"),
                       "revoke_out": r.out.strip()[:200], "revoke_err": r.err.strip()[:200]})
        f.cli("db", "drop", db, "--yes", agent=root)
    streams = [t["stream_end_from_start_s"] for t in trials if t["stream_end_from_start_s"] is not None]
    mcps = [t["mcp_refused_from_start_s"] for t in trials if t["mcp_refused_from_start_s"] is not None]
    clis = [t["cli_refused_s"] + t["revoke_cli_s"] for t in trials if t["cli_refused_s"] is not None]
    revokes = [t["revoke_cli_s"] for t in trials]
    return {"trials": trials, "revoke_cli_ms": summary(revokes), "mcp_refusal_from_revoke_start_ms": summary(mcps),
            "cli_refusal_from_revoke_start_ms": summary(clis), "stream_end_from_revoke_start_ms": summary(streams)}


# ----- fork size and disk -----------------------------------------------------------------------

def disk_breakdown(f: Fleet) -> dict:
    base = f.dev_dir / "data"
    out = {"total": du(base)}
    if base.exists():
        for p in base.iterdir():
            if p.is_dir():
                out[p.name] = du(p)
    return out


def forksize(f: Fleet) -> dict:
    root = "fs"
    assert f.cli("agent", "add", root).ok
    time.sleep(15)  # forks advance only once the control node's failover window has passed
    rows = []
    for mib in (10, 100, 250):
        src = f"agent-{root}-src{mib}"
        f.cli("db", "create", src, agent=root)
        f.cli("db", "exec", src, "CREATE TABLE blobs (id INTEGER PRIMARY KEY, b BLOB)", agent=root)
        n = mib * 256  # 4 KiB rows
        t_fill = time.monotonic()
        for lo in range(0, n, 2560):  # 10 MiB a statement
            hi = min(n, lo + 2560)
            r = f.cli("db", "exec", src, f"WITH RECURSIVE c(x) AS (SELECT {lo + 1} UNION ALL SELECT x + 1 FROM c WHERE x < {hi}) INSERT INTO blobs SELECT x, randomblob(4096) FROM c", agent=root)
            assert r.ok, r.err
        fill_s = time.monotonic() - t_fill
        time.sleep(3)
        before = disk_breakdown(f)
        r = f.cli("db", "fork", src, f"agent-{root}-copy{mib}", "-o", "json", "-q", agent=root)
        fork = r.json() if r.ok else {"error": r.err}
        time.sleep(4)  # the staged copy goes once holders are released
        after = disk_breakdown(f)
        rows.append({"mib": mib, "fill_s": round(fill_s, 2), "fork_cli_s": round(r.secs, 3),
                     "fork_elapsed_ms": fork.get("elapsed_ms"), "state": fork.get("state"),
                     "disk_before": before, "disk_after": after,
                     "disk_growth_ratio": round(after["total"] / max(1, before["total"]), 3)})
        print(json.dumps(rows[-1]), flush=True)
    return {"rows": rows}


# ----- per child ------------------------------------------------------------------------------

def perchild(f: Fleet) -> dict:
    root = "pc"
    assert f.cli("agent", "add", root).ok
    time.sleep(15)
    f.cli("db", "create", f"agent-{root}-memory", agent=root)
    f.cli("db", "exec", f"agent-{root}-memory", "CREATE TABLE t (x)", agent=root)
    steps = []
    cid = f"{root}.c1"
    holder = {}

    def step(name, fn):
        b = f.metrics()
        t0 = time.monotonic()
        out = fn()
        steps.append({"step": name, "wall_s": round(time.monotonic() - t0, 3),
                      "control": Fleet.delta(b, f.metrics()), "data": Fleet.delta(b, f.metrics(), "data"),
                      "database": Fleet.delta(b, f.metrics(), "database")})
        return out

    step("agent delegate", lambda: f.cli("agent", "delegate", cid, "--ttl", "10m", "--scope", f"admin:agent-{cid}-*",
                                         "--scope", f"stream:agent-{cid}-*", "--scope", f"fork:agent-{root}-memory", "-q"))
    holder["m"] = step("mcp --agent start + handshake", lambda: f.mcp(cid))
    m = holder["m"]
    step("db_fork (memory)", lambda: m.call("db_fork", {"source": f"agent-{root}-memory", "name": f"agent-{cid}-memory"}))
    step("db_create (scratch)", lambda: m.call("db_create", {"name": f"agent-{cid}-scratch", "expires_in": "1h"}))
    step("10 x exec", lambda: [m.call("exec", {"database": f"agent-{cid}-memory", "sql": "INSERT INTO t VALUES (?)", "params": [i]}) for i in range(10)])
    step("query", lambda: m.call("query", {"database": f"agent-{cid}-memory", "sql": "SELECT count(*) FROM t"}))
    step("db_drop (scratch)", lambda: m.call("db_drop", {"name": f"agent-{cid}-scratch", "confirm": f"agent-{cid}-scratch"}))
    m.close()
    step("parent query of child's memory", lambda: f.cli("db", "query", f"agent-{cid}-memory", "SELECT count(*) FROM t", agent=root))
    step("parent db drop child's memory", lambda: f.cli("db", "drop", f"agent-{cid}-memory", "--yes", agent=root))
    step("agent remove", lambda: f.cli("agent", "remove", cid, "--yes"))
    f.snapshot_tokens()
    return {"steps": steps}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["latency", "fanout", "revocation", "forksize", "perchild"])
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--port", type=int, default=7490)
    a = ap.parse_args()
    f = node(a.run_dir, a.port)
    try:
        result = globals()[a.what](f)
        result["leak_check"] = f.leak_check()
    finally:
        f.stop()
        f.close()
    (a.run_dir / "result.json").write_text(json.dumps(result, indent=2))
    printable = {k: v for k, v in result.items() if k not in ("trials",)}
    print(json.dumps(printable, indent=2)[:6000])


if __name__ == "__main__":
    main()
