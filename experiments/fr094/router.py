#!/usr/bin/env python3
"""FR-094 question 5: a child's delegated JWT through the router, with a libSQL-style client.

A dev node gets a `router` principal with `forward_tokens` (dev mode lets its configuration be
edited), and `fleetring router` serves Hrana over HTTP on loopback. The harness plays the
part ADR-0048's credential proxy would: it reads the child's token file and sends Hrana
pipeline requests with it as the bearer token, as `@libsql/client` would. It checks the child's
own database, a database outside its scopes, and the same token after the parent revokes it.

Usage: router.py --run-dir DIR --port PORT   (uses PORT, PORT+1 for the node, PORT+2 for the router)
"""

from __future__ import annotations

import argparse
import json
import signal
import time
import urllib.error
import urllib.request
from pathlib import Path

from harness import Fleet, fresh


def pipeline(base: str, db: str, token: str, sql: str) -> tuple[int, dict]:
    body = json.dumps({"requests": [{"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]}).encode()
    req = urllib.request.Request(f"{base}/db/{db}/v2/pipeline", data=body, method="POST",
                                 headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        text = e.read().decode(errors="replace")
        try:
            return e.code, json.loads(text)
        except json.JSONDecodeError:
            return e.code, {"raw": text[:300]}


def brief(status: int, body: dict) -> dict:
    results = body.get("results") or []
    first = results[0] if results else {}
    if first.get("type") == "ok":
        rows = first.get("response", {}).get("result", {}).get("rows")
        return {"http": status, "ok": True, "rows": rows}
    return {"http": status, "ok": False, "error": first.get("error") or body}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--port", type=int, default=7640)
    a = ap.parse_args()
    fresh(a.run_dir)
    f = Fleet(a.run_dir, a.port)
    f.start()
    f.stop()
    cfg = f.dev_dir / "fleetringd.toml"
    cfg.write_text(cfg.read_text() + '\n[[principal]]\nname = "router"\nscopes = []\nforward_tokens = true\n')
    tls = f.dev_dir / "tls"
    rtls = a.run_dir / "router-tls"  # dev mode refuses a dev directory holding files it didn't write
    assert f.cli("cert", "client", "router", "--dir", str(tls), "--out", str(rtls)).ok
    f.start()
    rport = a.port + 2
    (a.run_dir / "router.toml").write_text(f"""listen = "127.0.0.1:{rport}"
[tls]
ca = "{tls / 'ca.crt'}"
cert = "{rtls / 'client.router.crt'}"
key = "{rtls / 'client.router.key'}"
[control]
address = "127.0.0.1:{a.port}"
""")
    router = f.popen("router", "--config", str(a.run_dir / "router.toml"), name="router")
    time.sleep(2)
    base = f"http://127.0.0.1:{rport}"
    out: dict = {}
    assert f.cli("agent", "add", "42").ok
    time.sleep(12)
    f.cli("db", "create", "agent-42-memory", agent="42")
    r = f.cli("agent", "delegate", "42.c1", "--ttl", "10m", "-o", "json")
    assert r.ok, r.err
    f.snapshot_tokens()
    f.cli("db", "create", "agent-42.c1-work", agent="42.c1")
    token = (f.agents_dir / "42.c1" / "token").read_text().strip()  # what a credential proxy would inject
    out["own_db"] = brief(*pipeline(base, "agent-42.c1-work", token, "CREATE TABLE IF NOT EXISTS t (x)"))
    out["own_db_insert"] = brief(*pipeline(base, "agent-42.c1-work", token, "INSERT INTO t VALUES (1)"))
    out["own_db_select"] = brief(*pipeline(base, "agent-42.c1-work", token, "SELECT count(*) FROM t"))
    out["parents_memory"] = brief(*pipeline(base, "agent-42-memory", token, "SELECT 1"))
    out["garbage_token"] = brief(*pipeline(base, "agent-42.c1-work", token[:-4] + "AAAA", "SELECT 1"))
    t0 = time.monotonic()
    rm = f.cli("agent", "remove", "42.c1", "--yes")
    out["remove_ok"] = rm.ok
    refused_after = None
    while time.monotonic() - t0 < 10:
        res = brief(*pipeline(base, "agent-42.c1-work", token, "SELECT 1"))
        if not res["ok"]:
            refused_after = time.monotonic() - t0
            out["after_revocation"] = res
            break
        time.sleep(0.02)
    out["refused_after_revocation_s"] = round(refused_after, 3) if refused_after else None
    router.send_signal(signal.SIGTERM)
    router.wait(10)
    out["leak_check"] = f.leak_check()
    f.stop()
    f.close()
    (a.run_dir / "result.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
