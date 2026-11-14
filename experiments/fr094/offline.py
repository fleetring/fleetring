#!/usr/bin/env python3
"""FR-094 question 1: what can a child do while the control plane is unreachable but a data
node is reachable?

A two-node fleet on loopback, not dev mode: `ctl` (role control, with a delegation issuer)
and `data1` (role data). Root agent `42` is minted with `agent add --authority`, child
`42.c1` with `agent delegate`. The child opens a `db watch` stream, which is served by the
data node, and keeps an MCP server and the CLI busy. Then the control node is killed, and
the script records, for the child: whether requests still work, when the stream ends, and
what `agent delegate` does; then it restarts the control node and records the recovery. It
also times revocation on the separate data node, which hears of it through heartbeats.

Usage: offline.py --run-dir DIR --port PORT   (uses PORT .. PORT+3)
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import threading
import time
from pathlib import Path

from harness import FLEETRING, Fleet, fresh, summary


def write(path: Path, text: str) -> None:
    path.write_text(text)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--port", type=int, default=7600)
    a = ap.parse_args()
    fresh(a.run_dir)
    f = Fleet(a.run_dir, a.port)  # `metrics()` scrapes ctl's admin port (PORT + 1)
    tls = a.run_dir / "tls"
    p_ctl, p_ctl_admin, p_data, p_data_admin = a.port, a.port + 1, a.port + 2, a.port + 3
    for args in (["cert", "init", "--fleet", "off", "--dir", str(tls)],
                 ["cert", "node", "ctl", "--addresses", "127.0.0.1", "--dir", str(tls)],
                 ["cert", "node", "data1", "--addresses", "127.0.0.1", "--dir", str(tls)],
                 ["cert", "client", "admin", "--dir", str(tls)],
                 ["cert", "issuer", "--name", "ctl", "--dir", str(tls)]):
        r = f.cli(*args)
        assert r.ok, r.err
    common = f"""
[[principal]]
name = "admin"
scopes = ["admin:*", "stream:*", "fleet"]

[replication]
replication_factor = 1

[[changes.sink]]
name = "local"
kind = "file"
databases = ["*"]
path = "{a.run_dir / 'changes'}"
"""
    write(a.run_dir / "ctl.toml", f"""data_dir = "{a.run_dir / 'ctl-data'}"
listen = "127.0.0.1:{p_ctl}"
role = "control"
[node]
id = "ctl"
[control]
issuer_cert = "{tls / 'issuer.ctl.crt'}"
issuer_key = "{tls / 'issuer.ctl.key'}"
[tls]
ca = "{tls / 'ca.crt'}"
cert = "{tls / 'node.ctl.crt'}"
key = "{tls / 'node.ctl.key'}"
[admin]
listen = "127.0.0.1:{p_ctl_admin}"
{common}""")
    write(a.run_dir / "data1.toml", f"""data_dir = "{a.run_dir / 'data1-data'}"
listen = "127.0.0.1:{p_data}"
role = "data"
[node]
id = "data1"
[control]
address = "127.0.0.1:{p_ctl}"
[tls]
ca = "{tls / 'ca.crt'}"
cert = "{tls / 'node.data1.crt'}"
key = "{tls / 'node.data1.key'}"
[admin]
listen = "127.0.0.1:{p_data_admin}"
{common}""")

    procs: dict[str, subprocess.Popen] = {}

    def serve(name: str) -> None:
        procs[name] = f.popen("serve", "--config", str(a.run_dir / f"{name}.toml"), name=f"node-{name}")

    out: dict = {}
    serve("ctl")
    time.sleep(1.5)
    serve("data1")
    time.sleep(3)
    r = f.cli("context", "add", "off", "--control", f"127.0.0.1:{p_ctl}", "--tls-dir", str(tls), "--as", "admin")
    assert r.ok, r.err
    out["status"] = f.cli("status").out
    r = f.cli("agent", "add", "42", "--authority", str(tls))
    assert r.ok, r.err
    r = f.cli("agent", "delegate", "42.c1", "--ttl", "30m", "-o", "json")
    assert r.ok, r.err
    f.snapshot_tokens()
    time.sleep(12)  # past the control node's failover window
    f.cli("db", "create", "agent-42.c1-work", agent="42.c1")
    f.cli("db", "exec", "agent-42.c1-work", "CREATE TABLE t (x)", agent="42.c1")
    out["locate"] = f.cli("db", "locate", "agent-42.c1-work", agent="42.c1").out.strip()

    # The child's open stream lives on the data node; its MCP server and CLI make fresh calls.
    watch = f.popen("db", "watch", "agent-42.c1-work", "-o", "jsonl", agent="42.c1", name="watch-c1")
    m = f.mcp("42.c1", context="off")
    time.sleep(1.5)
    ok, _, _ = m.call("exec", {"database": "agent-42.c1-work", "sql": "INSERT INTO t VALUES (1)"})
    out["before_outage_mcp_ok"] = ok

    # Kill the control plane; the data node stays up.
    t_kill = time.monotonic()
    procs["ctl"].send_signal(signal.SIGKILL)
    procs["ctl"].wait()
    samples = []
    stream_end = None
    ended: dict = {}

    def watch_stream():
        while time.monotonic() - t_kill < 30:
            if watch.poll() is not None:
                ended["t"] = time.monotonic() - t_kill
                return
            time.sleep(0.02)

    watcher = threading.Thread(target=watch_stream)
    watcher.start()
    deadline = time.monotonic() + 20
    i = 0
    while time.monotonic() < deadline:
        i += 1
        ok, body, secs = m.call("exec", {"database": "agent-42.c1-work", "sql": "INSERT INTO t VALUES (?)", "params": [i]},
                                expect_ok=None, note="control plane down")
        cli = f.cli("db", "query", "agent-42.c1-work", "SELECT count(*) FROM t", agent="42.c1", timeout=20, note="control plane down")
        samples.append({"t": round(time.monotonic() - t_kill, 2), "mcp_ok": ok, "mcp_error": None if ok else body.get("error"),
                        "mcp_secs": round(secs, 3), "cli_code": cli.code, "cli_err": cli.err.strip()[-200:], "cli_secs": round(cli.secs, 3)})
        time.sleep(1)
    watcher.join(15)
    stream_end = ended.get("t")
    out["outage_samples"] = samples
    out["stream_ended_after_kill_s"] = round(stream_end, 2) if stream_end else None
    out["stream_stderr"] = (a.run_dir / "watch-c1.stderr").read_text().strip()[-500:]
    t = time.monotonic()
    d = f.cli("agent", "delegate", "42.c1.r1", "--ttl", "5m", timeout=120, note="delegate while the control plane is down")
    out["delegate_during_outage"] = {"code": d.code, "secs": round(time.monotonic() - t, 2), "stderr": d.err.strip()[-600:]}
    data_log = (a.run_dir / "node-data1.stderr").read_text()
    out["data_node_log_after_kill"] = [l[:300] for l in data_log.splitlines() if "stale" in l or "denylist" in l or "revocation" in l.lower()][-6:]

    # Bring the control plane back.
    t_back = time.monotonic()
    serve("ctl")
    recovered = None
    while time.monotonic() - t_back < 60:
        ok, body, _ = m.call("exec", {"database": "agent-42.c1-work", "sql": "INSERT INTO t VALUES (-1)"}, expect_ok=None, note="recovery")
        if ok:
            recovered = time.monotonic() - t_back
            break
        time.sleep(0.5)
    out["mcp_recovered_after_restart_s"] = round(recovered, 2) if recovered else None
    if watch.poll() is None:
        watch.kill()

    # Revocation on a separate data node: it hears of it through heartbeats.
    trials = []
    for k in range(6):
        cid = f"42.v{k}"
        r = f.cli("agent", "delegate", cid, "--ttl", "10m", "-q")
        if not r.ok:
            trials.append({"error": r.err.strip()})
            continue
        f.snapshot_tokens()
        db = f"agent-{cid}-db"
        f.cli("db", "create", db, agent=cid)
        w = f.popen("db", "watch", db, "-o", "jsonl", agent=cid, name=f"watch-{cid}")
        time.sleep(1.5 + k * 0.17)
        t0 = time.monotonic()
        rr = f.cli("token", "revoke", "--principal", f"agent-{cid}", "--yes", agent="42")
        t1 = time.monotonic()
        end = None
        while time.monotonic() - t1 < 10:
            if w.poll() is not None:
                end = time.monotonic()
                break
            time.sleep(0.005)
        if w.poll() is None:
            w.kill()
        trials.append({"revoke_ok": rr.ok, "revoke_err": rr.err.strip()[:200], "revoke_cli_s": round(t1 - t0, 4),
                       "stream_end_from_start_s": round(end - t0, 4) if end else None})
        f.cli("db", "drop", db, "--yes")
    ends = [t["stream_end_from_start_s"] for t in trials if t.get("stream_end_from_start_s")]
    out["revocation_on_data_node"] = {"trials": trials, "stream_end_ms": summary(ends)}
    m.close()
    out["leak_check"] = f.leak_check()
    for p in procs.values():
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(20)
            except subprocess.TimeoutExpired:
                p.kill()
    f.close()
    (a.run_dir / "result.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2)[:8000])


if __name__ == "__main__":
    main()
