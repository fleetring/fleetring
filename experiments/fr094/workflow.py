#!/usr/bin/env python3
"""FR-094: one parent/child agent workflow, end to end, on 0.8's online delegation.

The task is the ticket's: triage this repository's open `flaky-test` issues and
propose fixes. A root agent (the parent, `42`) keeps its memory in
`agent-42-memory`, splits the issues into tasks, and starts children through
the harness (this script), never through MCP:

1. the harness delegates `42.cN` with `fleetring agent delegate`, narrower
   scopes, and a short TTL;
2. the child forks the parent's memory with `db_fork` into
   `agent-42.cN-memory`, through its own `fleetring mcp --agent 42.cN`;
3. it works in its own databases, and is refused outside them;
4. the parent reads each child's memory and copies back what it wants, row by
   row, then drops the child's databases;
5. the harness removes the child with `fleetring agent remove`.

Also: a grandchild (`42.c1.r1`) delegated by `42.c1`'s harness; the parent
renewing `42.c2` mid-task; a child (`42.c4`) abandoned and revoked mid-task
with an open `db watch` stream; a fork cancelled with `db drop` while seeding;
optionally the dev node restarted mid-workflow (`--restart`); and at the end
the root removing itself while a last child (`42.tail`) still works.

The agents' reasoning is scripted (a keyword triage of each issue's text); what
the experiment measures is what a model-driven harness does identically: CLI
calls, MCP tool calls, their latency, their errors, and their round trips.

Usage: workflow.py --run-dir DIR --port PORT [--restart] [--children N]
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from pathlib import Path

from harness import Fleet, du, fresh, summary

HERE = Path(__file__).resolve().parent
ROOT = "42"
PARENT_MEMORY = f"agent-{ROOT}-memory"

TRIAGE = [
    ("port", "port", "Bind port 0 and pass the bound listener to the node instead of choosing a free port and releasing it."),
    ("tracing", "global-state", "Capture audit events with a per-test dispatcher (`tracing::subscriber::with_default`) on the test's own runtime thread, not a thread-local default."),
    ("tokio_unstable", "cfg", "Gate the row on `cfg(tokio_unstable)` or drive the drain with a deterministic signal instead of runtime internals."),
    ("transport error", "retry", "Treat a transport error from a node that is restarting as retryable in the test client, and wait for membership before asserting."),
    ("never became live", "deadline", "Wait for the members' liveness with an explicit condition and a deadline that scales under parallel runs."),
    ("times out", "deadline", "Replace the fixed deadline with waiting on the condition, and run the test in the serial group under a full parallel run."),
    ("lags", "race", "Poll until the chain catches up with what `verify` reads, rather than asserting once after the write."),
    ("under load", "deadline", "Wait for the replicas' deletion with an explicit condition instead of a fixed sleep."),
]

DENIED_PROBES = [
    # (tool, arguments builder, what it checks)
    ("db_create", lambda cid, sib: {"name": f"agent-{ROOT}-evil"}, "create in the root's own prefix"),
    ("exec", lambda cid, sib: {"database": PARENT_MEMORY, "sql": "DELETE FROM tasks"}, "write the parent's memory (fork scope only)"),
    ("query", lambda cid, sib: {"database": f"agent-{sib}-memory", "sql": "SELECT 1"}, "read a sibling's memory"),
    ("db_drop", lambda cid, sib: {"name": PARENT_MEMORY, "confirm": PARENT_MEMORY}, "drop the parent's memory"),
    ("db_create", lambda cid, sib: {"name": "agent-43-memory"}, "create in another root's namespace"),
]


def triage(issue: dict) -> tuple[str, str]:
    text = (issue["title"] + " " + issue["excerpt"]).lower()
    for needle, category, proposal in TRIAGE:
        if needle.lower() in text:
            return category, proposal
    return "unknown", "Reproduce in a loop under `--test-threads=1` with logs, then bisect."


class Run:
    def __init__(self, fleet: Fleet, restart: bool, children: int) -> None:
        self.fleet = fleet
        self.restart = restart
        self.n_children = children
        self.steps: list[dict] = []
        self.child_reports: dict[str, dict] = {}
        self.events: dict[str, threading.Event] = {}
        self.issues = json.loads((HERE / "flaky-issues.json").read_text())
        self.measures: dict = {}

    # A step: wall time, and the control plane's requests by method between its two scrapes.
    def step(self, name: str, fn, **notes):
        before = self.fleet.metrics()
        started = time.monotonic()
        out = fn()
        wall = time.monotonic() - started
        after = self.fleet.metrics()
        entry = {"step": name, "wall_s": round(wall, 3), "control_rpcs": Fleet.delta(before, after),
                 "data_rpcs": Fleet.delta(before, after, "data"), **notes}
        self.steps.append(entry)
        print(f"  {name}: {wall:.3f}s {entry['control_rpcs']}", flush=True)
        return out

    def delegate(self, child: str, *scopes: str, ttl: str = "20m") -> dict:
        args = ["agent", "delegate", child, "--ttl", ttl, "-o", "json"]
        for s in scopes:
            args += ["--scope", s]
        r = self.fleet.cli(*args, note=f"delegate {child}")
        self.fleet.snapshot_tokens()
        if not r.ok:
            raise RuntimeError(f"delegate {child}: {r.err}")
        return r.json()["agents"][0]

    # ----- the parent ---------------------------------------------------------------------

    def parent_setup(self) -> None:
        f = self.fleet
        self.step("start the dev node", f.start)
        r = self.step("agent add 42 (root)", lambda: f.cli("agent", "add", ROOT, note="root"))
        assert r.ok, r.err
        f.snapshot_tokens()
        self.parent = self.step("parent: mcp --agent 42 starts", lambda: f.mcp(ROOT))
        p = self.parent

        def memory():
            p.call("db_create", {"name": PARENT_MEMORY, "labels": {"purpose": "memory", "agent": ROOT},
                                 "description": "Long-term memory for agent 42.\nSchema v1: tasks (one per flaky-test issue), proposals (number, child), facts."})
            for sql in [
                "CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, number INTEGER NOT NULL, title TEXT NOT NULL, excerpt TEXT NOT NULL, assignee TEXT, state TEXT NOT NULL DEFAULT 'todo')",
                "CREATE TABLE IF NOT EXISTS proposals (number INTEGER NOT NULL, child TEXT NOT NULL, category TEXT NOT NULL, proposal TEXT NOT NULL, at INTEGER NOT NULL, PRIMARY KEY (number, child))",
                "CREATE TABLE IF NOT EXISTS facts (subject TEXT NOT NULL, predicate TEXT NOT NULL, object TEXT NOT NULL, PRIMARY KEY (subject, predicate))",
            ]:
                p.call("exec", {"database": PARENT_MEMORY, "sql": sql})
            kids = [f"{ROOT}.c{i + 1}" for i in range(self.n_children)]
            for i, issue in enumerate(self.issues):
                p.call("exec", {"database": PARENT_MEMORY,
                                "sql": "INSERT INTO tasks (id, number, title, excerpt, assignee) VALUES (?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING",
                                "params": [f"issue-{issue['number']}", issue["number"], issue["title"], issue["excerpt"], kids[i % len(kids)]]})
            p.call("exec", {"database": PARENT_MEMORY, "sql": "INSERT INTO facts VALUES ('run', 'task', 'triage flaky-test issues') ON CONFLICT DO NOTHING"})

        self.step("parent: memory, schema, 8 tasks (MCP)", memory)

    # ----- a child ------------------------------------------------------------------------

    def child(self, cid: str, sibling: str) -> None:
        f = self.fleet
        rep = {"agent": cid, "mcp_restarts": 0, "refusals": [], "retries": 0, "errors_seen": [], "calls": 0}
        self.child_reports[cid] = rep
        started = time.monotonic()
        holder = {"m": f.mcp(cid)}
        rep["mcp_start_s"] = round(time.monotonic() - started, 3)

        def call(tool, args, expect_ok=True, note=""):
            deadline = time.monotonic() + 90
            while True:
                rep["calls"] += 1
                ok, body, secs = holder["m"].call(tool, args, note=note, expect_ok=expect_ok)
                if ok or expect_ok is False:
                    return ok, body
                err = body.get("error", {})
                rep["errors_seen"].append({"tool": tool, "code": err.get("code"), "message": err.get("message"),
                                           "hint": err.get("hint"), "retryable": err.get("retryable")})
                if err.get("code") == "unauthenticated":
                    # What a harness must do after `agent renew`: the running server keeps the old token.
                    rep["mcp_restarts"] += 1
                    holder["m"].close()
                    holder["m"] = f.mcp(cid)
                    if rep["mcp_restarts"] > 3:
                        return ok, body
                    continue
                if (err.get("retryable") or err.get("code") == "harness") and time.monotonic() < deadline:
                    rep["retries"] += 1
                    if err.get("code") == "harness":
                        holder["m"].close()
                        holder["m"] = f.mcp(cid)
                        rep["mcp_restarts"] += 1
                    time.sleep(0.5)
                    continue
                return ok, body

        mem = f"agent-{cid}-memory"
        t = time.monotonic()
        ok, body = call("db_fork", {"source": PARENT_MEMORY, "name": mem,
                                    "labels": {"purpose": "memory", "agent": cid, "parent": PARENT_MEMORY},
                                    "description": f"Working memory for {cid}, forked from {PARENT_MEMORY}."})
        rep["fork_s"] = round(time.monotonic() - t, 3)
        rep["fork_ok"] = ok
        self.events[f"{cid}:forked"].set()

        ok, body = call("query", {"database": mem, "sql": "SELECT number, title, excerpt FROM tasks WHERE assignee = ? ORDER BY number", "params": [cid]})
        tasks = body.get("rows", []) if ok else []
        rep["tasks"] = [row[0] for row in tasks]

        scratch = f"agent-{cid}-scratch"
        call("db_create", {"name": scratch, "expires_in": "1h", "labels": {"purpose": "scratch", "agent": cid}})
        call("exec", {"database": scratch, "sql": "CREATE TABLE notes (number INTEGER PRIMARY KEY, words INTEGER)"})
        call("changes", {"database": mem})  # its own stream scope works through MCP

        for number, title, excerpt in tasks:
            category, proposal = triage({"title": title, "excerpt": excerpt})
            call("exec", {"database": scratch, "sql": "INSERT INTO notes VALUES (?, ?) ON CONFLICT DO NOTHING",
                          "params": [number, len(excerpt.split())]})
            call("exec", {"database": mem, "sql": "INSERT INTO proposals VALUES (?, ?, ?, ?, ?) ON CONFLICT (number, child) DO UPDATE SET proposal = excluded.proposal",
                          "params": [number, cid, category, proposal, int(time.time() * 1000)]})
            call("exec", {"database": mem, "sql": "UPDATE tasks SET state = 'done' WHERE number = ?", "params": [number]})
            time.sleep(0.3)  # a model thinks between calls

        for tool, build, what in DENIED_PROBES:
            ok, body = call(tool, build(cid, sibling), expect_ok=False, note=f"probe: {what}")
            err = body.get("error", {})
            rep["refusals"].append({"probe": what, "refused": not ok, "code": err.get("code"), "message": err.get("message"),
                                    "hint": err.get("hint")})

        if cid == f"{ROOT}.c1":
            self.grandchild(cid, rep)

        # Waits for the parent's renewal, when this child is the one renewed, then works on.
        if f"{cid}:renewed" in self.events:
            self.events[f"{cid}:renewed"].wait(60)
            call("exec", {"database": mem, "sql": "INSERT INTO proposals VALUES (0, ?, 'note', 'after renewal', 0) ON CONFLICT DO NOTHING", "params": [cid]},
                 note="first call after the parent renewed this child")

        call("db_drop", {"name": scratch, "confirm": scratch})
        rep["wall_s"] = round(time.monotonic() - started, 3)
        holder["m"].close()
        self.events[f"{cid}:done"].set()

    def grandchild(self, cid: str, rep: dict) -> None:
        f = self.fleet
        gid = f"{cid}.r1"
        before = f.metrics()
        t = time.monotonic()
        info = self.delegate(gid, ttl="10m")
        rep["grandchild_delegate_s"] = round(time.monotonic() - t, 3)
        g = f.mcp(gid)
        db = f"agent-{gid}-scratch"
        g.call("db_create", {"name": db, "expires_in": "1h"})
        g.call("exec", {"database": db, "sql": "CREATE TABLE t (x)"})
        g.call("exec", {"database": db, "sql": "INSERT INTO t VALUES (1)"})
        ok, body, _ = g.call("query", {"database": db, "sql": "SELECT count(*) FROM t"})
        probes = []
        for tool, args, what in [
            ("db_create", {"name": f"agent-{cid}-x"}, "create in its parent's prefix"),
            ("db_fork", {"source": PARENT_MEMORY, "name": f"agent-{gid}-memory"}, "fork the root's memory without fork scope"),
        ]:
            ok2, body2, _ = g.call(tool, args, note=f"probe: {what}", expect_ok=False)
            probes.append({"probe": what, "refused": not ok2, "message": body2.get("error", {}).get("message")})
        g.close()
        r = f.cli("agent", "remove", gid, "--yes", note="the child removes its grandchild")
        # Revoking an agent leaves its databases: the child cleans up after its grandchild.
        d = f.cli("db", "drop", db, "--yes", agent=cid, note="the child drops its grandchild's database")
        rep["grandchild"] = {"id": gid, "scopes": info["scopes"], "query_ok": ok, "probes": probes,
                             "remove_ok": r.ok, "remove_out": r.out.strip(), "drop_by_child_ok": d.ok,
                             "control_rpcs": Fleet.delta(before, f.metrics())}

    # ----- the abandoned child --------------------------------------------------------------

    def abandoned(self) -> None:
        f = self.fleet
        cid = f"{ROOT}.c4"
        self.step("delegate 42.c4 (to abandon)", lambda: self.delegate(cid, ttl="15m"))
        m = f.mcp(cid)
        inbox = f"agent-{cid}-inbox"
        m.call("db_create", {"name": inbox, "expires_in": "1h"})
        m.call("exec", {"database": inbox, "sql": "CREATE TABLE msgs (id INTEGER PRIMARY KEY, body TEXT)"})
        watch = f.popen("db", "watch", inbox, "-o", "jsonl", agent=cid, name="watch-c4")
        time.sleep(1.5)
        stop = threading.Event()
        first_refusal: dict = {}

        def busy():
            i = 0
            while not stop.is_set():
                i += 1
                ok, body, _ = m.call("exec", {"database": inbox, "sql": "INSERT INTO msgs (body) VALUES (?)", "params": [f"step {i}"]},
                                     expect_ok=None)
                if not ok and "t" not in first_refusal:
                    first_refusal["t"] = time.monotonic()
                    first_refusal["error"] = body.get("error")
                    return
                time.sleep(0.05)

        worker = threading.Thread(target=busy)
        worker.start()
        time.sleep(2)
        t0 = time.monotonic()
        r = f.cli("agent", "remove", cid, "--yes", note="the parent revokes an abandoned child mid-task")
        t1 = time.monotonic()
        stream_end = None
        while time.monotonic() - t1 < 15:
            if watch.poll() is not None:
                stream_end = time.monotonic()
                break
            time.sleep(0.02)
        worker.join(15)
        stop.set()
        cli = f.cli("db", "query", inbox, "SELECT count(*) FROM msgs", agent=cid, note="the revoked child's CLI")
        m.close()
        if watch.poll() is None:
            watch.kill()
        rows = f.cli("db", "query", inbox, "SELECT count(*) FROM msgs", agent=ROOT).out.strip().splitlines()[-1:]
        f.cli("db", "drop", inbox, "--yes", agent=ROOT, note="the parent drops the abandoned child's database")
        self.measures["abandoned"] = {
            "remove_cli_s": round(t1 - t0, 3), "remove_ok": r.ok, "remove_out": r.out.strip(),
            "mcp_refused_after_remove_returned_s": round(first_refusal["t"] - t1, 3) if "t" in first_refusal else None,
            "mcp_refusal": first_refusal.get("error"),
            "stream_ended_after_remove_returned_s": round(stream_end - t1, 3) if stream_end else None,
            "stream_exit_code": watch.returncode,
            "stream_stderr": (f.run_dir / "watch-c4.stderr").read_text().strip()[-400:],
            "cli_after_remove": {"code": cli.code, "stderr": cli.err.strip()},
            "rows_written_before_refusal": rows,
            "identity_dir_left": (f.agents_dir / cid).exists(),
        }

    # ----- a fork cancelled while seeding ---------------------------------------------------

    def fork_cancel(self) -> None:
        f = self.fleet
        big = f"agent-{ROOT}-big"
        f.cli("db", "create", big, agent=ROOT)
        f.cli("db", "exec", big, "CREATE TABLE blobs (id INTEGER PRIMARY KEY, b BLOB)", agent=ROOT)
        for chunk in range(4):  # 4 x 4,000 x 4 KiB = 64 MiB
            lo = chunk * 4000
            f.cli("db", "exec", big, f"WITH RECURSIVE c(x) AS (SELECT {lo + 1} UNION ALL SELECT x + 1 FROM c WHERE x < {lo + 4000}) INSERT INTO blobs SELECT x, randomblob(4096) FROM c", agent=ROOT)
        disk_before = du(f.dev_dir / "data")
        target = f"agent-{ROOT}-bigfork"
        t0 = time.monotonic()
        r = f.cli("db", "fork", big, target, "--detach", "-o", "json", agent=ROOT, note="start a fork to cancel")
        op = r.json().get("operation_id") if r.ok else None
        show = f.cli("db", "show", target, "-o", "json", agent=ROOT)
        state_at_drop = json.loads(show.out).get("fork", {}).get("state") if show.ok else None
        d = f.cli("db", "drop", target, "--yes", agent=ROOT, note="cancel the fork while it seeds")
        t1 = time.monotonic()
        again = f.cli("db", "fork", big, target, "--operation-id", op, "-o", "json", agent=ROOT, note="resume the cancelled fork") if op else None
        time.sleep(3)
        disk_after = du(f.dev_dir / "data")
        orphans = f.cli("orphan", "list", "-o", "json", note="as the dev admin")
        self.measures["fork_cancel"] = {
            "source_bytes_on_disk": disk_before, "state_when_dropped": state_at_drop, "drop_ok": d.ok,
            "detach_to_drop_s": round(t1 - t0, 3), "drop_out": d.out.strip(), "drop_err": d.err.strip(),
            "resume_code": again.code if again else None,
            "resume_state": (json.loads(again.out).get("state") if again and again.out.strip().startswith("{") else (again.err.strip() if again else None)),
            "disk_after_cancel": disk_after, "orphans": orphans.out.strip()[:600],
        }
        f.cli("db", "drop", big, "--yes", agent=ROOT)

    # ----- the root removes itself while a child works ------------------------------------------

    def root_removes_itself(self) -> None:
        f = self.fleet
        cid = f"{ROOT}.tail"
        self.delegate(cid, ttl="10m")
        m = f.mcp(cid)
        db = f"agent-{cid}-work"
        m.call("db_create", {"name": db, "expires_in": "1h"})
        m.call("exec", {"database": db, "sql": "CREATE TABLE t (x)"})
        watch = f.popen("db", "watch", db, "-o", "jsonl", agent=cid, name="watch-tail")
        time.sleep(1.5)
        t0 = time.monotonic()
        r = f.cli("agent", "remove", ROOT, "--yes", note="the root removes itself")
        t1 = time.monotonic()
        stream_end = None
        while time.monotonic() - t1 < 15:
            if watch.poll() is not None:
                stream_end = time.monotonic()
                break
            time.sleep(0.02)
        ok, body, _ = m.call("query", {"database": db, "sql": "SELECT 1"}, expect_ok=False, note="the tail child after its root left")
        m.close()
        if watch.poll() is None:
            watch.kill()
        child_cli = f.cli("db", "list", "-o", "json", agent=cid, note="the tail child's CLI after its root left")
        adm = f.cli("db", "drop", db, "--yes", note="the dev admin drops what the departed lineage left")
        self.measures["root_self_removal"] = {
            "remove_cli_s": round(t1 - t0, 3), "remove_ok": r.ok, "remove_out": r.out.strip(), "remove_err": r.err.strip(),
            "child_mcp_refused": not ok, "child_mcp_error": body.get("error"),
            "child_stream_ended_after_s": round(stream_end - t1, 3) if stream_end else None,
            "child_stream_stderr": (f.run_dir / "watch-tail.stderr").read_text().strip()[-300:],
            "child_cli": {"code": child_cli.code, "stderr": child_cli.err.strip()[:300]},
            "child_identity_dir_left": (f.agents_dir / cid).exists(),
            "leftover_db_dropped_by_admin": adm.ok,
        }

    # ----- the whole run ------------------------------------------------------------------

    def run(self) -> dict:
        f = self.fleet
        wall0 = time.monotonic()
        self.parent_setup()
        kids = [f"{ROOT}.c{i + 1}" for i in range(self.n_children)]
        for k in kids:
            for e in ("forked", "done"):
                self.events[f"{k}:{e}"] = threading.Event()
        self.events[f"{ROOT}.c2:renewed"] = threading.Event()

        # 1. The harness delegates each child: its own prefix, fork on the parent's memory, 20 minutes.
        for k in kids:
            extra = [f"grant:agent-{k}.*", f"admin:agent-{k}.*"] if k.endswith(".c1") else []
            self.step(f"delegate {k}", lambda k=k, extra=extra: self.delegate(
                k, f"admin:agent-{k}-*", f"stream:agent-{k}-*", f"fork:{PARENT_MEMORY}", *extra))

        # 2-3. Children start their MCP servers and work, in parallel.
        threads = [threading.Thread(target=self.child, args=(k, kids[(i + 1) % len(kids)])) for i, k in enumerate(kids)]
        before = f.metrics()
        t_children = time.monotonic()
        for t in threads:
            t.start()

        # The parent renews c2 once c2 has forked, mid-task.
        self.events[f"{ROOT}.c2:forked"].wait(60)
        r = self.step("parent renews 42.c2 mid-task", lambda: f.cli("agent", "renew", f"{ROOT}.c2", note="renew mid-task"))
        f.snapshot_tokens()
        self.measures["renew"] = {"ok": r.ok, "out": r.out.strip(), "err": r.err.strip()}
        self.events[f"{ROOT}.c2:renewed"].set()

        if self.restart:
            time.sleep(0.5)
            t = time.monotonic()
            stop_s = f.stop()
            # While the node is down, the harness tries to delegate a child: the CLI resends on UNAVAILABLE.
            delegate_during = {}

            def try_delegate():
                t0 = time.monotonic()
                rr = f.cli("agent", "delegate", f"{ROOT}.late", "--ttl", "5m", "-o", "json", note="delegate while the control node is down")
                delegate_during.update({"code": rr.code, "secs": round(time.monotonic() - t0, 3), "stderr": rr.err.strip()[-500:]})

            dt = threading.Thread(target=try_delegate)
            dt.start()
            time.sleep(2)
            start_s = f.start()
            dt.join(90)
            f.snapshot_tokens()
            self.measures["restart"] = {"stop_s": round(stop_s, 3), "start_s": round(start_s, 3),
                                        "outage_s": round(time.monotonic() - t, 3), "delegate_during_outage": delegate_during}
            if delegate_during.get("code") == 0:
                f.cli("agent", "remove", f"{ROOT}.late", "--yes")

        for t in threads:
            t.join(300)
        after = f.metrics()
        self.steps.append({"step": f"{len(kids)} children work in parallel (MCP)", "wall_s": round(time.monotonic() - t_children, 3),
                           "control_rpcs": Fleet.delta(before, after), "data_rpcs": Fleet.delta(before, after, "data")})

        # 4. The parent reads each child's memory and copies back what it wants, row by row.
        p = self.parent

        def copy_back():
            copied = 0
            for k in kids:
                ok, body, _ = p.call("query", {"database": f"agent-{k}-memory", "sql": "SELECT number, child, category, proposal, at FROM proposals WHERE number > 0 ORDER BY number"})
                for row in body.get("rows", []) if ok else []:
                    p.call("exec", {"database": PARENT_MEMORY, "sql": "INSERT INTO proposals VALUES (?, ?, ?, ?, ?) ON CONFLICT (number, child) DO UPDATE SET proposal = excluded.proposal", "params": row})
                    p.call("exec", {"database": PARENT_MEMORY, "sql": "UPDATE tasks SET state = 'done' WHERE number = ?", "params": [row[0]]})
                    copied += 1
                db = f"agent-{k}-memory"
                p.call("db_drop", {"name": db, "confirm": db})
            return copied

        copied = self.step("parent copies back row by row, drops children's memories (MCP)", copy_back)
        ok, body, _ = p.call("query", {"database": PARENT_MEMORY, "sql": "SELECT state, count(*) FROM tasks GROUP BY state"})
        self.measures["copy_back"] = {"rows": copied, "tasks_by_state": body.get("rows")}

        # 5. The harness removes each child.
        for k in kids:
            self.step(f"agent remove {k}", lambda k=k: f.cli("agent", "remove", k, "--yes", note="remove a finished child"))

        self.abandoned()
        self.fork_cancel()
        p.close()
        self.root_removes_itself()
        self.measures["leak_check"] = f.leak_check()
        f.stop()
        return {"wall_s": round(time.monotonic() - wall0, 3), "steps": self.steps, "children": self.child_reports,
                "measures": self.measures, "cli_calls": f.cli_calls,
                "errors": f.errors}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--port", type=int, default=7470)
    ap.add_argument("--restart", action="store_true")
    ap.add_argument("--children", type=int, default=3)
    a = ap.parse_args()
    fresh(a.run_dir)
    fleet = Fleet(a.run_dir, a.port)
    try:
        result = Run(fleet, a.restart, a.children).run()
    finally:
        try:
            fleet.stop()
        except Exception:
            pass
        fleet.close()
    (a.run_dir / "result.json").write_text(json.dumps(result, indent=2))
    print(json.dumps({k: result[k] for k in ("wall_s", "cli_calls")}, indent=2))
    print(json.dumps(result["measures"], indent=2))


if __name__ == "__main__":
    main()
