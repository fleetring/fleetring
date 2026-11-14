"""Shared plumbing for the FR-094 parent/child experiment.

A `Fleet` is one isolated dev node: its own HOME (so its `~/.fleetring`, its
contexts, and its agents directory never touch the operator's), its own port,
and a transcript of every CLI invocation and MCP call the harness made. The
harness reads token files only to check, at the end, that no token text leaked
into a transcript, an MCP server's stderr, or the node's log; it never writes a
token anywhere.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import statistics
import subprocess
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

from mcp_client import McpClient

REPO = Path(__file__).resolve().parents[2]
FLEETRING = os.environ.get("FLEETRING_BIN", str(REPO / "target" / "release" / "fleetring"))


@dataclass
class Result:
    args: list[str]
    code: int
    out: str
    err: str
    secs: float

    @property
    def ok(self) -> bool:
        return self.code == 0

    def json(self) -> dict:
        return json.loads(self.out)


@dataclass
class Fleet:
    run_dir: Path
    port: int
    cli_calls: int = 0
    errors: list[dict] = field(default_factory=list)
    tokens: dict[str, str] = field(default_factory=dict)  # token id -> sha256 of the token text
    _token_texts: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.home = self.run_dir / "home"
        self.home.mkdir(exist_ok=True)
        self.env = {k: v for k, v in os.environ.items() if not k.startswith(("FLEETRING_", "XDG_CONFIG", "XDG_DATA_HOME"))}
        self.env["HOME"] = str(self.home)
        self.env["NO_COLOR"] = "1"
        self.transcript = open(self.run_dir / "transcript.jsonl", "a")
        self.dev_dir = self.home / ".fleetring"
        self.agents_dir = self.dev_dir / "agents"
        self.mcp_clients: list[McpClient] = []

    # ----- the CLI -------------------------------------------------------------------------

    def cli(self, *args: str, agent: str | None = None, timeout: float = 120, stdin: str | None = None,
            note: str = "") -> Result:
        argv = [FLEETRING, *with_agent(list(args), agent)]
        started = time.monotonic()
        try:
            proc = subprocess.run(argv, capture_output=True, text=True, env=self.env, timeout=timeout,
                                  input=stdin, cwd=self.run_dir)
            code, out, err = proc.returncode, proc.stdout, proc.stderr
        except subprocess.TimeoutExpired as exc:
            code, out, err = 124, exc.stdout or "", (exc.stderr or "") + "\n[harness: timed out]"
            if isinstance(out, bytes):
                out = out.decode()
            if isinstance(err, bytes):
                err = err.decode()
        secs = time.monotonic() - started
        self.cli_calls += 1
        result = Result(argv[1:], code, out, err, secs)
        self.log({"kind": "cli", "args": argv[1:], "code": code, "secs": round(secs, 4), "stdout": out,
                  "stderr": err, "note": note})
        if code != 0:
            self.errors.append({"where": "cli", "args": argv[1:], "code": code, "stderr": err.strip(), "note": note})
        return result

    def popen(self, *args: str, agent: str | None = None, name: str) -> subprocess.Popen:
        argv = [FLEETRING, *with_agent(list(args), agent)]
        self.log({"kind": "spawn", "args": argv[1:], "name": name})
        out = open(self.run_dir / f"{name}.stdout", "ab")
        err = open(self.run_dir / f"{name}.stderr", "ab")
        return subprocess.Popen(argv, stdout=out, stderr=err, env=self.env, cwd=self.run_dir)

    def log(self, entry: dict) -> None:
        entry["t"] = time.time()
        self.transcript.write(json.dumps(entry) + "\n")
        self.transcript.flush()

    # ----- the node ------------------------------------------------------------------------

    def start(self) -> float:
        started = time.monotonic()
        r = self.cli("dev", "-d", "--port", str(self.port), note="start the dev node")
        if not r.ok:
            raise RuntimeError(f"dev -d failed: {r.err}")
        return time.monotonic() - started

    def stop(self) -> float:
        started = time.monotonic()
        self.cli("dev", "stop", note="stop the dev node")
        return time.monotonic() - started

    def metrics(self) -> dict[str, float]:
        """The node's per-method RPC counters and duration sums, keyed `service/method/code`."""
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port + 1}/metrics", timeout=5) as resp:
                text = resp.read().decode()
        except OSError:
            return {}
        out: dict[str, float] = {}
        for line in text.splitlines():
            m = re.match(r'fleetring_rpc_requests_total\{service="(\w+)",method="(\w+)",code="(\w+)"\} (\S+)', line)
            if m:
                out[f"{m[1]}/{m[2]}/{m[3]}"] = float(m[4])
                continue
            m = re.match(r'fleetring_rpc_duration_seconds_sum\{service="(\w+)",method="(\w+)"\} (\S+)', line)
            if m:
                out[f"dur/{m[1]}/{m[2]}"] = float(m[3])
        return out

    @staticmethod
    def delta(before: dict[str, float], after: dict[str, float], service: str = "control") -> dict[str, int]:
        """Requests per `method/code` between two scrapes, for one service."""
        d: dict[str, int] = {}
        for key, value in after.items():
            if not key.startswith(service + "/"):
                continue
            n = int(value - before.get(key, 0))
            if n:
                d[key.split("/", 1)[1]] = n
        return d

    # ----- MCP -----------------------------------------------------------------------------

    def mcp(self, agent: str, *extra: str, context: str = "dev") -> "LoggedMcp":
        client = McpClient(FLEETRING, agent, self.env, self.run_dir / f"mcp-{agent}.stderr", list(extra), context)
        self.mcp_clients.append(client)
        self.log({"kind": "mcp-start", "agent": agent, "server": client.server_info.get("serverInfo")})
        return LoggedMcp(self, client)

    # ----- tokens, only for the leak check ----------------------------------------------------

    def snapshot_tokens(self) -> None:
        """Remembers every token file's text and ID now, so a later leak check can find them."""
        files = list(self.agents_dir.glob("*/token")) + list((self.home / ".local/share/fleetring/agents").glob("*/*/token"))
        for token_file in files:
            text = token_file.read_text().strip()
            if not text or text in self._token_texts:
                continue
            self._token_texts.add(text)
            ident = token_file.parent / "identity.toml"
            token_id = ""
            if ident.exists():
                m = re.search(r'^token_id\s*=\s*"([^"]+)"', ident.read_text(), re.M)
                token_id = m[1] if m else ""
            self.tokens[token_id or token_file.parent.name] = hashlib.sha256(text.encode()).hexdigest()[:16]

    def leak_check(self) -> dict:
        """Greps every transcript, MCP stderr, CLI capture, and the node log for token text."""
        self.snapshot_tokens()
        files = [p for p in self.run_dir.glob("*") if p.is_file() and p.suffix in {".jsonl", ".stderr", ".stdout"}]
        log = self.dev_dir / "fleetring.log"
        if log.exists():
            files.append(log)
        hits: list[dict] = []
        id_mentions = 0
        for path in files:
            body = path.read_text(errors="replace")
            for text in self._token_texts:
                # The whole token, and its signature segment alone, in case of wrapping.
                for needle in (text, text.rsplit(".", 1)[-1]):
                    if len(needle) >= 32 and needle in body:
                        hits.append({"file": path.name, "needle": "token" if needle == text else "signature"})
            for token_id in self.tokens:
                id_mentions += body.count(token_id)
        return {"tokens_checked": len(self._token_texts), "files_checked": len(files), "token_text_hits": hits,
                "token_id_mentions": id_mentions}

    def close(self) -> None:
        for client in self.mcp_clients:
            client.close()
        self.transcript.close()


class LoggedMcp:
    """An MCP client whose every call goes into the fleet's transcript."""

    def __init__(self, fleet: Fleet, client: McpClient) -> None:
        self.fleet = fleet
        self.client = client
        self.agent = client.agent

    def call(self, tool: str, arguments: dict, note: str = "", expect_ok: bool | None = None) -> tuple[bool, dict, float]:
        started = time.monotonic()
        try:
            ok, body = self.client.call(tool, arguments)
        except Exception as exc:  # the server died or refused the request itself
            ok, body = False, {"error": {"code": "harness", "message": str(exc)}}
        secs = time.monotonic() - started
        self.fleet.log({"kind": "mcp", "agent": self.agent, "tool": tool, "arguments": arguments, "ok": ok,
                        "result": body, "secs": round(secs, 4), "note": note})
        if not ok:
            self.fleet.errors.append({"where": "mcp", "agent": self.agent, "tool": tool, "error": body.get("error", body),
                                      "note": note, "expected": expect_ok is False})
        return ok, body, secs

    def close(self) -> None:
        self.client.close()


def with_agent(args: list[str], agent: str | None) -> list[str]:
    """Puts `--agent` after the subcommand: `fleetring --agent 42 db ...` is refused (see the write-up)."""
    if agent is None:
        return args
    at = 2 if args and args[0] in {"db", "agent", "token", "orphan", "cert"} else 1
    return [*args[:at], "--agent", agent, *args[at:]]


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    k = (len(ordered) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def summary(values: list[float], scale: float = 1000.0) -> dict:
    """p50, p95, p99, mean, min, max, in milliseconds by default."""
    if not values:
        return {}
    return {"n": len(values), "p50": round(percentile(values, 50) * scale, 2),
            "p95": round(percentile(values, 95) * scale, 2), "p99": round(percentile(values, 99) * scale, 2),
            "mean": round(statistics.mean(values) * scale, 2), "min": round(min(values) * scale, 2),
            "max": round(max(values) * scale, 2)}


def du(path: Path) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.lstat(os.path.join(root, f)).st_blocks * 512
            except OSError:
                pass
    return total


def fresh(run_dir: Path) -> None:
    if run_dir.exists():
        shutil.rmtree(run_dir)
