"""A minimal MCP client over stdio, as an agent harness runs one (FR-094).

It starts `fleetring mcp --agent <id>`, performs the handshake, and calls tools. It
never sees a token: the server finds its own credentials, which is the point of
ADR-0047 point 7. Everything the server writes on stderr is kept, so the run can
grep it for token text afterwards.
"""

from __future__ import annotations

import json
import subprocess
import threading
import time
from pathlib import Path


class McpError(Exception):
    pass


class McpClient:
    def __init__(self, fleetring: str, agent: str, env: dict[str, str], stderr_path: Path,
                 extra_args: list[str] | None = None, context: str = "dev") -> None:
        self.agent = agent
        self.stderr_path = stderr_path
        self._stderr = open(stderr_path, "ab")
        args = [fleetring, "mcp", "--agent", agent, "--context", context, *(extra_args or [])]
        self.proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=self._stderr, env=env)
        self._next_id = 0
        self._lock = threading.Lock()
        self.round_trips = 0
        self._initialize()

    def _send(self, message: dict) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write((json.dumps(message) + "\n").encode())
        self.proc.stdin.flush()

    def _request(self, method: str, params: dict, timeout: float = 90.0) -> dict:
        with self._lock:
            self._next_id += 1
            rid = self._next_id
            self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
            assert self.proc.stdout is not None
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                line = self.proc.stdout.readline()
                if not line:
                    raise McpError(f"server for {self.agent} closed stdout (exit {self.proc.poll()})")
                msg = json.loads(line)
                if msg.get("id") == rid:
                    if "error" in msg:
                        raise McpError(json.dumps(msg["error"]))
                    return msg["result"]
            raise McpError(f"{method} timed out")

    def _initialize(self) -> None:
        self.server_info = self._request("initialize", {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "fr094-harness", "version": "0"},
        })
        self._send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def tools(self) -> list[dict]:
        return self._request("tools/list", {})["tools"]

    def call(self, tool: str, arguments: dict) -> tuple[bool, dict]:
        """Calls a tool; returns (ok, parsed JSON result or error object)."""
        self.round_trips += 1
        result = self._request("tools/call", {"name": tool, "arguments": arguments})
        text = "".join(c.get("text", "") for c in result.get("content", []) if c.get("type") == "text")
        try:
            body = json.loads(text) if text else result.get("structuredContent", {})
        except json.JSONDecodeError:
            body = {"raw": text}
        return (not result.get("isError", False), body)

    def close(self) -> None:
        try:
            if self.proc.stdin:
                self.proc.stdin.close()
            self.proc.wait(timeout=10)
        except Exception:
            self.proc.kill()
        self._stderr.close()
