"""A plain-HTTP echo server on host loopback for ex-endpoints.sh: it logs each request it
receives and sends the request's headers and body back, so a transcript shows what the
supervisor forwarded and whether a response can carry an injected credential back.

    python3 -I echo.py <port> <log file>"""
import http.server
import sys

LOG = sys.argv[2]


class Echo(http.server.BaseHTTPRequestHandler):
    def handle_one(self):
        n = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(n) if n else b""
        seen = f"{self.command} {self.path}\n{self.headers}{body[:400]!r}\n"
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(f"--- port {self.server.server_port}\n{seen}")
        out = ("echo received:\n" + seen).encode()
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    do_GET = do_POST = handle_one

    def log_message(self, *args):
        pass


http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), Echo).serve_forever()
