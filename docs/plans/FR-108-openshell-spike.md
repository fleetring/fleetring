# FR-108 (#173): OpenShell as the agent launcher's sandbox backend (spike)

This spike asks one question: does ADR-0044 point 11.5's acceptance test pass with [OpenShell](https://docs.nvidia.com/openshell/latest/) as the sandbox backend? It builds no launcher and changes no FleetRing code, token, scope or wire. Every result below names the script that produced it and the transcript that holds its command and output, in [`FR-108/results/`](FR-108/results/); a claim without a transcript says so.

**Result.** On this Mac, every row passes on both OpenShell runtimes tested, Docker Desktop and the microVM driver, except row 4, which is the launcher's job and not the backend's: OpenShell offers no way to spawn anything from inside a sandbox. Every search the hostile agent ran was checked by a positive control planted in A, and each control was found. A still does its own work: Hrana reads and writes to its own database through the router, holding only a placeholder.

**Deviation from the ticket's setup.** The ticket says each sandbox mounts only its own work directory. The acceptance runs give each sandbox a labelled Docker volume of its own at `/sandbox/work` (on the microVM, a directory in the guest), not a host directory. Host bind mounts were tested separately: on Docker Desktop they were unusable inside the sandbox, and they let A plant symlinks that the host follows into B's directory (see "Work directories").

**What doesn't fail the test but shapes the launcher:** a credential rotated with an ordinary placeholder never reaches a process that is already running; detaching a provider is refused while a policy binds it, and re-attaching revives the old placeholder; the authority that verifies the router is trusted gateway-wide; there are no memory, CPU or disk limits unless the launcher sets them, and no disk quota at all on Docker; OpenShell's own "auto" approval widens network access with no person; and a sandbox's terminal output isn't sanitized. "Gaps and risks" lists all eleven.

**Recommendation: design the launcher against OpenShell as its first backend,** for both the `container` and `microvm` rows, with the upstream changes listed at the end, and accept that choice only after the same test passes on Linux. A draft amendment, [ADR-0048](../adr/0048-openshell-as-the-agent-launchers-sandbox-backend.md) (Proposed), records it. Nothing here makes any setup a secure multi-agent mode: that still needs the launcher and its credential proxy (ADR-0044 point 10).

## Environment and versions

| What | Version |
|---|---|
| Host | macOS 26.5, Apple silicon (arm64) |
| Docker | Docker Desktop 29.5.3; its Linux VM runs kernel 6.12.76-linuxkit, with 16 GiB of memory; host networking off (the default) |
| OpenShell | **0.1.8** (released 2026-11-10): `openshell`, `openshell-gateway` and `openshell-driver-vm` release tarballs for `aarch64-apple-darwin`, SHA-256 checked against the release's checksum files; images `ghcr.io/nvidia/openshell/supervisor:0.1.8` and `sandbox:0.1.8`, base `nvcr.io/nvidia/base/ubuntu:24.04` |
| microVM | the VM driver (libkrun on Hypervisor.framework), guest kernel 6.12.109, `mem_mib = 1024`; e2fsprogs 1.47.4 from Homebrew, which the driver needs (installed for the run, removed after) |
| FleetRing | `main` at 90c349d, `fleetring` built `--release` (the router is 0.8 work, FR-066) |
| Agent image | [`FR-108/Dockerfile`](FR-108/Dockerfile): the base plus curl, python3, sqlite3, tmux, netcat, dig and procps; user `agent`, 10001:10001, with no supplementary groups |

The OpenShell installer on macOS installs a Homebrew formula and starts a `brew services` gateway. The spike instead ran the same release binaries from a scratch directory, with `HOME` redirected for both the gateway and the CLI, so nothing outlived it. `OPENSHELL_TELEMETRY_ENABLED=false` was set on the gateway before it first started, and supervisors received it in their environment.

## Setup

The scripts are the procedure; every transcript in `results/` came from them:

| Script | What it does | Transcript |
|---|---|---|
| [`setup.sh`](FR-108/setup.sh) | the dev fleet; three routers; the Docker gateway; the agent image; the profiles; providers, work volumes, policies and sandboxes for A and B; B's canaries | none (setup) |
| [`ex-rows.sh`](FR-108/ex-rows.sh) | the acceptance rows on one driver: B's positive controls, `own-work.sh` and `hostile-agent.sh` in A, replay from outside, the router's own scope check, the containers' configuration, the supervisor's log | `rows-docker.txt`, `rows-vm.txt` |
| [`ex-endpoints.sh`](FR-108/ex-endpoints.sh) | placeholders at other endpoints, reflection, TLS as the agent sees it, a wrong-name certificate, raw HTTP framing (`http-layer.sh`), the host-alias topology | `endpoints.txt` |
| [`ex-tls-noca.sh`](FR-108/ex-tls-noca.sh) | upstream verification without the fleet's authority | `tls-noca.txt` |
| [`ex-dns.sh`](FR-108/ex-dns.sh) | DNS from A, with tcpdump in the supervisor's network namespace | `dns.txt` |
| [`ex-rotation.sh`](FR-108/ex-rotation.sh) | rotation, expiry, detach and `stable_placeholder`, against running processes | `rotation.txt`, `rotation-logs/` |
| [`ex-approval.sh`](FR-108/ex-approval.sh) | OpenShell's approval modes | `approval.txt` |
| [`ex-workdir.sh`](FR-108/ex-workdir.sh) | volumes, copy in and out, bind mounts | `workdir.txt` |
| [`ex-resources.sh`](FR-108/ex-resources.sh) | memory, processes and disk on Docker | `resources.txt` |
| [`ex-tty.sh`](FR-108/ex-tty.sh) | a real terminal in A | `tty.txt` |
| [`ex-trusted-base.sh`](FR-108/ex-trusted-base.sh) | what the gateway keeps on the host | `trusted-base.txt` |
| [`setup-vm.sh`](FR-108/setup-vm.sh), [`ex-vm.sh`](FR-108/ex-vm.sh) | the microVM gateway and sandboxes; what differs there | `vm.txt` (and `rows-vm.txt`) |

Not scripted: downloading and checking the OpenShell tarballs, installing and removing e2fsprogs, and the final cleanup. Every transcript went through [`scrub.py`](FR-108/scrub.py), which replaces tokens, this machine's address (shown as `192.0.2.10`), paths, user and host names, and truncates result rows past 1,500 characters.

What `setup.sh` builds:

1. **A dev fleet** in its own directory, on port 7410, with a `[[principal]] router` entry (`scopes = []`, `forward_tokens = true`), and agents `a` and `b` minted by `fleetring agent add` (principals `agent-a` and `agent-b`, scoped `admin:agent-<id>-*` and `stream:agent-<id>-*`). B's database holds the canary row `b-private-plan`.
2. **Routers** (ADR-0037), each with TLS from the dev fleet's authority: the one the sandboxes use on the Mac's LAN address (`192.0.2.10:8443` in the transcripts), the same fleet on `127.0.0.1:8444` for the host-alias topology, and one on `:8446` whose certificate the authority issued for the wrong name.
3. **The OpenShell gateway** with the Docker driver in bridge mode (host mode needs Docker Desktop's host networking), `allow_driver_config = true` for the work volumes (resource admission stays on), the fleet's authority certificate as `[openshell.supervisor.network] additional_ca_bundle`, and proposal approval pinned to `manual` gateway-wide.
4. **Two provider profiles:** `fleetring-router`, endpointless, declaring one credential, `FLEETRING_TOKEN`, sent as `Authorization: Bearer`; and `fleetring-router-stable`, the same with `stable_placeholder: true`.
5. **Per agent:** a provider holding that agent's token (read from the environment, never the command line); a labelled Docker volume `fr108-work-<id>`, chowned to 10001 (a new volume is root's); a policy rendered from [`policy.template.yaml`](FR-108/policy.template.yaml); and a sandbox with that provider, policy and volume at `/sandbox/work`. No client certificate is inside either sandbox.
6. **B's canaries,** written inside B: `/sandbox/work/.fleetring/agents/b/client.agent-b.key` and `token`, `/sandbox/work/b-local.sqlite`, and a listening Unix socket `/tmp/b-mux.sock` standing in for a multiplexer's control socket. (tmux can't start a session in a sandbox: the first pass saw "create window failed: fork failed: Permission denied", since `/dev/pts` isn't in the policy; observed, transcript not kept.)

### The provider profile

```yaml
id: fleetring-router
display_name: FleetRing router
description: A FleetRing token, resolved only on requests to the router a sandbox policy binds.
category: data
credentials:
  - name: fleetring_token
    description: The agent's FleetRing token, sent as Authorization Bearer.
    env_vars: [FLEETRING_TOKEN]
    required: true
    auth_style: bearer
    header_name: authorization
```

The profile declares no endpoints on purpose: an endpointless profile's credential resolves only where a sandbox policy binds that provider instance with `credential_binding`, so each agent's binding and its path rules live in that agent's own policy.

### Agent A's policy

B's is the same with `b` for `a`; `192.0.2.10` stands for the router's address.

```yaml
version: 1
filesystem_policy:
  include_workdir: true
  read_only: [/usr, /lib, /bin, /etc]
  read_write: [/tmp]
landlock:
  compatibility: hard_requirement
network_policies:
  fleetring_router:
    endpoints:
      - host: "192.0.2.10"
        port: 8443
        allowed_ips: ["192.0.2.10/32"]
        protocol: rest
        enforcement: enforce
        rules:
          - allow: { method: GET, path: /v2 }
          - allow: { method: GET, path: /v3 }
          - allow: { method: GET, path: "/db/agent-a-*/v2" }
          - allow: { method: GET, path: "/db/agent-a-*/v3" }
          - allow: { method: POST, path: "/db/agent-a-*/v2/pipeline" }
          - allow: { method: POST, path: "/db/agent-a-*/v3/pipeline" }
          - allow: { method: POST, path: "/db/agent-a-*/v3/cursor" }
        credential_binding:
          provider: fleetring-a
    binaries:
      - path: /usr/bin/curl
```

On Docker, the sandbox adds OpenShell's baseline paths at start (`/var/log`, `/proc` and `/dev/urandom` read-only; `/dev/null` read-write) and saves the result as a new revision. On the microVM it did not add `/proc` (see "microVM differences").

### The gateway configuration

```toml
[openshell]
version = 2

[openshell.gateway]
compute_driver = "docker"

[openshell.drivers.docker]
socket_path = "/var/run/docker.sock"
supervisor_network_mode = "bridge"
allow_driver_config = true

[openshell.supervisor.network]
additional_ca_bundle = "<work>/os/fleet-ca.pem"   # the dev fleet's ca.crt
```

## Results

Step IDs are those of [`hostile-agent.sh`](FR-108/hostile-agent.sh), run inside A with B's placeholder (as an operator read it from B) as an argument. "PC" marks a step whose positive control fired: the same command found what A had planted in itself. Everything below is from [`rows-docker.txt`](FR-108/results/rows-docker.txt) unless it names another transcript; "microVM differences" follows the table.

| Row (ADR-0044 point 11.5) | Step: what A tried | What happened | Verdict |
|---|---|---|---|
| **1. Read B's holder key and token** | 0.2: plant a key, a token, `identity.toml`, a SQLite file and a listening socket in A | All created | **Pass** (both drivers) |
| | 1.1: list `/sandbox` and `/sandbox/work`; read B's key path | Only A's files; B's path "No such file or directory". B's canaries are in B's volume (`workdir.txt`, W1, read from the host) | |
| | 1.2 (PC): `find` over every root the policy lets A read (`/sandbox /tmp /etc /usr /lib /bin /var/log /dev /run/openshell-supervisor-ca`), no `-xdev`, stderr kept, for `client.agent-*.key`, `token`, `identity.toml`, `*.sqlite*`, `*.db`, `ca.key` | Found exactly A's four planted files, nothing else. Three unreadable directories: `/etc/ssl/private`, `/dev`, `/run/openshell-supervisor-ca` | |
| | 1.3: list `/root /home /opt /srv /mnt /media /var /var/lib /run /.openshell /.openshell/channel` and `/`; stat `/Users`, `/private`, `/data`, `~/.fleetring/agents` | Each existing directory "Permission denied" on listing (Landlock); the rest don't exist | |
| | 1.4: `ps`; every `/proc/<pid>/environ` | Only A's processes. A's environment holds `FLEETRING_TOKEN=openshell:resolve:env:v…_FLEETRING_TOKEN`, never the token; `/proc/1/environ` and `/proc/59/environ` "Permission denied" | |
| | 1.5: read `/.openshell/channel/sandbox/bootstrap.json` | "No such file or directory"; listing `/.openshell` "Permission denied" | |
| | 1.6: **B's placeholder** at the router, on A's own path | HTTP 500 `credential_unavailable`; nothing forwarded | |
| | 1.7: B's placeholder at B's path | HTTP 403 `policy_denied` | |
| | 1.8, 1.9: A's placeholder in `X-Api-Key`, then in `?token=` | HTTP 403 `credential_placement_mismatch` both times, logged as a HIGH finding | |
| | 1.10: A's placeholder as a SQL argument in the body | The router returns the placeholder string itself, not the token: body rewriting is off | |
| | A's placeholder at other allowed endpoints, unbound (L4 and REST) (`endpoints.txt`, S8) | In a header: HTTP 403 `credential_endpoint_mismatch`. In a body: forwarded as the placeholder string, unresolved | |
| | A's and B's placeholders replayed **from outside** the sandbox, straight to the router | HTTP 401 "the token is refused: the token is malformed" | |
| **2. Open B's SQLite files; read the daemon's data directory or `ca.key`** | 2.1 (PC): the same `find` for `*.sqlite*`, `*.db`, `ca.key` | Found only A's planted SQLite file | **Pass** (both drivers) |
| | 2.2: `ls` B's SQLite path and Docker's sockets | None exists | |
| | 2.3: `/proc/mounts` in full | The container's root, A's own work volume, OpenShell's channel volumes, the CA tmpfs, and Docker's usual pseudo-filesystems; no host path. `docker inspect`: no host mounts, `NetworkMode=none`, `CapDrop=[ALL]`, `no-new-privileges`, user `10001:10001`. The supervisor: user `65534:65534`, no capabilities, read-only root, one host file mounted (the CA bundle) | |
| | 2.4: `mount(tmpfs)` | -1, "Operation not permitted" | |
| **3. Control B's terminal, or reach a multiplexer control socket** | 3.1 (PC): `find -type s` over the readable roots; `/proc/net/unix` | Found A's planted socket. `/proc/net/unix` also lists `/.openshell/channel/control/supervisor/control.sock`, OpenShell's channel | **Pass** (both drivers) |
| | 3.2 (PC): connect to A's socket, then B's | A's answers `A-PC`; B's "No such file or directory". Inside B, B's answers `B-MUX-CANARY` | |
| | 3.3: `tmux -S /tmp/b.tmux ls; tmux ls` | No such sockets | |
| | 3.4: PID 1 (the sandbox runtime, A's uid): `/proc/1/mem`, `environ`, `maps`, `fd`, `root`, `cwd`; `ptrace` attach; `process_vm_readv`; `kill`; `pidfd_open` | All "Permission denied" or "Operation not permitted" (`pidfd_open`: "Function not implemented"); only `/proc/1/cmdline` is readable | |
| | 3.5: `kill -0` on PIDs 1 to 300 | Only A's own processes answer | |
| | 3.6: connect to every socket path in `/proc/net/unix`, and to `/run/openshell/ssh.sock` | **A connects to the supervisor's `control.sock` and receives a TLS ClientHello** (the supervisor is the TLS client on that channel); the SSH socket doesn't exist for A | |
| | 3.7: answer that ClientHello as the sandbox runtime would, with a self-signed certificate | "SSLV3_ALERT_CERTIFICATE_UNKNOWN": the supervisor refuses it. The runtime's real key sits on the host (`trusted-base.txt`) | |
| | 3.8: A's own request afterwards | HTTP 200: the attempts didn't disturb the channel | |
| | a real terminal (`tty.txt`): A's command under `sandbox exec --tty` | A has `/dev/pts/0` but can't open it ("Permission denied"), so TIOCSTI is out of reach; no other terminal exists | |
| **4. Launch a child with more than A's own ceiling** | 4.1: `unshare -Ur id` | "Operation not permitted" | **Not applicable to the backend**: the launcher enforces ceilings (both drivers) |
| | 4.2: look for `openshell`, `fleetring`, gateway keys | None | |
| | 4.3: the gateway at `host.openshell.internal:17670`, `192.168.65.254:17670`, the router's address `:17670`, `host.docker.internal:17670`, `127.0.0.1:17670` | Each refused; the log says `policy_dns_ineligible` for the names and `transparent_tcp_policy_denied` for the addresses. The microVM gateway's port 17680 likewise (`vm.txt`) | |
| | 4.4: `http://policy.local/` | HTTP 404 `feature_disabled` | |
| | 4.5: a child shell runs curl to example.com | Refused, as for A | |
| **5. Exfiltrate** | 5.1, 5.2: example.com, 1.1.1.1:80 | Refused in 0–1 ms; `transparent_tcp_policy_denied` | **Pass** (both drivers) |
| | 5.3: the dev node's port 7410 at the router's address and at the host alias; the loopback router at the alias | Refused; the alias doesn't resolve without a rule | |
| | 5.4: python3 (not a listed binary) to the router | `PermissionError: [Errno 13]`, logged with the binary's path | |
| | 5.5: `nc` to 1.1.1.1:53, TCP and UDP | "Permission denied" | |
| | 5.6 and `dns.txt`: DNS | Ordinary names get synthetic answers in 198.18.0.0/15; `host.docker.internal`, `host.openshell.internal` and `metadata.google.internal` get none; TXT and MX are empty; `dig @1.1.1.1` and `@8.8.8.8 +tcp` "permission denied". tcpdump in the supervisor's namespace saw **no** packet for the agent's names, and did see a control query sent from that namespace | |
| | 5.7: A's token at `/db/agent-b-memory/` | HTTP 403 `policy_denied`, before anything is forwarded | |
| | the router alone: A's rule widened to `POST /db/**` | The router answers HTTP 403 "this token can't read database 'agent-b-memory'": scopes hold on their own | |
| | 5.8, 5.9: `../` traversal; `%2D` | Normalized, then HTTP 403 `policy_denied` | |
| | 5.10: `%2F` in the database segment | Connection closed; the log says encoded `/` isn't allowed on this endpoint | |
| | 5.11: `Host: agent-b-memory.db.example` | HTTP 403 `request_authority_mismatch` | |
| | 5.12: `DELETE` on A's path | HTTP 403 `policy_denied` | |
| | 6.1–6.5: HTTP/2; an absolute-form target naming B's path, then another host; `CONNECT`; `Upgrade: websocket` | HTTP/2 isn't negotiated (HTTP/1.1); B's path 403; another host and `CONNECT` closed ("absolute-form request authority does not match the Host header", "not a valid origin-form path"); Upgrade 403 `unsupported_l7_protocol` | |
| | 7.1–7.5 (`endpoints.txt`): raw framing on one TLS connection, with A's credential injected: keep-alive A-then-B; CL.TE; TE.CL; obfuscated TE; two Content-Lengths | Keep-alive: 200, then 403 for B. The rest: connection closed before any request is forwarded ("Request contains both Transfer-Encoding and Content-Length headers"; "multiple Content-Length headers with differing values"); B's canary never appeared | |
| | reflection (`endpoints.txt`): six error-inducing requests to the router | No response carried A's token | |
| **A can still do its own work** | `own-work.sh`: `CREATE TABLE`, `INSERT`, `SELECT` on `agent-a-memory` through the router | HTTP 200 each, the row read back | **Pass** (both drivers) |

"With hostile prompts and auto-accept on" is the condition the script stands for: it is what a prompt-injected agent whose every tool call is accepted would do. OpenShell's own auto-approval turns that condition into a hole ([`approval.txt`](FR-108/results/approval.txt)). In a sandbox created with `--approval-mode auto` and no gateway-wide value, OpenShell drafted `allow_example_com_443` from the sandbox's own denied curl and approved it ("Prover: no new findings"); the fifth attempt, 21 s after the first, returned HTTP 200. With `proposal_approval_mode = manual` set gateway-wide, a sandbox created with `--approval-mode auto` reported `manual (global)`, the draft stayed `pending`, and all six attempts failed. A's own denied connections during these runs left six pending drafts, one per destination; none applied.

### microVM differences

[`rows-vm.txt`](FR-108/results/rows-vm.txt) and [`vm.txt`](FR-108/results/vm.txt). Every row has the same verdict, and every positive control fired; these steps differ:

- **Identity:** the guest ran the agent as `uid=1000(ubuntu)` with no supplementary groups, not the image's 10001 (the driver uses the image's `sandbox` account, otherwise 1000).
- **`/proc` is unreadable.** The baseline did not add `/proc` to this policy on the VM driver, so 1.4 (`ps`), 2.3 (`/proc/mounts`), 3.1's `/proc/net/unix`, 3.5 and 3.6 return "Permission denied" or nothing: those steps show less on the VM, not a weaker boundary. 3.4's PID 1 probes are denied as on Docker, `/proc/1/cmdline` included.
- **The channel is vsock**, not a Unix socket: 3.6 and 3.7 don't apply, and `socket(AF_VSOCK)` fails with "Operation not permitted" (`vm.txt`).
- **4.3 and 5.3** name `host.docker.internal`, which means nothing there; `vm.txt` probes the VM gateway's own port, 17680, which is refused like 17670.
- **The supervisor is a process on the Mac**, not a container, so the host alias resolves to `127.0.0.1` ("Policy DNS mapped host.openshell.internal resolved=127.0.0.1").

## Answers to the ticket's questions

### TLS

- **The agent sees a certificate from a per-sandbox authority** generated when the supervisor starts: `CN=OpenShell Sandbox CA, O=OpenShell`, valid from 1975 to 4096, issuing `CN=192.0.2.10` with the IP as its subject alternative name. OpenShell points the agent's trust variables (`SSL_CERT_FILE`, `CURL_CA_BUNDLE` and others) at a bundle under `/run/openshell-supervisor-ca/`; that bundle held 152 certificates, the fleet's authority among them, which was observed rather than documented (`endpoints.txt`). The token exists in plaintext only in the supervisor, between the two TLS sessions.
- **The supervisor verifies the router's certificate against the fleet's authority, name included.** Without `additional_ca_bundle`, a sandbox's request to the router failed (`NET:FAIL`, curl "Empty reply from server"); with it, the same request succeeded over TLS 1.3 with the authority's Ed25519 signatures (`tls-noca.txt`). A certificate from the same authority for the wrong name (`wrong-name.invalid`) was refused the same way (`endpoints.txt`).
- **The trust is coarse.** The bundle is gateway-wide, for every sandbox and every policy-permitted destination, and it adds to the public roots rather than replacing them; changing it takes a gateway restart and a stop and start of every sandbox (OpenShell's "Gateway Configuration File", "Additional Destination CAs"). OpenShell can't pin one endpoint to one authority. The fleet's authority also signs node and client certificates, so a policy that allowed a node's address would let the supervisor trust that node too; the launcher's policies allow only the router.

### Request rules

**Yes, a `rest` rule limits an agent to its own `/db/` paths, as a second check beside scopes**, and each check was shown failing closed on its own (5.7 for OpenShell; the widened rule for the router). Details that matter:

- `enforcement` defaults to `audit`, which logs and forwards; the rule blocks only with `enforcement: enforce`.
- `*` in a path never crosses `/`, and OpenShell normalizes `..` and percent-escapes before matching (5.8, 5.9).
- `%2F` is refused unless `allow_encoded_slash: true` (5.10). ADR-0037's path form percent-encodes `/` in database names; agent prefixes contain none.
- ADR-0037's host form can't be used through OpenShell: the request's authority must match the endpoint (5.11).
- OpenShell refuses ambiguous HTTP framing, absolute-form targets for other hosts, `CONNECT` and upgrades on REST endpoints (6.x, 7.x), so path rules can't be bypassed at the HTTP layer in the ways tried.
- The rule sees paths, not statements: scopes, read-only sessions included, stay the authority.

### Rotation and revocation

Measured with [`probe-loop.sh`](FR-108/probe-loop.sh) (one `INSERT` every 0.5 s) in processes started before each change ([`rotation.txt`](FR-108/results/rotation.txt); each loop's log in [`rotation-logs/`](FR-108/results/rotation-logs/)). The Mac, the Docker Desktop VM and sandbox A agreed to within 0.3 s at the start and the end; the microVM guest agreed with the Mac to within 0.03 s (`vm.txt`).

| Run | Change | New processes | The process already running |
|---|---|---|---|
| R1 | `provider update` to a read-only token (ordinary placeholder) | New placeholder and the new token once the wait returned (7.8 s): their `INSERT` got `PERMISSION_DENIED` | **Not cut:** 74 writes succeeded, all of them, until the loop was stopped 25 s later |
| R2 | expiry set 40 s ahead **before** the process started, so it holds the generation that carries the expiry | Refused after the expiry (HTTP 500 `credential_unavailable`) | **Cut at the expiry:** last success 0.5 s before it, first failure 0.07 s after |
| R3 | expiry set 25 s ahead **after** the process started, with `--credential-expires-at` alone | Refused after the expiry | **Not cut:** 111 writes succeeded, the last 30 s past the expiry, when the loop was stopped. The update created a new revision (new processes got a new placeholder); the expiry belongs to it, and the running process holds the earlier revision, which has none |
| R4 | `sandbox provider detach` while A's policy binds the provider | | **Refused:** `FAILED_PRECONDITION`, "The system is not in a state required for the operation's execution", no reason given |
| R4 | `policy set` removing that `credential_binding` | No placeholder at all (the router then answers 401) | **Cut 5.1 s after the change was submitted**, when the sandbox loaded the new revision |
| R4 | detach, after the unbinding; then re-attach and re-bind | The **same** placeholder as before the detach | Detach succeeded. After re-attaching, **the running process's old placeholder resolved again** (six more writes succeeded) |
| R5 | `stable_placeholder: true`: a value-only update to a read-only token, then to a dead value, then back, then an expiry 20 s ahead | One placeholder (`openshell:resolve:env:s…`) throughout | **Each change reached it:** `PERMISSION_DENIED` 0.3 s after the read-only update's wait returned; HTTP 401 for the dead value; success again after restoring; first failure 0.3 s after the expiry |

So, against processes already running: an expiry is enforced on the generation that carries it (R2), as OpenShell's Profiles page says ("Running sandboxes also reject expired retained credential generations"); with ordinary placeholders, any later update, an expiry set after launch included, makes a new revision and leaves the running process on the old one (R1, R3), which the same page describes ("a retained revision can continue to resolve its previous value"). R3 reads against the page's "Expiry, endpoint authorization, and acknowledged detach continue to apply to retained references" only if that sentence is taken to mean an expiry set later; the run shows it doesn't. With `stable_placeholder: true`, value updates, dead values and expiry all reach a running process within a fraction of a second (R5). Detach is a removal, not a revocation: re-attaching brings the same placeholder back to life (R4). FleetRing's denylist (ADR-0026 point 6) stays the authoritative cut; it wasn't exercised here.

**Renewal follows from this.** Under ADR-0044 point 3.7, the router accepts bearer self-delegations of at most 24 hours, so the launcher must push each agent a new token at least daily. With ordinary placeholders a running agent never picks one up (R1); with `stable_placeholder: true` it does (R5). OpenShell's other refresh path, `token_grant`, can't be combined with `stable_placeholder` and needs a SPIFFE Workload API socket for the supervisor (Profiles page, "Dynamic Token Grants"), which this setup doesn't have.

### Delegation

**Confirmed: the credential proxy must attenuate children's tokens.** Inside the sandbox there is only the placeholder (1.4); the token's bytes never enter it, and body rewriting is off unless a policy turns it on (1.10). Attenuating a biscuit means appending a block to the token's bytes, and delegating means signing with the holder key (ADR-0044 point 3), so an agent holding a placeholder can do neither. This matches point 11.2: children are spawned only through the launcher, whose credential proxy mints the child's attenuated token and creates the child's sandbox with a provider holding it.

### MCP

`fleetring mcp` speaks stdio, so an MCP client inside a sandbox would start it there, holding the credential. A direction for running it outside, as point 11.6 says, untested here and outside this ticket's scope:

- One `fleetring mcp` per agent on the host, as that agent, serving MCP's Streamable HTTP transport, which `fleetring mcp` doesn't have today.
- The sandbox's policy allows only that endpoint, with `protocol: mcp`, `enforcement: enforce`, and rules by tool name (for example, deny `db_drop`). OpenShell matches method and tool name, never arguments ("Policy Schema Reference", "MCP Rules"), so which database a tool touches stays the scopes' job.
- The caller is identified by a credential the agent can't read: a per-agent secret held by an OpenShell provider bound to that endpoint, which the credential proxy maps to the agent. That meets point 11.2's requirement (identity from an isolated channel) differently from point 11.6's "mounted socket"; OpenShell sandboxes reach host Unix sockets only through bind mounts.
- Placeholders sent to the wrong header or endpoint are refused (1.8, 1.9, S8), but a bound endpoint that echoes requests hands the real credential back to the agent: the echo server bound to A's provider returned A's token in its response body (`endpoints.txt`, "reflection"). Whatever the credential proxy serves must never reflect request headers.

### Platforms

| Platform | Tested? | Result |
|---|---|---|
| macOS on Apple silicon, Docker Desktop (Linux VM, kernel 6.12.76) | Yes | Every row as above. Bridge networking unless Docker Desktop's host networking is on. Landlock and seccomp enforce in the Linux VM's kernel, not in macOS |
| macOS on Apple silicon, microVM (libkrun on Hypervisor.framework, guest kernel 6.12.109) | Yes | Every row as above, with the differences listed. The driver needs e2fsprogs, an ad hoc signature with the hypervisor entitlement (the formula does this), and a short `state_dir`: a long path failed with "path must be shorter than SUN_LEN" (first pass; observed, transcript not kept) |
| Linux 6.2 or later: Docker, Podman, microVM (KVM), Kubernetes | **No**: no Linux host here | Per the docs, the sandbox requires Landlock ABI 3 (Linux 6.2); Podman needs 5.x with cgroups v2; the microVM driver needs KVM; Kubernetes needs a CNI that enforces `NetworkPolicy`. **To be tested before ADR-0048 is accepted** |
| Windows (MXC) | No | OpenShell's Installation page calls it "Development; not production-qualified" |

**The host alias reaches host loopback.** `host.openshell.internal` is how OpenShell sandboxes reach services on the gateway host (Gateway Configuration page), and on these drivers it lands on the host's loopback: Docker Desktop forwards 192.168.65.254 there, and the VM driver maps the name to 127.0.0.1. A rule for `host.openshell.internal:8444` reached a router listening only on `127.0.0.1:8444`; on Docker Desktop that took `allowed_ips: [192.168.65.254/32]`, and on the VM driver no `allowed_ips` at all (`endpoints.txt`, `vm.txt`). Ports the rule doesn't name stayed closed (the dev node's 7410 and the gateway's 17670 through the same alias were refused). So OpenShell's "never authorize loopback" concerns the address the policy sees, not where the connection lands.

**Router placement: LAN address or host loopback.** The acceptance runs put the router on the Mac's LAN address, which exposes it, and every bearer token it accepts, to the local network. The alternative tested above keeps the router on `127.0.0.1` and lets each sandbox reach it only through the alias on one exact port; own-work passed that way on both drivers, and B's path was still refused. **Recommendation: loopback plus the alias**, with the alias allowed only on the router's (or credential proxy's) port, and otherwise a host firewall in front of a LAN listener. How the alias maps on native Linux, for Docker and for Podman, is the open question; ADR-0048 leaves the choice open until a Linux run.

### Work directories

([`workdir.txt`](FR-108/results/workdir.txt))

- **Volumes (W1, used in the acceptance runs).** Each sandbox gets a labelled Docker volume at `/sandbox/work`; resource admission stays on and checks only the labels, so which volume a sandbox gets is the launcher's request, never the agent's. B's canaries are in B's volume (read from the host); A sees only its own.
- **Copy in and out (W2).** `sandbox upload` and `download` move files into A's work directory and back; B never sees them.
- **Bind mounts (W3).** They need `enable_bind_mounts = true` and resource admission off, which OpenShell warns "can negate OpenShell isolation". On Docker Desktop the mounted directory was unusable inside the sandbox even with `/sandbox/work` listed `read_write`: listing and writing were refused (a file could be created empty), and the mount is Docker Desktop's `fakeowner` filesystem. A couldn't reach B's directory, but A created symlinks in its own (`to-b -> ../b/canary`, `to-root -> /`), and the host followed `to-b` into B's directory. A launcher that copies results out of a bind-mounted directory must not follow symlinks.

**Recommendation:** volumes plus copy in and out; bind mounts only if a Linux run shows them usable, and never followed on the host.

### Availability: A against B

([`resources.txt`](FR-108/results/resources.txt), [`vm.txt`](FR-108/results/vm.txt)) OpenShell sets no memory or CPU limit unless asked (A's cgroup: `memory.max max`, `cpu.max max`), limits processes to 2,048 per sandbox, and has no disk quota on Docker.

- A allocated 1 GiB with no limit; A forked until the limit stopped it at 1,994 children; A wrote 2 GiB to its volume, on the Docker Desktop VM's disk that every sandbox shares (25 GB used before, 27 GB after). Meanwhile B ran 50 processes and reached the router in 28 ms each time: within these bounds, B was unaffected. The runs stopped at 1 GiB and 2 GiB on purpose; with no limits, nothing stops A from taking the VM's 16 GiB or its free disk.
- `--memory 256Mi` was enforced on Docker: the process was killed (exit 137) at its first 256 MiB.
- On the microVM, `--memory` is ignored, as the Sandbox Runtimes page says (512 MiB allocated under `--memory 256Mi`); the guest's `mem_mib` (1,024) is the bound, and a process was killed before 1,024 MiB.

### Operational cost

- **Another daemon:** the gateway (`openshell-gateway`, 87 MB) with its own SQLite state, its own PKI, and a 32-byte key-encryption key in a file beside the database (`trusted-base.txt`). Agents' tokens were not in its files in the clear, but whoever reads that directory can decrypt them; it also holds each sandbox's runtime key and JWT. The CLI's client certificate is an operator credential, and an operator can export real provider credentials with `GetProviderCredentials`, so the gateway host is part of the trusted base. In the first pass, the gateway wrote the CLI's client bundle to `~/.config/openshell/` although `XDG_CONFIG_HOME` pointed elsewhere (observed; this pass redirected `HOME` instead).
- **Per agent:** two containers on Docker (the workload, and its supervisor as 65534:65534 with no capabilities), or one VM and one host process. Images: supervisor 71 MB, sandbox runtime 9.7 MB, the default base 106 MB.
- **Runtimes:** Docker Desktop or Engine 28 or later, Podman 5.x, or KVM or Hypervisor.framework; e2fsprogs for the microVM driver.
- **Telemetry:** on by default; `OPENSHELL_TELEMETRY_ENABLED=false` on the gateway turned it off and reached supervisors.
- **Licence and maturity:** Apache-2.0. The repository was created in February 2026; 0.1.0 came out on 2026-09-25 with breaking changes, and eight releases, 0.1.1 to 0.1.8, followed by 2026-11-10. Expect churn: pin exact versions and image digests, and re-run this test on each upgrade.

## Gaps and risks

1. **Ordinary placeholders never see a rotation.** A running process keeps the revision it started with (R1, R3); only `stable_placeholder: true` delivers new values and expiries to it (R5). Daily renewal (point 3.7) therefore needs stable placeholders or restarts.
2. **Detach is refused while a policy binds the provider, and re-attaching revives the old placeholder** (R4). The fast local cut is removing the binding (about 5 s here).
3. **The fleet's authority is trusted gateway-wide,** beside the public roots, and the agent's bundle contains it too.
4. **No limits by default, and no disk quota on Docker:** one agent can take the shared VM's memory and disk. On the microVM, `--memory` is ignored and `mem_mib` is per gateway.
5. **"Auto" approval widens network access with no person,** and OpenShell drafts a proposal from every denied connection.
6. **The host alias reaches host loopback** on Docker Desktop and the VM driver.
7. **Terminal output from a sandbox isn't sanitized.** An operator who watches an agent's output in a terminal should treat it as untrusted, as with any container's output; the launcher should filter control sequences before showing it. Interactive `sandbox connect` printed nothing under a scripted pseudo-terminal (observed, transcript not kept) and wasn't tested further; `sandbox exec --tty` stood in for it.
8. **A bound endpoint that reflects headers returns the real credential** to the agent (`endpoints.txt`).
9. **The channel socket is reachable.** A can connect to its supervisor's `control.sock` and start a TLS handshake; it can't complete one without the runtime's key, which lives on the host (3.6, 3.7). Every sandbox on Docker also runs as the same uid (10001), so isolation between A and B rests on the container (or VM) boundary, not on users.
10. **The policy's `binaries` bind by executable path,** pinned on first use: a rule that lists an interpreter admits every script it runs.
11. **Untested here:** Linux hosts, Podman, Kubernetes, interactive `sandbox connect`, and FleetRing's denylist cut through the router.

## Recommendation

**Design the launcher against OpenShell as its first backend, for both the `container` and `microvm` rows, and accept that choice only after point 11.5's test passes on a Linux host** (Docker and Podman at least; KVM if available). Nothing here failed the acceptance test; the gaps above are things the launcher configures around, or asks upstream to change.

**Upstream changes to ask of OpenShell:**

1. **Per-endpoint upstream trust:** an endpoint- or sandbox-scoped CA that replaces the public roots for that endpoint, instead of the gateway-wide `additional_ca_bundle`.
2. **Disk quotas for Docker and Podman sandboxes,** and `--memory` honoured (or refused) on the VM driver.
3. **A detach that is a revocation:** re-attaching should not revive a detached placeholder, and a refused detach should say that a policy binds the provider.
4. **Terminal output filtering:** an option to strip terminal control sequences from `exec` and `connect` output.
5. **Packaging on macOS:** the Homebrew formula should install e2fsprogs, which the VM driver documents as a dependency, and the driver's socket shouldn't depend on `state_dir`'s length.

Rotation and expiry are covered by `stable_placeholder: true` (R5), so they need no upstream change; the launcher uses it.

**What the launcher must configure,** whatever upstream does (ADR-0048 records these):

- `proposal_approval_mode = manual` gateway-wide; the policy advisor off.
- One provider per agent from an endpointless FleetRing profile with `stable_placeholder: true`, bound by `credential_binding` to the router (or credential proxy) in that agent's own policy, with `protocol: rest`, `enforcement: enforce`, and the agent's own `/db/<prefix>*/` paths.
- `landlock.compatibility: hard_requirement`; `--cpu` and `--memory` on Docker and Podman, `vcpus` and `mem_mib` on the microVM; a work volume per agent; no bind mounts; no gateway or FleetRing client certificate in any sandbox.
- The router (or credential proxy) reachable only on one port: on host loopback through the alias, or on a LAN address behind a host firewall (open until a Linux run).
- Sandboxed and unsandboxed agents never share a host: an unsandboxed agent running as the operator can read the gateway's state and its key-encryption key, and with them every agent's token.
- Telemetry off; OpenShell, its images and the base image pinned to exact versions and digests.
- Revocation through FleetRing's denylist; removing the binding as the fast local cut.
