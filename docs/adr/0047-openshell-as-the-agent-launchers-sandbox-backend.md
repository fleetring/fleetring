# ADR-0047: The agent launcher is designed against OpenShell as its first sandbox backend, with each agent's token injected by OpenShell's supervisor on one endpoint only

- **Status:** Proposed. Accepting it needs ADR-0044 point 11.5's test to pass on a Linux host first (see "Open questions for the owner").
- **Date:** 2026-11-12
- **Related:** ADR-0044 (point 11, the agent launcher; this record amends point 11.6's backend table and its contract's network and credential lines), ADR-0037 (the router), ADR-0025 (point 3, tokens), ADR-0026 (point 6, the denylist); ticket FR-108 (#173) and its spike, [`docs/plans/FR-108-openshell-spike.md`](../plans/FR-108-openshell-spike.md)

## Context

ADR-0044 point 11.6 says the launcher runs agents in an existing isolation mechanism behind one interface, and that existing agent sandboxes are evaluated as backends before anything is built. FR-108 evaluated the first candidate, OpenShell (NVIDIA, Apache-2.0, 0.1.8), by running point 11.5's acceptance test with it, on macOS on Apple silicon, with OpenShell's Docker driver (Docker Desktop) and its microVM driver.

Agents A and B each ran in their own sandbox, each holding only an OpenShell placeholder for its own FleetRing token, which OpenShell's supervisor, outside the sandbox, replaced with the token only on requests to the router. A hostile script in A tried every attack of point 11.5, with a positive control planted in A for every search. Every attack failed: A couldn't read B's key, token or files, open B's SQLite files or the fleet's data directory, reach B's terminal or a multiplexer socket, resolve B's placeholder or its own anywhere but the router, or reach any destination but the router; with OpenShell's path rule widened, the router's scopes still refused B's database. Spawning children is the launcher's job, and OpenShell offers no path to it from inside a sandbox. A still did its own work through the router. Nothing was tested on Linux.

Five facts from the spike shape the decision:

- **OpenShell's credential model fits FleetRing's.** A provider credential resolves only for the endpoint a sandbox policy binds, only in the declared header, and never for a placeholder another sandbox holds; elsewhere it is refused (`credential_endpoint_mismatch`, `credential_placement_mismatch`). An endpoint that echoes requests would hand the real credential back, so what the credential is bound to must never reflect it. The agent can't attenuate a token it never sees, so the launcher's credential proxy attenuates (ADR-0044 point 3), as point 11.2 already requires.
- **It doesn't use a mounted socket.** Point 11.6's contract has one mounted FleetRing proxy socket per sandbox. OpenShell sandboxes reach host Unix sockets only through bind mounts, which require turning resource admission off and which OpenShell warns can negate its isolation. Its equivalent of an isolated channel is one network endpoint plus a credential the sandbox can't read.
- **Placeholders come in two kinds.** An ordinary placeholder pins a running process to the credential revision it started with: an update, or an expiry set later, never reaches it. With `stable_placeholder: true`, value updates, dead values and expiry reach running processes within a second.
- **Nothing is bounded by default.** Without `--cpu` and `--memory`, one agent can take the shared VM's memory; Docker and Podman have no disk quota; on the VM driver `--memory` is ignored and `mem_mib` applies.
- **It is young and moves weekly.** 0.1.0 shipped on 2026-09-25 with breaking changes, and eight releases (0.1.1 to 0.1.8) followed by 2026-11-10.

## Decision

1. **OpenShell is the first backend the launcher is designed against,** for both the `container` and `microvm` rows. It passed point 11.5's test on macOS with Docker Desktop and with the VM driver; on Linux (Docker, Podman, KVM) and Kubernetes it is untested, and OpenShell's Windows support is development-only. Point 11.6's backend table is amended to:

   | Backend | Isolation | Where | Tested |
   |---|---|---|---|
   | `none` | trusted local development (point 3) | everywhere; the default until a backend ships | n/a |
   | `container`, on OpenShell's Docker or Podman driver | a workload container with no network, all capabilities dropped, a non-root user, `no_new_privs`, Landlock (ABI 3 or later) and seccomp, beside a separate supervisor container that mediates its traffic. On macOS and Windows these runtimes run a Linux VM, which enforces Landlock and seccomp | developer machines and Linux servers | macOS with Docker Desktop; Linux and Podman untested |
   | `microvm`, on OpenShell's VM driver (libkrun: KVM on Linux, Hypervisor.framework on macOS) | a guest kernel per agent, with no network device; the supervisor is a host process | servers and clouds, for hostile or multi-tenant agents; Macs | macOS; KVM untested |

   Apple's `container`, Firecracker and Cloud Hypervisor stay possible later behind the same interface; they are no longer the planned first backends.

2. **The contract's network and credential lines read, for OpenShell backends:**
   - **Network:** deny by default, except one endpoint, the router's or the credential proxy's, reached only through the supervisor, with `protocol: rest`, `enforcement: enforce`, and rules allowing only the agent's own `/db/<prefix>*/` paths and the version probes.
   - **Credentials:** the sandbox holds no token and no holder key. The agent's token is an OpenShell provider credential of an endpointless FleetRing profile, with `stable_placeholder: true`, bound to that endpoint by `credential_binding` in the agent's own policy. A caller is identified by the credential its supervisor injects, which the agent can't read, never by a name or variable the agent supplies.
   - **Work directory:** a volume per agent, filled and emptied by copying in and out. No bind mounts.

   The other lines (no home directory or host sockets, `spawn(child)`) stand. **Resources** become: `--cpu` and `--memory` on Docker and Podman, `vcpus` and `mem_mib` on the VM driver, set on every sandbox; disk is unbounded on Docker and Podman until OpenShell offers a quota.

3. **The launcher configures OpenShell this way, and refuses to start otherwise:**
   - `proposal_approval_mode = manual` gateway-wide, and the policy advisor off, so nothing a sandbox asks for widens its policy without a person.
   - `landlock.compatibility: hard_requirement`; no gateway or FleetRing client certificate in any sandbox. The gateway's state directory, which holds the key that decrypts every agent's token, and its client certificate are administrator credentials.
   - **Sandboxed and unsandboxed agents never share a host:** an agent running unsandboxed as the operator could read the gateway's state, and with it every agent's token.
   - The fleet's authority in `[openshell.supervisor.network] additional_ca_bundle`, and no policy allowing any other endpoint that authority certifies.
   - The endpoint of point 2 reachable on one port only. `host.openshell.internal` reaches the host's loopback on Docker Desktop and the VM driver, so a rule for it names exactly that port.
   - Telemetry off (`OPENSHELL_TELEMETRY_ENABLED=false`); OpenShell, its images, and the base image pinned to exact versions and digests. Each upgrade re-runs point 11.5's test.

4. **Revocation and renewal.** FleetRing's denylist (ADR-0026 point 6) is the authoritative cut. The launcher's fast local cut is removing the policy's `credential_binding` (about 5 s in the spike); it never relies on a detach, which re-attaching undoes. Under ADR-0044 point 3.7 the router accepts bearer self-delegations of at most 24 hours, so the launcher pushes each agent a new token at least daily, as a value-only provider update; with stable placeholders a running agent picks it up at once, and with ordinary ones it never would.

5. **Upstream changes FleetRing asks of OpenShell,** none blocking the launcher's design: per-endpoint upstream trust instead of a gateway-wide bundle; disk quotas on Docker and Podman, and `--memory` honoured or refused on the VM driver; a detach that revokes; an option to strip terminal control sequences from sandbox output; e2fsprogs installed by the macOS package. Until they land, a secure multi-agent mode documents them as limits. FleetRing's fork, `fleetring/OpenShell`, can carry one the launcher needs before upstream takes it.

**A direction, not a decision: MCP under the launcher.** `fleetring mcp` would run outside the sandbox, one per agent, as the credential proxy, serving MCP's Streamable HTTP transport (new FleetRing work), reached as point 2's endpoint with `protocol: mcp` rules by tool name, and identifying the agent by an injected per-agent secret. None of this was tested, and it is outside FR-108's scope; the launcher's own design decides it. Tool-name rules never replace scopes: OpenShell doesn't see tool arguments.

On acceptance, ADR-0044 gets a dated note under point 11.6 pointing here, like its existing note on S2 and ADR-0046, and its row in the index gains "point 11.6 amended by 0047". ADR-0044's text otherwise stays as written.

## Alternatives considered

- **Build the `container` backend on a bare OCI runtime with FleetRing's own credential proxy and per-agent socket,** as point 11.6 first sketched. No 0.1.x dependency, and the socket matches the contract as written, but FleetRing would build and maintain what OpenShell already has and the spike tested: endpoint-bound credential injection, L7 path rules that refuse ambiguous HTTP framing, executable binding, deny-by-default egress with DNS mediation, and one interface over containers and microVMs.
- **Keep the token inside the sandbox, scoped to the agent.** Simplest, and scopes alone kept A off B's database. But the token could then be copied out and used from anywhere until it expires or is revoked, which the placeholder prevents (a placeholder replayed from outside got `401`).
- **Mount a per-agent Unix socket into each OpenShell sandbox.** It keeps the contract's words, but needs OpenShell's bind mounts, which require resource admission off and were unusable inside the sandbox on Docker Desktop.
- **Ordinary placeholders with a restart for each new token.** Works, but restarting every agent daily is disruptive, and an expiry or a rotation would never cut a process that kept running.
- **Wait for OpenShell 1.0.** Less churn, but the launcher's design would go unvalidated; pinning versions and re-running the acceptance test on each upgrade bounds the cost.
- **Evaluate another sandbox first.** Not needed to show a backend exists that passes point 11.5 on macOS; others can still be added behind the interface.

## Consequences

- The launcher can be designed against a backend that has passed point 11.5 on macOS; accepting this record waits for a Linux run.
- FleetRing takes a dependency on a young project with a weekly release cadence, pinned exactly, with the acceptance test as its upgrade gate.
- Point 11.6's "one mounted FleetRing proxy socket" becomes "one endpoint and one injected credential" for OpenShell backends.
- The launcher owns daily token renewal through stable placeholders, resource limits on every sandbox, and the work-volume lifecycle (create, chown, copy in and out, delete).
- Revocation that must take effect now still goes through FleetRing's denylist.
- `docs/lexicon.md` gains nothing yet: "sandbox", "supervisor", "provider" and "placeholder" are OpenShell's terms, used only where OpenShell is meant. This record's "credential proxy" is the lexicon's term for what ADR-0044 also calls the broker.

## Open questions for the owner

- **O1. Where does the router (or credential proxy) listen for sandboxes?** (a) On host loopback, reached through `host.openshell.internal` on one exact port: tested on both macOS drivers, and nothing listens on the LAN. (b) On a LAN address behind a host firewall: what the acceptance runs used. *Recommended: (a), confirmed by a Linux run, where how the alias maps for Docker and Podman is still to be seen.*
- **O2. Must a Linux run pass before this record is accepted?** *Recommended: yes, with Docker and Podman at least, and KVM if a host has it.*
- **O3. Work directories: volumes with copy in and out, or bind mounts?** Bind mounts need resource admission off, were unusable inside the sandbox on Docker Desktop, and let an agent plant symlinks the host follows. *Recommended: volumes, with bind mounts reconsidered only after a Linux run.*
- **O4. Is disk without a quota acceptable for the first secure mode on Docker and Podman?** *Recommended: yes, documented as a limit, with the microVM backend (bounded overlay disk) for agents that must not share disk, and the quota asked upstream.*

To unblock: "Run the Linux test; if it passes, accept ADR-0047 with O1 to O4 as recommended."
